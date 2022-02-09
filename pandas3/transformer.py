import json
import logging
from multiprocessing import get_context
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
from pandarallel import pandarallel
from web3 import Web3

from pandas3.logging_util import logging_basic_config

logging_basic_config()


class Transformer:
    w3 = Web3()

    logger = logging.getLogger(__name__)

    abi_cache_map: Dict[str, Any] = {}

    def __init__(self, nb_workers: int = get_context("fork").cpu_count()):
        pandarallel.initialize(nb_workers=nb_workers)

    def traces_to_func_call_df(
            self,
            df: pd.DataFrame,
            # column name to alias
            alias: Optional[Dict[str, str]] = None,
            # contract address to abi
            abi_map: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        if alias is not None:
            df.rename(alias, axis=1, inplace=True)

        self._batch_load_abi_json(df, abi_map)
        # Avoid checking if abi is NaN
        if 'abi' in df.columns:
            df.abi.fillna('', inplace=True)

        assert ({'block_number', 'tx_index', 'trace_address', 'address', 'input'}.issubset(df.columns))

        # decode input by abi and get parsed df
        parsed_df: pd.DataFrame = df.parallel_apply(
            lambda x: self._parse_input_with_abi(
                address=x.address,
                input_data=x.input,
                abi=x.abi if 'abi' in df.columns and x.abi else abi_map[x.address]
            ),
            axis=1,
            result_type='expand'
        ).rename(
            columns={0: 'func_name', 1: 'input_schema', 2: 'input_params'}
        )

        df = pd.concat([df, parsed_df], axis=1)
        # filtrate the records with valid input
        df = df[df['func_name'] != '']
        df.trace_address.fillna('', inplace=True)

        df['hash_index'] = df.parallel_apply(
            lambda x: self._calculate_trace_index(x.block_number, x.tx_index, x.trace_address),
            axis=1
        )

        distinct_func = df[['address', 'func_name', 'input_params']] \
            .groupby(['address', 'func_name']) \
            .indices \
            .keys()

        result_df = None

        for (address, func_name) in distinct_func:
            indices = df.loc[(df.address == address) & (df.func_name == func_name)].index

            df.loc[indices, 'input_params'] = df.loc[indices].parallel_apply(lambda x: self._nested_tuple_dict_ser(
                raw_dict=x.input_params,
                input_schema=x.input_schema,
                append_obj={'hash_index': x.hash_index}
            ), axis=1)

            funcall_df = pd \
                .json_normalize(df.loc[indices, 'input_params'].values.tolist()) \
                .set_index('hash_index')
            funcall_df.columns = f'{address}.{func_name}.' + funcall_df.columns

            if result_df is None:
                result_df = funcall_df
            else:
                # outer join will append the index from result_df to the column of result.
                result_df = result_df \
                    .join(other=funcall_df, on='hash_index', how='outer') \
                    .set_index('hash_index')

        return result_df.join(df[['hash_index', 'block_number', 'tx_index', 'trace_address']].set_index('hash_index'),
                              on='hash_index',
                              how='inner')

    def _batch_load_abi_json(
            self,
            df: pd.DataFrame,
            abi_map: Optional[Dict[str, str]] = None
    ):
        if 'abi' in df.columns:
            abi_address_tuples = df.groupby(['abi', 'address'])['block_number'].count().index.to_list()

            for (abi, address) in abi_address_tuples:
                if address not in self.abi_cache_map:
                    self.abi_cache_map[address] = json.loads(abi)

        if abi_map is not None:
            for address, abi in abi_map.items():
                if address not in self.abi_cache_map:
                    self.abi_cache_map[address] = json.loads(abi)

    def _parse_input_with_abi(
            self,
            address: str,
            input_data: str,
            abi: str
    ) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        """
        :return: function_name: str,
                 input_schema: List[Dict[str, typ]],
                 input_params: Dict[str, Any]
        """
        try:
            abi_obj = self.abi_cache_map[address]
            contract = self.w3.eth.contract(abi=abi)
            func_obj, input_params = contract.decode_function_input(input_data)

            func_name = vars(func_obj)['fn_name']

            input_schema = \
                [i for i in abi_obj if 'name' in i and i['name'] == func_name and i['type'] == 'function'][0]['inputs']
        except Exception as ex:
            self.logger.warning("parsing input with abi failed: %s", ex)
            func_name = ''
            input_schema = '{}'
            input_params = '{}'

        return func_name, input_schema, input_params

    @staticmethod
    def _calculate_trace_index(
            block_number: int,
            tx_index: int,
            trace_address: str
    ):
        return f'{block_number}_{tx_index}_{trace_address}'

    @staticmethod
    def _tuple_to_dict(
            raw_tuple: Tuple,
            component_schema: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        tuple_dict = {}
        for index, field_type in enumerate(component_schema):
            if field_type['type'] != 'tuple':
                tuple_dict[field_type['name']] = raw_tuple[index]
            else:
                tuple_dict[field_type['name']] = Transformer._tuple_to_dict(raw_tuple[index], field_type['components'])

        return tuple_dict

    @staticmethod
    def _nested_tuple_dict_ser(
            raw_dict: Dict[str, Any],
            input_schema: List[Dict[str, Any]],
            append_obj: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Some func_params decoded by web3.contract are dictionary with some nested tuple fields,
            the function will transform them to dictionary just contains some base type fields.
        """
        result_dict = append_obj
        for key, value in raw_dict.items():
            if type(value) is not tuple:
                result_dict[key] = value
            else:
                component_schema = [i for i in input_schema \
                                    if 'name' in i and 'type' in i and i['name'] == key and i['type'] == 'tuple'][0][
                    'components']
                result_dict[key] = Transformer._tuple_to_dict(value, component_schema)

        return result_dict
