import json
import logging
from datetime import timedelta, datetime
from multiprocessing import get_context
from typing import Optional, Tuple, List, Dict, Any

import cachetools
import pandas as pd
from eth_utils import event_abi_to_log_topic, encode_hex, decode_hex
from pandarallel import pandarallel
from pandas import DataFrame
from web3 import Web3

from . import etherscan
from .logging_util import logging_basic_config

logging_basic_config()


class Transformer:

    def __init__(
            self,
            init_abi_map: Dict[str, str] = {},
            nb_workers: int = get_context("fork").cpu_count()
    ):
        self.w3 = Web3()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.contract_cache = cachetools.TTLCache(50, ttl=timedelta(minutes=3), timer=datetime.now)
        self.abi_cache = cachetools.TTLCache(50, ttl=timedelta(minutes=3), timer=datetime.now)
        self._update_abi_cache(init_abi_map)
        self.is_multiprocessing = nb_workers > 1

        if self.is_multiprocessing:
            pandarallel.initialize(nb_workers=nb_workers, verbose=1)  # filtered INFO log for pandarallel
        else:
            DataFrame.parallel_apply = DataFrame.apply

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

        self._cache_abi_and_contract_by_df(df=df, abi_map=abi_map)

        assert ({'block_number', 'tx_index', 'trace_address', 'contract_address', 'input'}.issubset(df.columns))

        # decode input by abi and get parsed df
        parsed_df: pd.DataFrame = df.parallel_apply(
            lambda x: self._parse_function_input_with_abi(
                contract_address=x.contract_address,
                input_data=x.input,
            ),
            axis=1,
            result_type='expand'
        ).rename(
            columns={0: 'call_name', 1: 'input_schema', 2: 'input_params'}
        )

        df = pd.concat([df, parsed_df], axis=1)
        # filtrate the records with valid input
        df = df[df['call_name'] != '']
        df.trace_address.fillna('', inplace=True)

        df['hash_index'] = df.parallel_apply(
            lambda x: self._calculate_trace_index(x.block_number, x.tx_index, x.trace_address),
            axis=1
        )

        distinct_func = df[['contract_address', 'call_name', 'block_number']] \
            .groupby(['contract_address', 'call_name']) \
            .indices \
            .keys()

        result_df = self._join_multi_name_parts(address_name_tuples=distinct_func, df=df)

        return result_df.join(df[['hash_index', 'block_number', 'tx_index', 'trace_address']].set_index('hash_index'),
                              on='hash_index',
                              how='inner')

    def logs_to_func_call_df(
            self,
            df: pd.DataFrame,
            # column name to alias
            alias: Optional[Dict[str, str]] = None,
            # contract address to abi
            abi_map: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        if alias is not None:
            df.rename(alias, axis=1, inplace=True)

        self._cache_abi_and_contract_by_df(df=df, abi_map=abi_map)

        assert ({'block_number', 'tx_index', 'contract_address', 'topic1', 'topic2', 'topic3', 'topic4', 'data'} \
                .issubset(df.columns))

        parsed_df: pd.DataFrame = df.parallel_apply(
            lambda x: self._parse_event_data_with_abi(
                contract_address=x.contract_address,
                data=x.data,
                topics=[i for i in [x.topic1, x.topic2, x.topic3, x.topic4] if not pd.isna(i)]
            ),
            axis=1,
            result_type='expand'
        ).rename(
            columns={0: 'call_name', 1: 'input_schema', 2: 'input_params'}
        )

        df = pd.concat([df, parsed_df], axis=1)
        # filtrate the records with valid input
        df = df[df['call_name'] != '']

        df['hash_index'] = df.parallel_apply(lambda x: f'{x.block_number}_{x.tx_index}', axis=1)

        distinct_event = df[['contract_address', 'call_name', 'block_number']] \
            .groupby(['contract_address', 'call_name']) \
            .indices \
            .keys()

        result_df = self._join_multi_name_parts(address_name_tuples=distinct_event, df=df)

        return result_df.join(df[['hash_index', 'block_number', 'tx_index']].set_index('hash_index'),
                              on='hash_index',
                              how='inner')

    def _join_multi_name_parts(
            self,
            address_name_tuples: List[Tuple[str, str]],
            df: pd.DataFrame
    ) -> pd.DataFrame:
        result_df = None
        for (address, name) in address_name_tuples:
            indices = df.loc[(df.contract_address == address) & (df.call_name == name)].index

            df.loc[indices, 'input_params'] = df.loc[indices].parallel_apply(lambda x: self._nested_tuple_dict_ser(
                raw_dict=x.input_params,
                input_schema=x.input_schema,
                append_obj={'hash_index': x.hash_index}
            ), axis=1)

            funcall_df = pd \
                .json_normalize(df.loc[indices, 'input_params'].values.tolist()) \
                .set_index('hash_index')
            funcall_df.columns = f'{address}.{name}.' + funcall_df.columns

            if result_df is None:
                result_df = funcall_df
            else:
                # outer join will append the index from result_df to the column of result.
                result_df = result_df \
                    .join(other=funcall_df, on='hash_index', how='outer') \
                    .set_index('hash_index')

        return result_df

    def _cache_abi_and_contract_by_df(
            self,
            df: pd.DataFrame,
            abi_map: Optional[Dict[str, str]] = None
    ):
        if 'abi' in df.columns:
            abi_map_from_df = df.loc[~pd.isna(df.abi), ['contract_address', 'abi']] \
                .drop_duplicates(subset=['contract_address']) \
                .set_index('contract_address') \
                .squeeze() \
                .to_dict()
            self._update_abi_cache(abi_map_from_df)
            df.drop('abi', axis=1, inplace=True)

        if abi_map is not None:
            self._update_abi_cache(abi_map)

        if self.is_multiprocessing:
            # only read cache
            self._preheat_abi_and_contract(df.contract_address.unique())

    def _update_abi_cache(self, abi_map: Dict[str, Any]):
        for address, abi in abi_map.items():
            if address not in self.abi_cache:
                self.abi_cache[address] = json.loads(abi)

    def _load_abi(self, address: str):
        if address not in self.abi_cache:
            self.abi_cache[address] = json.loads(etherscan.get_contract_abi(address))
        return self.abi_cache[address]

    def _load_contract(self, address: str):
        if self.is_multiprocessing:
            return self.w3.eth.contract(abi=self._load_abi(address))
        if address not in self.contract_cache:
            self.contract_cache[address] = self.w3.eth.contract(
                abi=self._load_abi(address)
            )
        return self.contract_cache[address]

    def _preheat_abi_and_contract(self, addresses: List[str]):
        for address in addresses:
            self._load_contract(address)

    def _parse_function_input_with_abi(
            self,
            contract_address: str,
            input_data: str,
    ) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        """
        :return: function_name: str,
                 input_schema: List[Dict[str, Any]],
                 input_params: Dict[str, Any]
        """
        try:
            abi = self._load_abi(contract_address)
            contract = self._load_contract(contract_address)

            func_obj, input_params = contract.decode_function_input(input_data)

            func_name = vars(func_obj)['fn_name']

            input_schema = \
                [i for i in abi if 'name' in i and i['name'] == func_name and i['type'] == 'function'][0]['inputs']
        except Exception as ex:
            self.logger.warning("parsing input with abi failed: %s", ex)
            return '', [], {}

        return func_name, input_schema, input_params

    def _parse_event_data_with_abi(
            self,
            contract_address: str,
            topics: List[str],
            data: Optional[str]
    ) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        """
        :return: event_name: str,
                 input_schema: List[Dict[str, Any]],
                 input_params: Dict[str, Any]
        """
        try:
            if data is None:
                raise Exception('The data is empty.')

            abi = self._load_abi(contract_address)
            contract = self._load_contract(contract_address)

            event_inputs: Optional[List[Dict[str, Any]]] = None
            event_name: Optional[str] = None

            for event_abi in abi:
                if 'name' in event_abi and topics[0] == encode_hex(event_abi_to_log_topic(event_abi)):
                    event_inputs = event_abi['inputs']
                    event_name = event_abi['name']

            if event_inputs is None or event_name is None:
                raise Exception('Could not find any event with matching selector.')

            event = contract.events[event_name]().processLog({
                'data': data,
                'topics': [decode_hex(topic) for topic in topics],
                # Placeholder only
                'logIndex': '',
                'transactionIndex': '',
                'transactionHash': '',
                'address': '',
                'blockHash': '',
                'blockNumber': ''
            })
        except Exception as ex:
            self.logger.warning("parsing event data with abi failed: %s", ex)
            return '', [], {}

        return event.event, event_inputs, event.args

    @staticmethod
    def _calculate_trace_index(
            block_number: int,
            tx_index: int,
            trace_address: str
    ):
        return f'{int(block_number)}_{int(tx_index)}_{trace_address}'

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
                component_schema = [i for i in input_schema
                                    if 'name' in i and 'type' in i and i['name'] == key and i['type'] == 'tuple'][0][
                    'components']
                result_dict[key] = Transformer._tuple_to_dict(value, component_schema)

        return result_dict
