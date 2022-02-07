import json
from typing import Dict, Any, Tuple, Optional, Union

import pandas as pd
from pandas import DataFrame
from web3 import Web3

w3 = Web3()


def _calculate_trace_index(block_number: int,
                           tx_index: int,
                           trace_address: Union[Optional[str], type(None)]):
    if isinstance(trace_address, type(None)) or trace_address is None:
        trace_address_str = ''
    else:
        trace_address_str = trace_address

    return f'{block_number}_{tx_index}_{trace_address_str}'


def _tuple_to_dict(raw_tuple: Tuple, component_schema: [Dict[str, Any]]) -> Dict[str, Any]:
    tuple_dict = {}
    for index, field_type in enumerate(component_schema):
        if field_type['type'] != 'tuple':
            tuple_dict[field_type['name']] = raw_tuple[index]
        else:
            tuple_dict[field_type['name']] = _tuple_to_dict(raw_tuple[index], field_type['components'])

    return tuple_dict


def _nested_tuple_dict_ser(
        raw_dict: Dict[str, Any],
        input_schema: [Dict[str, Any]],
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
            component_schema = list(
                filter(lambda x: 'name' in x and 'type' in x and x['name'] == key and x['type'] == 'tuple',
                       input_schema)
            )[0]['components']

            result_dict[key] = _tuple_to_dict(value, component_schema)

    return result_dict


def _parse_input_with_abi(input_data: str, abi: str) -> (str, [Dict[str, type]], Dict[str, Any]):
    """
    :return: function_name: str,
             input_schema: [Dict[str, typ]],
             input_params: Dict[str, Any]
    """
    abi_obj = json.loads(abi)
    contract = w3.eth.contract(abi=abi)
    func_obj, input_params = contract.decode_function_input(input_data)

    func_name = vars(func_obj)['fn_name']

    input_schema = \
        list(filter(lambda x: 'name' in x and x['name'] == func_name and x['type'] == 'function', abi_obj))[0]['inputs']

    return func_name, input_schema, input_params


def traces_to_func_call_df(
        df: DataFrame,
        # column name to alias
        alias: Optional[Dict[str, str]] = None,
        # contract address to abi
        abi_map: Optional[Dict[str, str]] = None
) -> DataFrame:
    if alias is not None:
        df = df.rename(alias)

    assert {'block_number', 'tx_index', 'trace_address', 'address', 'input'}.issubset(df.columns)

    has_abi_column = {'abi'}.issubset(df.columns)

    parsed_df: DataFrame = df.apply(
        lambda x: _parse_input_with_abi(x.input,
                                        x.abi if has_abi_column and not pd.isna(x.abi) else abi_map[x.address]),
        axis=1,
        result_type='expand'
    )
    parsed_df = parsed_df.rename(columns={0: 'func_name', 1: 'input_schema', 2: 'input_params'})

    df = pd.concat([df, parsed_df], axis=1)
    df['hash_index'] = df.apply(lambda x: _calculate_trace_index(x.block_number, x.tx_index, x.trace_address), axis=1)

    if has_abi_column:
        df.drop(['input', 'abi'], axis=1)
    else:
        df.drop(['input'], axis=1)

    distinct_func_map = df[['address', 'func_name', 'input_params']] \
        .groupby(['address', 'func_name']) \
        .count() \
        .to_dict()['input_params']

    result_df = None

    for (address, func_name) in list(distinct_func_map.keys()):
        sub_df: DataFrame = df.loc[(df['address'] == address) & (df['func_name'] == func_name)] \
            .copy(deep=True)
        sub_df['input_params'] = sub_df.apply(lambda x: _nested_tuple_dict_ser(
            raw_dict=x.input_params,
            input_schema=x.input_schema,
            append_obj={'hash_index': x.hash_index}
        ), axis=1)

        funcall_df = pd.json_normalize(sub_df['input_params'].values.tolist()).set_index('hash_index')
        funcall_df.columns = f'{address}.{func_name}.' + funcall_df.columns

        if result_df is None:
            result_df = funcall_df
        else:
            # outer join will append the index from result_df to the column of result.
            result_df = result_df.join(other=funcall_df,
                                       on='hash_index',
                                       how='outer').set_index('hash_index')

    return result_df.join(df[['hash_index', 'block_number', 'tx_index', 'trace_address']].set_index('hash_index'),
                          on='hash_index',
                          how='inner')
