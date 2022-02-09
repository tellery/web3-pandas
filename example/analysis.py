import sys
import os
current_file_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_file_dir, '..'))

import timeit
import pandas as pd
from pandas3 import Transformer


weth_contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'


def get_tmp_resource_path(file_name: str) -> str:
    return os.path.join(current_file_dir, '..', 'tmp', file_name)


def analysis_weth_withdraw_value():
    start = timeit.default_timer()

    transformer = Transformer()

    df = pd.read_csv(get_tmp_resource_path('trace.csv'))
    weth_df = df.loc[df['to_address'] == weth_contract_address].copy(deep=True)

    func_call_df = transformer.traces_to_func_call_df(
        df=weth_df,
        alias={'transaction_index': 'tx_index', 'to_address': 'address'},
    )
    withdraw_value_col = f'{weth_contract_address}.withdraw.wad'

    agg_df = func_call_df[['block_number', withdraw_value_col]] \
        .dropna() \
        .groupby(['block_number']) \
        .agg({withdraw_value_col: sum}) \
        .squeeze()
    # .apply(Web3.fromWei, unit='ether') \

    print(agg_df.to_string())

    stop = timeit.default_timer()
    print('Time: ', stop - start)


if __name__ == "__main__":
    analysis_weth_withdraw_value()
