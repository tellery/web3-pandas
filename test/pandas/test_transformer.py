import unittest
from typing import AnyStr

import pandas as pd

import test.resources
from pandas3.transformer import traces_to_func_call_df

RESOURCE_GROUP = 'test_transformer'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.resources.get_resource_path([RESOURCE_GROUP], file_name)


class TestTransformer(unittest.TestCase):
    def test_traces_to_func_call_df(self):
        df = pd.read_csv(_get_resource_path('data1.csv'))
        df = traces_to_func_call_df(df=df)

        self.assertEqual(
            first=df.loc['11565326_60_nan', '0x1D5D9A2DDA0843ED9D8A9BDDC33F1FCA9F9C64A0.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565322_55_nan', '0x8B1C079F8192706532CC0BF0C02DCC4FF40D045D.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565108_139_nan', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.tokenURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4'
        )

        self.assertEqual(
            first=df.loc['11565303_29_nan', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4'
        )