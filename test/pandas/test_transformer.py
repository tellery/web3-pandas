import unittest
from typing import AnyStr

import pandas as pd

import test.resources
from pandas3.transformer import Transformer

RESOURCE_GROUP = 'test_transformer'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.resources.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.resources.read_resource([RESOURCE_GROUP], file_name)


class TestTransformer(unittest.TestCase):
    transformer = Transformer()

    def test_traces_to_func_call_df_with_abi(self):
        df = pd.read_csv(_get_resource_path('traces1.csv'))
        df = self.transformer.traces_to_func_call_df(df=df)

        self.assertEqual(
            first=df.loc['11565326_60_', '0x1D5D9A2DDA0843ED9D8A9BDDC33F1FCA9F9C64A0.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565322_55_', '0x8B1C079F8192706532CC0BF0C02DCC4FF40D045D.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565108_139_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.tokenURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4'
        )

        self.assertEqual(
            first=df.loc['11565303_29_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4'
        )

    def test_traces_to_func_call_df_without_abi(self):
        df = pd.read_csv(_get_resource_path('traces2.csv'))
        abi = _read_resource('trace_test_abi.json')
        df = self.transformer.traces_to_func_call_df(df=df, abi_map={'0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7': abi})

        self.assertEqual(
            first=df.loc['11565108_139_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.tokenURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4'
        )

        self.assertEqual(
            first=df.loc['11565303_29_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4'
        )

    def test_traces_to_func_call_df_with_part_abi(self):
        df = pd.read_csv(_get_resource_path('traces3.csv'))
        abi = _read_resource('trace_test_abi.json')
        df = self.transformer.traces_to_func_call_df(df=df, abi_map={'0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7': abi})

        self.assertEqual(
            first=df.loc['11565326_60_', '0x1D5D9A2DDA0843ED9D8A9BDDC33F1FCA9F9C64A0.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565322_55_', '0x8B1C079F8192706532CC0BF0C02DCC4FF40D045D.transferOwnership.newOwner'],
            second='0xF8523c551763FE4261A28313015267F163de7541'
        )

        self.assertEqual(
            first=df.loc['11565108_139_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.tokenURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4'
        )

        self.assertEqual(
            first=df.loc['11565303_29_', '0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataURI'],
            second='https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4'
        )

    def test_logs_to_func_call_df_with_abi(self):
        df = pd.read_json(_get_resource_path('logs1.json'))
        abi = _read_resource('log_test_abi.json')
        df = self.transformer.logs_to_func_call_df(df=df, abi_map={'0x7be8076f4ea4a4ad08075c2508e481d6c946d12b': abi})

        self.assertEqual(
            first=df.loc['5779378_51', '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b.OrdersMatched.maker'],
            second='0x6Be4a7bbb812Bfa6A63126EE7b76C8A13529BDB8'
        )

        self.assertEqual(
            first=df.loc['5779474_83', '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b.OrdersMatched.taker'],
            second='0x0239769A1aDF4DeF9f07Da824B80B9C4fCB59593'
        )

        self.assertEqual(
            first=df.loc['5779612_122', '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b.OrdersMatched.metadata'],
            second=b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        )

        self.assertEqual(first=len(df), second=3)
