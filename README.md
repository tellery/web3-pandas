# web3-pandas: Pandas extension utils for Web3

## How to use

Transform the ethereum.trace dataframe to a function call dataframe.

```python
from pandas3.transformer import Transformer

transformer = Transformer()

df = pd.read_csv(_get_resource_path('data1.csv'))
df = transformer.traces_to_func_call_df(df=df)

# If the dataframe without abi or miss part of abi, 
# you need pass the abi json string.
df = pd.read_csv(_get_resource_path('data3.csv'))
abi = _read_resource('abi.json')
df = transformer.traces_to_func_call_df(df=df, abi_map={'0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7': abi})
```

Result example:

```
                 0x1D5D9A2DDA0843ED9D8A9BDDC33F1FCA9F9C64A0.transferOwnership.newOwner 0x8B1C079F8192706532CC0BF0C02DCC4FF40D045D.transferOwnership.newOwner                           0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.tokenURI                        0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataURI                                       0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.contentHash                 0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.data.metadataHash  0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.bidShares.prevOwner.value  0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.bidShares.creator.value 0xABEFBC9FD2F806065B4F3C237D4B59D9A97BCAC7.mint.bidShares.owner.value  block_number  tx_index  trace_address
hash_index                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
11565326_60_nan                             0xF8523c551763FE4261A28313015267F163de7541                                                                   NaN                                                                                     NaN                                                                                     NaN                                                                                                    NaN                                                                               NaN                                                                        NaN                                                                      NaN                                                                   NaN      11565326        60            NaN
11565322_55_nan                                                                    NaN                            0xF8523c551763FE4261A28313015267F163de7541                                                                                     NaN                                                                                     NaN                                                                                                    NaN                                                                               NaN                                                                        NaN                                                                      NaN                                                                   NaN      11565322        55            NaN
11565108_139_nan                                                                   NaN                                                                   NaN  https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4  https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4                 b'\xe3\xf4\xbd\x94\xa6\x1b\x85W1^\xcc&\x8c\x026\x97"\xddj\xb6]\x99xR\xc7\xc9O1=\x87|3'  b'\x9e\xf9\xd6\xd1\xdc<\xfb\xd36T\x95\x078l@36C0Puv_d-\xb4\x1f\xb5v\xf8\xf9\x9d'                                                                        0.0                                                                      0.0                                                 100000000000000000000      11565108       139            NaN
11565303_29_nan                                                                    NaN                                                                   NaN                                                                               /dev/null  https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4  b"\xe3\xb0\xc4B\x98\xfc\x1c\x14\x9a\xfb\xf4\xc8\x99o\xb9$'\xaeA\xe4d\x9b\x93L\xa4\x95\x99\x1bxR\xb8U"  b'\x9e\xf9\xd6\xd1\xdc<\xfb\xd36T\x95\x078l@36C0Puv_d-\xb4\x1f\xb5v\xf8\xf9\x9d'                                                                        0.0                                                                      0.0                                                 100000000000000000000      11565303        29            NaN
11565275_26_nan                                                                    NaN                                                                   NaN  https://ipfs.fleek.co/ipfs/bafybeickkbbjtoc3qpwpjxyzuiapwky52tfynnvy5c3u6dnr375a4ys3vu  https://ipfs.fleek.co/ipfs/bafybeifpxcq2hhbzuy2ich3duh7cjk4zk4czjl6ufbpmxep247ugwzsny4             b'x\xd9\xce\x97`g\xaa\xa5\xaa\x90$\xc1zrl\x9b\x12\x1d\x14\xab\xb7:\xc7dLb\xc7\nn\x1a\x9a_'  b'\x9e\xf9\xd6\xd1\xdc<\xfb\xd36T\x95\x078l@36C0Puv_d-\xb4\x1f\xb5v\xf8\xf9\x9d'                                                                        0.0                                                                      0.0                                                 100000000000000000000      11565275        26            NaN
```

The columns consist of the address of the contract, the function name, and the parameter name. If the parameter is
nested, you will get a name of more parts.

The hash indices consist of the block number, transaction index and the trace address, which uniquely identify a trace
record.

## ethereum-etl example

The [ethereum-etl](https://github.com/blockchain-etl/ethereum-etl) is a tool to convert blockchain data into
convenient formats like CSVs and relational databases. We can use it and *web3-pandas* to do some analysis of the
on-chain contract interaction.

Now, we will get 2000 blocks before Feb. 7, 2022, and analyze the total WETH withdraw over time.
The result would be a dataframe, where the index is block number and the column is the total of the WETH withdraw.

1. Get traces for 2000 blocks before Feb. 7, 2022

```shell
$ mkdir -p tmp
$ ethereumetl export_traces -p https://quiet-lingering-voice.quiknode.pro/a5bd71658a5dc6b895d3d04d48bf0e8271d4d9d7/ -o ./tmp/trace.csv -s 14155621 -e 14157621

......
2022-02-08 12:07:17,870 - CompositeItemExporter [INFO] - trace items exported: 1417764
```

2. Enter [etherscan](https://etherscan.io/address/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2#code) to copy contract ABI
   and save locally.

3. Run `python /example/analysis.py`, you need to take care about the path of the resource file.

And then you will get the dataframe:

```
             0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.withdraw.wad
block_number
14155621                                         1371007768230527738
14155623                                       257031256017724005109
14155624                                        19809274570083293112
14155625                                        11960921327985548345
14155627                                        29667275973210843191
14155628                                        13899160369482435379
14155629                                         9917493109506905911

......
```