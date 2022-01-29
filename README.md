# web3-pandas: Pandas extension utils for Web3

## How to use

Transform the ethereum.trace dataframe to a function call dataframe.

```python
from pandas3.transformer import traces_to_func_call_df

df = pd.read_csv(_get_resource_path('data1.csv'))
df = traces_to_func_call_df(df=df)
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