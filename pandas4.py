from functools import reduce
from concurrent.futures import ThreadPoolExecutor

import dask
from pandas import pandas as pd
import dask.dataframe as ddf
import dask.bag as db

bi, ii, oi, columns = 0, 2, 3, 3882


def pandas4():
    with dask.config.set(pool=ThreadPoolExecutor(6)):
        # Dask is lazy, so creating this variable shouldn't matter
        df = pd.read_csv('data_test', sep=' ', names=list(range(columns)), dtype={k: str for k in range(columns)})
        # print(df.dtypes)
        # print(df.memory_usage().sum() / 1e3)
        # dtype = pd.SparseDtype(str)
        # df = df.astype(dtype)
        # print(df.dtypes)
        # print(df.memory_usage().sum() / 1e3)
        # return db.from_delayed(df.apply(lambda v: {
        #                    v[x + int(v[oi])]: (int(v[bi]), float(v[x]))
        #                    for x in range(4 + int(v[ii]), 4 + int(v[ii]) + int(v[oi]))
        #                    if not (v[x + int(v[oi])] in v.iloc[4: 4 + int(v[ii])].values)
        #                }, axis=1, meta=('v', object))) \
        #          .fold(lambda acc, b: {**acc, **b}) \
        #          .to_delayed()
        return reduce(lambda acc, b: {**acc, **b},
                       df.apply(lambda v: {
                           v[x + int(v[oi])]: (int(v[bi]), float(v[x]))
                           for x in range(4 + int(v[ii]), 4 + int(v[ii]) + int(v[oi]))
                           if not (v[x + int(v[oi])] in v.iloc[4: 4 + int(v[ii])].values)
                       }, axis=1).values)
