from functools import reduce
from pandas import pandas as pd
import dask.dataframe as ddf
import numpy as np
import time
bi, ii, oi, columns = 0, 2, 3, 3882


# https://stackoverflow.com/questions/54432583/when-should-i-not-want-to-use-pandas-apply-in-my-code
def pandas5():
    print(time.time())
    df = pd.read_csv('data_test_small', sep=' ', names=list(range(columns)),
                     dtype={k: int for k in range(4)}.update({k: str for k in range(4, columns)}))
    # Read as sparse table, saves RAM at the expense of time
    # df = ddf.read_csv('data_test_small', sep=' ', names=list(range(columns)),
    #                   dtype={k: np.int for k in range(4)}.update({k: 'object' for k in range(4, columns)}))
    # dtype = pd.SparseDtype('object')
    # df = df.astype(dtype).compute().reset_index(drop=True)
    # print(df)
    print(time.time())

    d = {}
    # for b, i, o, r in zip(df[bi], df[ii], df[oi], range(len(df[bi]))):
    #     b, i, o = int(b), int(i), int(o)
    #     for x in range(4 + i, 4 + i + o):
    #         d[df[x + o].iloc[r]] = (b, float(df[x].iloc[r]))
    #     for y in range(4, 4 + i):
    #         del d[df[y].iloc[r]]

    d = {df[x + o].iloc[r]: (b, float(df[x].iloc[r]))
         for b, i, o, r in zip(df[bi], df[ii], df[oi], range(len(df[bi])))
         for x in range(4 + i, 4 + i + o)
         }
    print(time.time())
    for i, r in zip(df[ii], range(len(df[ii]))):
        for y in range(4, 4 + i):
            del d[df[y].iloc[r]]
    print(time.time())

    # df['dict2'] = df.apply(lambda v: {
    #                         v[x + int(v[oi])]: (int(v[bi]), float(v[x]))
    #                         for x in range(4 + int(v[ii]), 4 + int(v[ii]) + int(v[oi]))
    #                         if not (v[x + int(v[oi])] in v.iloc[4: 4 + int(v[ii])].values)
    #                         }, axis=1)
    #
    # for cell in df['dict2'].values:
    #     d.update(cell)

    return d
