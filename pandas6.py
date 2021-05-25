from functools import reduce
from pandas import pandas as pd
import dask.dataframe as ddf
import numpy as np
import time
import csv
bi, ii, oi = 0, 2, 3


# https://stackoverflow.com/questions/54432583/when-should-i-not-want-to-use-pandas-apply-in-my-code
def pandas6():
    # print(1, time.time())
    df = pd.read_csv('data_test_processed', sep=' ', names=list(range(4)),
                     dtype={k: int for k in range(4)})
    # print(2, time.time())

    d, b = {}, 0
    with open('data_test_small', 'r') as f:
        for l, i, o in zip(f, df[ii], df[oi]):
            v = l.split(' ')
            for x in range(4 + i, 4 + i + o):
                d[v[x + o]] = (b, float(v[x]))
            for k in v[4: 4 + i]:
                del d[k]
            b += 1

    # with open('data_test_small', 'r') as f:
    #     dfii, dfoi = df[ii].values, df[oi].values
    #     for l in f:
    #         i, o = dfii[b], dfoi[b]
    #         v = l.split(' ')
    #         for x in range(4 + i, 4 + i + o):
    #             d[v[x + o]] = (b, float(v[x]))
    #         for k in v[4: 4 + i]:
    #             del d[k]
    #         b += 1

    # with open('data_test_small', 'r') as f:
    #     d = {l.split(' ')[x + o]: (b, float(l.split(' ')[x]))
    #          for b, i, o, r, l in zip(df[bi], df[ii], df[oi], range(len(df[bi])), f)
    #          for x in range(4 + i, 4 + i + o)
    #          }
    #     print(3, time.time())
    #     f.seek(0, 0)
    #     print(4, time.time())
    #     for i, r, l in zip(df[ii], range(len(df[ii])), f):
    #         for y in range(4, 4 + i):
    #             del d[l.split(' ')[y]]
    # print(5, time.time())

    # with open('data_test_small', 'r') as f:
    #     ff = csv.reader(f, delimiter=' ')
    #     d = {v[x + o]: (b, float(v[x]))
    #          for b, i, o, r, v in zip(df[bi], df[ii], df[oi], range(len(df[bi])), ff)
    #          for x in range(4 + i, 4 + i + o)
    #          }
    #     # print(time.time())
    #     f.seek(0, 0)
    #     # print(time.time())
    #     for i, r, v in zip(df[ii], range(len(df[ii])), ff):
    #         for y in range(4, 4 + i):
    #             del d[v[y]]
    # # print(time.time())

    return d
