from functools import reduce
from pandas import pandas as pd

bi, ii, oi = 0, 2, 3


def pandas2(columns):
    df = pd.read_csv('data_test', sep=' ', names=list(range(columns)))
    df['dict2'] = df.apply(lambda v: {
                            v[x + int(v[oi])]: (int(v[bi]), float(v[x]))
                            for x in range(4 + int(v[ii]), 4 + int(v[ii]) + int(v[oi]))
                            if not (v[x + int(v[oi])] in v.iloc[4: 4 + int(v[ii])].values)
                            }, axis=1)
    ddd = reduce(lambda acc, b: {**acc, **b}, df['dict2'].values)
