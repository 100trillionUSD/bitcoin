from functools import reduce
from pandas import pandas as pd

bi, ii, oi, columns = 0, 2, 3, 3882


def pandas2():
    df = pd.read_csv('data_test', sep=' ', names=list(range(columns)))
    df['dict2'] = df.apply(lambda v: {
                            v[x + int(v[oi])]: (int(v[bi]), float(v[x]))
                            for x in range(4 + int(v[ii]), 4 + int(v[ii]) + int(v[oi]))
                            if not (v[x + int(v[oi])] in v.iloc[4: 4 + int(v[ii])].values)
                            }, axis=1)
    return reduce(lambda acc, b: {**acc, **b}, df['dict2'].values)
