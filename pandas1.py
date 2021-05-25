from pandas import pandas as pd
import dask.dataframe as ddf

bi, ii, oi, columns = 0, 2, 3, 3882


def build_dict(v):
    # print(v)
    b, i, o = int(v[bi]), int(v[ii]), int(v[oi])
    not_allowed = v.iloc[4: 4 + i].values
    res = {v[x + o]: (b, float(v[x]))
           for x in range(4 + i, 4 + i + o)
           if not (v[x + o] in not_allowed)
           }
    # res = {}
    # for x in range(4 + i, 4 + i + o):
    #     key = v[x + o]
    #     if not(key in not_allowed):
    #         res[key] = (b, float(v[x]))

    return res


def pandas1():
    data = ddf.read_csv('data_test', sep=' ', names=list(range(columns)), dtype={k: str for k in range(columns)})
    # Create sparse dataframe as most of our cells are NaN
    # sdf = df.astype(pd.SparseDtype("string", ''))
    dtype = pd.SparseDtype(str)
    df = data.astype(dtype).compute().reset_index(drop=True)
    # df = data.map_partitions(lambda part: part.to_sparse(fill_value=0)).compute().reset_index(drop=True)
    print(df.head())
    df['dict'] = df.apply(lambda row: build_dict(row), axis=1)
    return {k: v for d in df['dict'].to_numpy() for k, v in d.items()}
