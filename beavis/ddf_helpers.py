import pandas as pd
import dask.dataframe as dd


def print_partitions(ddf):
    for i in range(ddf.npartitions):
        print(ddf.partitions[i].compute())


def print_dtypes(ddf):
    my_dictionary = ddf.dtypes.to_dict()
    my_dictionary = {k: str(my_dictionary[k]) for k, v in my_dictionary.items()}
    print(my_dictionary)


def create_ddf(rows, col_names, npartitions=2):
    transposed = list(map(list, zip(*rows)))
    res = {}
    for idx, col_name in enumerate(col_names):
        res[col_name] = transposed[idx]
    df = pd.DataFrame(res)
    return dd.from_pandas(df, npartitions=npartitions)
