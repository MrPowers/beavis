import pandas as pd
import dask.dataframe as dd
import numpy


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


def memory_by_partition(ddf):
    return ddf.map_partitions(lambda x: x.memory_usage(deep=True).sum()).compute()


def partition_report(ddf):
    series = memory_by_partition(ddf)
    total = series.count()
    print(f"Total number of partitions: {total}")
    total = total.astype(numpy.float64)
    lt_1kb = series.where(lambda x : x < 1000).count()
    lt_1kb_percentage = '{:.1%}'.format(lt_1kb/total)
    lt_1mb = series.where(lambda x : x < 1000000).count()
    lt_1mb_percentage = '{:.1%}'.format(lt_1mb/total)
    gt_1gb = series.where(lambda x : x > 1000000000).count()
    gt_1gb_percentage = '{:.1%}'.format(gt_1gb/total)
    print(f"Num partitions < 1 KB: {lt_1kb} ({lt_1kb_percentage})")
    print(f"Num partitions < 1 MB: {lt_1mb} ({lt_1mb_percentage})")
    print(f"Num partitions > 1 GB: {gt_1gb} ({gt_1gb_percentage})")
