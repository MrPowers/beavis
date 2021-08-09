import pandas as pd


def create_pdf(rows, col_names):
    transposed = list(map(list, zip(*rows)))
    res = {}
    for idx, col_name in enumerate(col_names):
        res[col_name] = transposed[idx]
    return pd.DataFrame(res)
