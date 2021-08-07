def print_partitions(ddf):
    for i in range(ddf.npartitions):
        print(ddf.partitions[i].compute())


def print_dtypes(ddf):
    my_dictionary = ddf.dtypes.to_dict()
    my_dictionary = {k: str(my_dictionary[k]) for k, v in my_dictionary.items()}
    print(my_dictionary)
