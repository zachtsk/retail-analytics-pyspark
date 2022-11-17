from pyspark.sql import functions as F


def collect_group(df, group_cols, data_cols):
    df_data = df.groupby(*group_cols).agg(F.collect_list(F.struct(*data_cols)).alias('data'))
    return df_data


def explode_group(df, group_cols, data_cols):
    df_transform = df.select(*group_cols + [F.explode('data').alias('data')])
    df_transform = df_transform.select(*group_cols + [F.col(f'data.{x}') for x in data_cols])
    return df_transform
