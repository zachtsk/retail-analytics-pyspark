import shutil

from pgsrc.utils.io import init_spark, read_spark_data, write_spark_data


def test_read_df():
    test_file_path = '/datalake/raw/dh_transactions.csv.gz'
    filetype = 'csv.gz'
    spark = init_spark()
    df = read_spark_data(spark, test_file_path, filetype)
    assert len(df.collect()) == 524950


def test_write_df():
    in_file_path = '/datalake/raw/dh_transactions.csv.gz'
    filetype = 'csv.gz'
    spark = init_spark()
    df = read_spark_data(spark, in_file_path, filetype)
    try:
        out_file_path = '/datalake/tmp/dh_transactions.parquet'
        out_filetype = 'parquet'
        write_spark_data(df, out_file_path, out_filetype)
        shutil.rmtree(out_file_path)
    except:
        print(f"Could not save dataframe to [{out_file_path}]")