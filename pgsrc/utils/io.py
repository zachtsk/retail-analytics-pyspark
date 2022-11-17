import json
import os

from zipfile import ZipFile
from pgsrc.utils.log import log
from functools import reduce  # For Python 3.x

import yaml
from pyspark.sql import SparkSession, DataFrame, functions as F


def init_spark(app_name=""):
    spark = SparkSession.builder.getOrCreate()
    log.getLogger("py4j").setLevel(log.INFO)
    return spark


def print_schema(df):
    print(yaml.dump(json.loads(df.schema.json())))


def build_schema(spark, schema: dict):
    j = json.dumps(schema)
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(j).toDDL()


def read_spark_data(spark, filepath, filetype, schema=None, **kwargs):
    if filetype == 'csv.zip':
        filepath = extract_csv_from_zip(filepath)
        if schema:
            schema = build_schema(spark, schema)
            df = spark.read.csv(filepath, header=True, schema=schema)
        else:
            df = spark.read.csv(filepath, header=True)
        df = df.withColumn('src', F.lit(filepath))

    elif filetype in ('csv', 'csv.gz'):
        if schema:
            schema = build_schema(spark, schema)
            df = spark.read.csv(filepath, header=True, schema=schema)
        else:
            df = spark.read.csv(filepath, header=True)

    elif filetype in 'parquet':
        df = spark.read.parquet(filepath)

    return df


def write_spark_data(df, filepath, filetype='parquet', **kwargs):
    # Only writing parquets right now
    (df
     .write
     .mode('overwrite')
     .option("overwriteSchema", "true")
     .option("compression", "gzip")
     .parquet(filepath))
    log.info(f"Wrote file: {filepath}")
    return filepath


def extract_csv_from_zip(filepath):
    is_extracted = False
    basename = os.path.basename(filepath)
    filepath_root = filepath.replace(basename, '')
    new_filepath = os.path.join(filepath_root, basename.lower().replace('.zip', ''))
    # File has already been extracted
    if os.path.isfile(new_filepath):
        is_extracted = True
    # Try to extract file
    else:
        with ZipFile(filepath, 'r') as zip_object:
            list_of_file_names = [x for x in zip_object.namelist()
                                  if x.lower().endswith('.csv') and 'macosx' not in x.lower()]
            for fileName in list_of_file_names:
                zip_object.extract(fileName, filepath_root)
                is_extracted = True
    return new_filepath


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
