from grocery.utils.io import init_spark
from pyspark.sql import Row


def test_init_spark():
    spark = init_spark()
    df = spark.createDataFrame([{'colA': 123}])
    assert df.collect() == [Row(colA=123)]
