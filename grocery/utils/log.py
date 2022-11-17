import logging

import logging as log
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

LOGGING_LEVEL = 20  # (logging.INFO)  # only showing info messages
# LOGGING_LEVEL = logging.WARNING  # only showing warning messages

FMT = "%(asctime)s: [%(levelname)s]: %(lineno)s: %(message)s"

DT_FMT = "%y-%m-%d %H:%M:%S"

logging.basicConfig(level=LOGGING_LEVEL, format=FMT, datefmt=DT_FMT)

# disable logging for this module, causes this:
# >>> 20-06-12 04:09:29: [INFO]: /databricks/_spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py:2325: Received command c on object id p0
logging.getLogger("py4j").setLevel(logging.ERROR)

log = logging


def entity_log(f):
    """
    Decorator that can be used with any object that inherits the Entity class
    """

    def wrapper(*args, **kwargs):
        # Get class info
        self = args[0]
        spark = self._spark
        log.basicConfig(level=self._logging['level'], format=self._logging['format'],
                        datefmt=self._logging['datefmt'])
        log_path = self._logging['logging_path']
        process_name = f"{self.__class__.__name__}.{f.__name__}"
        message_start = "*" * 10 + f"  START PROCESS [{process_name}] " + '*' * 10
        message_end = "*" * 10 + f" FINISH PROCESS [{process_name}] " + "*" * 10

        log.info(message_start)
        start = datetime.now()
        val = f(*args, **kwargs)
        end = datetime.now()
        log.info(message_end)

        # Save results
        track_schema = StructType([StructField("process", StringType(), True),
                                   StructField("nodes", IntegerType(), True),
                                   StructField("start_time_utc", TimestampType(), True),
                                   StructField("end_time_utc", TimestampType(), True),
                                   StructField("elapsed_mins", DoubleType(), True)])
        delta = end - start
        elapsed_mins = delta.seconds / 60.
        nodes = spark.sparkContext.defaultParallelism
        write_df = spark.createDataFrame([[process_name, nodes, start, end, elapsed_mins]], track_schema)
        (write_df
         .repartition(4)
         .write.mode("append")
         .option("overwriteSchema", "true")
         .option("compression", "gzip")
         .parquet(log_path))
        return val

    return wrapper
