from pgsrc.abstract.entity import Entity
from pydantic import PrivateAttr
from pyspark.sql import SparkSession

from pgsrc.utils.log import log, entity_log
from pgsrc.utils.io import read_spark_data, write_spark_data


class StandardizeFiles(Entity):
    _spark: SparkSession = PrivateAttr()

    def __init__(self,
                 spark,
                 config,
                 version=None,
                 run_id=None,
                 save_config_snapshot=False,
                 **kwargs):
        super().__init__(spark, config, version, run_id, save_config_snapshot, **kwargs)

    @entity_log
    def read_save_compress(self, read_io, write_io):
        df = read_spark_data(self._spark, **read_io)
        write_spark_data(df, **write_io)

    @entity_log
    def run(self):
        for f in self._jobs['standardize_files']['io_sources']:
            read_io = self.get_data_io(f['in_schema'], f['source'])
            write_io = self.get_data_io(f['out_schema'], f['source'])
            self.read_save_compress(read_io, write_io)
