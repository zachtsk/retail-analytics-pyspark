import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F

from pgsrc.utils.log import entity_log


class Entity(ABC):

    def __init__(self,
                 spark: SparkSession,
                 config: dict,
                 version=None,
                 run_id=None,
                 save_config_snapshot=False,
                 **kwargs):
        super().__init__()
        self._spark = spark
        self._config = config
        self._logging = config['logging']
        self._data = config['data']
        self._jobs = config['jobs']
        if not version:
            self._version = config['version']
        else:
            self._version = version
        if not run_id:
            self._run_id = datetime.utcnow().strftime(config['run_info']['run_fmt'].format(**config))
        else:
            self._run_id = run_id
        self._run_history_folder = config['run_info']['run_history']
        self._save_config_snapshot = save_config_snapshot
        # Save snapshot
        if save_config_snapshot:
            self.write_config_snapshot()

    @entity_log
    def write_config_snapshot(self):
        run_filename = f"{self._run_id}__{self.__class__.__name__}.yml"
        run_filepath = os.path.join(self._run_history_folder, run_filename)
        Path(self._run_history_folder).mkdir(parents=True, exist_ok=True)
        with open(run_filepath, 'w') as f:
            f.write(yaml.dump(self._config))

    @entity_log
    def append_run_info_to_dataframe(self, df):
        df = df.withColumn('run_id', F.lit(self._run_id))
        return df

    def get_data_io(self, schema, source):
        return self._data[schema][source]

    @abstractmethod
    def run(self):
        """
        All classes should have a `run` entrypoint
        """
        pass
