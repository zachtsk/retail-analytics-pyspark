from pgsrc.abstract.entity import Entity
from pyspark.sql import functions as F
from pgsrc.jobs.make_store_clusters import MakeStoreClusters
from pgsrc.utils.log import entity_log
from pgsrc.utils.io import read_spark_data, write_spark_data


class MakeModelData(Entity):

    @property
    def make_store_clusters(self) -> MakeStoreClusters:
        # instantiate dependency module
        return MakeStoreClusters(self._spark,
                                 self._config,
                                 self._version,
                                 self._run_id,
                                 self._save_config_snapshot)

    def __init__(self,
                 spark,
                 config,
                 version=None,
                 run_id=None,
                 save_config_snapshot=False,
                 **kwargs):
        super().__init__(spark, config, version, run_id, save_config_snapshot, **kwargs)
        # IO
        self.trx_io = self.get_data_io(self._jobs['make_model_data']['trx_schema'],
                                       self._jobs['make_model_data']['trx_source'])
        self.store_cluster_io = self.get_data_io(self._jobs['make_model_data']['store_schema'],
                                                 self._jobs['make_model_data']['store_source'])
        self.write_model_data_io = self.get_data_io(self._jobs['make_model_data']['write_schema'],
                                                    self._jobs['make_model_data']['write_source'])
        # PySpark Agg Info
        self.sql_groups = self._jobs['make_model_data']['sql_groups']
        self.sql_exprs = self._jobs['make_model_data']['sql_exprs']
        self.sql_exprs_cols = [F.expr(v).alias(k) for k, v in self.sql_exprs.items()]

    @entity_log
    def run(self):
        # Load data
        trx = read_spark_data(self._spark, **self.trx_io)
        store_clusters = read_spark_data(self._spark, **self.store_cluster_io)
        store_clusters = store_clusters.select('store_num', 'store_cluster')

        # Join cluster data
        trx = trx.join(store_clusters, ['store_num'])

        # Grab non-promo features
        trx_agg = trx.groupby(*self.sql_groups).agg(*self.sql_exprs_cols)
        write_spark_data(trx_agg, **self.write_model_data_io)

    @entity_log
    def run_safe(self):
        self.make_store_clusters.run_safe()
        self.run()
