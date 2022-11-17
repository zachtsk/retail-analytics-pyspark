from pyspark.sql import functions as F, Window as W
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from grocery.abstract.entity import Entity
from grocery.jobs.standardize_files import StandardizeFiles
from grocery.utils.io import read_spark_data, write_spark_data
from grocery.utils.log import log, entity_log


class MakeStoreClusters(Entity):

    @property
    def standardize_files(self) -> StandardizeFiles:
        # instantiate dependency module
        return StandardizeFiles(self._spark,
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
        # Set IO data
        self.read_io = self.get_data_io(self._jobs['make_store_banner_clusters']['in_schema'],
                                        self._jobs['make_store_banner_clusters']['in_source'])
        self.write_io = self.get_data_io(self._jobs['make_store_banner_clusters']['out_schema'],
                                         self._jobs['make_store_banner_clusters']['out_source'])
        # Set spark transformation info
        self.agg_params = self._jobs['make_store_banner_clusters']['exprs']
        self.aggs = [F.expr(x['sql']).alias(x['alias']) for x in self.agg_params]
        self.agg_cols = [x['alias'] for x in self.agg_params]
        self.groups = self._jobs['make_store_banner_clusters']['groupby']
        self.cluster_name = self._jobs['make_store_banner_clusters']['cluster_col_name']

    @entity_log
    def make_store_agg(self, df):
        """
        Inputs:
        @exprs -> SQL expressions to use in aggregation
        @groupby -> columns to groupby in aggregation
        """

        df = df.groupby(*self.groups).agg(*self.aggs)
        return df

    @entity_log
    def make_scaled_features(self, df):
        # Prepare columns
        assembler = VectorAssembler(inputCols=self.agg_cols, outputCol="features")
        df_vector = assembler.transform(df)
        # For each column, set between 0,1
        MinMaxScalerizer = (
            MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("scaled_features")
        )
        df = MinMaxScalerizer.fit(df_vector).transform(df_vector)
        return df

    @entity_log
    def make_clusters(self, df):
        # Persist df since reused
        df.persist()

        # Evaluate clustering by computing Silhouette score
        evaluator = ClusteringEvaluator()

        # Silhouette scores
        scores = []
        for k in range(2, 10):
            kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol('scaled_features')
            model = kmeans.fit(df)

            # Make predictions
            predictions = model.transform(df)

            # Get Silhouette score
            silhouette = round(evaluator.evaluate(predictions), 2)
            scores.append((k, silhouette))

            log.info(f"Silhouette for {k} clusters => {silhouette}")

        scores_sorted = sorted(scores, key=lambda x: (x[1], x[0]), reverse=True)
        selected_clusters = scores_sorted[0][0]

        kmeans = KMeans().setK(selected_clusters).setSeed(1).setFeaturesCol('scaled_features')
        model = kmeans.fit(df)

        # Make predictions
        stores_with_cluster = model.transform(df)
        stores_with_cluster = stores_with_cluster.withColumnRenamed('prediction','store_cluster')
        return stores_with_cluster

    @entity_log
    def run(self):
        df = read_spark_data(self._spark, **self.read_io)
        df = self.make_store_agg(df)
        df = self.make_scaled_features(df)
        df = self.make_clusters(df)
        df = self.append_run_info_to_dataframe(df)
        write_spark_data(df, **self.write_io)
        return df

    @entity_log
    def run_safe(self):
        self.standardize_files.run()
        self.run()
