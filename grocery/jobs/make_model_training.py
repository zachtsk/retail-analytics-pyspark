from pyspark.sql.types import StructField, FloatType, StructType, Row
import numpy as np
from sklearn.linear_model import LinearRegression

from grocery.abstract.entity import Entity
from pyspark.sql import functions as F

from grocery.jobs.make_model_data import MakeModelData
from grocery.utils.dataframe import collect_group
from grocery.utils.log import entity_log
from grocery.utils.io import read_spark_data, write_spark_data


class MakeModelTraining(Entity):

    @property
    def make_model_data(self) -> MakeModelData:
        # instantiate dependency module
        return MakeModelData(self._spark,
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
        self.model_data_io = self.get_data_io(self._jobs['make_model_training']['model_data_schema'],
                                              self._jobs['make_model_training']['model_data_source'])
        self.model_results_io = self.get_data_io(self._jobs['make_model_training']['write_schema'],
                                                 self._jobs['make_model_training']['write_source'])
        # Model data
        self.model_groups = self._jobs['make_model_training']['model_groups']
        self.model_target = self._jobs['make_model_training']['model_target']
        self.model_features = self._jobs['make_model_training']['model_features']
        self.model_result_exprs = self._jobs['make_model_training']['model_result_exprs']
        self.model_result_exprs_cols = [F.expr(v).alias(k) for k, v in self.model_result_exprs.items()]

    @entity_log
    def collect_model_matrix(self, df):
        data_cols = self.model_target + self.model_features
        return collect_group(df, self.model_groups, data_cols)

    @entity_log
    def train_model_get_coefs(self, df):
        train_model_schema = StructType([
            StructField("price_coef", FloatType(), False),
            StructField("display_coef", FloatType(), False),
            StructField("feature_coef", FloatType(), False),
            StructField("tpr_coef", FloatType(), False),
            StructField("intercept", FloatType(), False)
        ])

        @F.udf(train_model_schema)
        def train_model(ls):
            """
            For reference: https://scikit-learn.org/stable/modules/generated/sklearn.inspection.plot_partial_dependence.html#sklearn.inspection.plot_partial_dependence
            """

            # Set up process to train file
            try:
                data = np.array(ls)
                y = data[:, 0:1].astype('float64')
                X = data[:, 1:].astype('float64')
                m = LinearRegression()
                m.fit(X, y)
                price_coef, display_coef, feature_coef, tpr_coef = m.coef_[0]
                intercept = m.intercept_[0]
                assert price_coef < 0
                return Row('price_coef',
                           'display_coef',
                           'feature_coef',
                           'tpr_coef',
                           'intercept')(float(price_coef),
                                        float(display_coef),
                                        float(feature_coef),
                                        float(tpr_coef),
                                        float(intercept))
            except:
                return Row('price_coef',
                           'display_coef',
                           'feature_coef',
                           'tpr_coef',
                           'intercept')(0.0, 0.0, 0.0, 0.0, 0.0)

        df_result = df.withColumn('model', train_model('data'))
        df_result = df_result.select(
            'upc',
            'store_cluster',
            F.col('model.price_coef').alias('price_coef'),
            F.col('model.display_coef').alias('display_coef'),
            F.col('model.feature_coef').alias('feature_coef'),
            F.col('model.tpr_coef').alias('tpr_coef'),
            F.col('model.intercept').alias('intercept')
        )
        return df_result

    @entity_log
    def make_model_result(self, df_base, df_model_coefs):
        result = df_base.join(df_model_coefs, ['upc', 'store_cluster'])
        # Apply expression columns to get Yhat
        result = result.select(*result.columns + self.model_result_exprs_cols)
        return result

    @entity_log
    def run(self):
        model_data = read_spark_data(self._spark, **self.model_data_io)
        model_prep = self.collect_model_matrix(model_data)
        coefs = self.train_model_get_coefs(model_prep)
        # Join back to original data
        result = self.make_model_result(model_data, coefs)
        write_spark_data(result, **self.model_results_io)

    @entity_log
    def run_safe(self):
        self.make_model_data.run_safe()
        self.run()
