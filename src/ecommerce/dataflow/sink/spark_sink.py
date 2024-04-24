import pandas as pd
from pyspark.sql import SparkSession

from ecommerce.env.config_info import Configuration
from ecommerce.dataflow.sink.base_sink import BaseSink


class SparkSink(BaseSink):
    def __init__(self, spark: SparkSession, df: pd.DataFrame, config: Configuration):
        self._spark = spark
        self._df = df
        self._target_table = config.target_tables.customer_product_orders.table_name
        self._target_path = config.root_path.joinpath(config.target_tables.customer_product_orders.path)
        self._partition = config.target_tables.customer_product_orders.partition

    def output(self) -> None:
        # df = self._spark.createDataFrame(self._df)
        df = self._spark.createDataFrame(self._df)
        writer = df.write.mode("overwrite").format("parquet")
        if self._partition:
            writer = writer.partitionBy(self._partition)
        writer.save(self._target_path.joinpath(self._target_table).as_uri())
