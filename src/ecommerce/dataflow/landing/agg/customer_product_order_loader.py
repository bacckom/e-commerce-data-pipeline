from typing import Optional

from pyspark.sql import SparkSession

from ecommerce.env.config_info import Configuration
from ecommerce.data_schema.customer_product_orders import CustomerProductOrderObject
from ecommerce.dataflow.landing.base_loader import BaseLoader


class ProductOrdersLoader(BaseLoader):
    def __init__(
        self, configs: Configuration, spark: Optional[SparkSession] = None
    ) -> None:
        """ Use Spark to read parquet result file."""
        self._spark = spark
        self._table_name = configs.target_tables.customer_product_orders.table_name
        self._path = configs.root_path.joinpath(configs.target_tables.customer_product_orders.path)

    def load(self) -> CustomerProductOrderObject:
        df = self._spark.read.parquet(self._path.joinpath(self._table_name).as_uri()).toPandas()
        return CustomerProductOrderObject(df)
