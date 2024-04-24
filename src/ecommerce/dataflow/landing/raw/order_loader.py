from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from ecommerce.env.config_info import Configuration
from ecommerce.data_schema.orders import OrderObject
from ecommerce.dataflow.landing.base_loader import BaseLoader


class OrdersLoader(BaseLoader):
    def __init__(
        self, configs: Configuration, spark: Optional[SparkSession] = None
    ) -> None:
        """If spark is None, then use Pandas to read local file."""
        self._spark = spark
        self._table_name = configs.source_tables.orders.table_name
        self._path = configs.root_path.joinpath(configs.source_tables.orders.path)
        self._type = configs.source_tables.orders.type

    def load(self) -> OrderObject:
        df = (
            self._spark.read.table(self._table_name).toPandas()
            if self._type != "local"
            else pd.read_csv(self._path)
        )
        return OrderObject(df)
