from functools import reduce
from typing import Optional

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from ecommerce.env.environment import Environment
from ecommerce.data_schema.customer_product_orders import CustomerProductOrderObject
from ecommerce.dataflow.pipeline.base_pipeline import BasePipeline
from ecommerce.dataflow.landing.raw.order_items_loader import OrderItemsLoader
from ecommerce.dataflow.landing.raw.order_loader import OrdersLoader
from ecommerce.dataflow.landing.raw.order_payments_loader import OrderPaymentsLoader
from ecommerce.dataflow.landing.raw.product_loader import ProductLoader
from ecommerce.dataflow.landing.raw.customer_loader import CustomerLoader
from ecommerce.dataflow.landing.raw.order_reviews_loader import OrderReviewsLoader
from ecommerce.dataflow.sink.spark_sink import SparkSink
from ecommerce.engine.spark_engine import create_spark_session
from pyspark.sql import SparkSession


class SalesDataPipeline(BasePipeline):
    def __init__(self, environment: Optional[Environment] = None):
        self.configs = environment.configs if environment else Environment().configs

    def run(self) -> CustomerProductOrderObject:
        configs = self.configs
        spark = create_spark_session()

        """ Landing data..."""
        product = ProductLoader(configs, spark).load().df
        customer = CustomerLoader(configs, spark).load().df
        order = OrdersLoader(configs, spark).load().df
        order_items = OrderItemsLoader(configs, spark).load().df
        order_payments = OrderPaymentsLoader(configs, spark).load().df
        order_reviews = OrderReviewsLoader(configs, spark).load().df

        """ Transform data..."""
        customer_product_order = self._merge_customer_product_orders(product, customer, order, order_items, order_payments, order_reviews)
        cleaned_data = self._clean_data(customer_product_order)
        final_data = self._dimension_reduction(cleaned_data)

        """ Sink data..."""
        SparkSink(spark, final_data, configs).output()
        return CustomerProductOrderObject(final_data)


    def _clean_data(self, customer_product_order: pd.DataFrame) -> pd.DataFrame:
        spark = create_spark_session()
        """ use head 50000 for testing purpose"""
        s_df = spark.createDataFrame(customer_product_order.head(20000))
        s_df.dropna(subset=["order_purchase_timestamp", "product_id", "seller_id", "customer_city", "payment_value",
                            "review_score"])
        s_df.drop_duplicates(
            subset=["order_purchase_timestamp", "product_id", "seller_id", "customer_city", "payment_value",
                    "review_score"])
        return s_df.toPandas()


    def _merge_customer_product_orders(
        self,
        product: pd.DataFrame,
        customer: pd.DataFrame,
        order: pd.DataFrame,
        order_items: pd.DataFrame,
        order_payments: pd.DataFrame,
        order_reviews: pd.DataFrame
    ) -> pd.DataFrame:
        dfs = [order, order_items, order_payments, order_reviews]
        orders = reduce(lambda left, right: pd.merge(left, right, on="order_id"), dfs)
        product_order = pd.merge(orders, product, on="product_id")
        customer_product_order = pd.merge(product_order, customer, on="customer_id")
        return customer_product_order


    def _dimension_reduction(self, sales: pd.DataFrame) -> pd.DataFrame:
        # pick up factors for ML models
        df = sales[["order_purchase_timestamp", "product_id", "seller_id", "customer_city", "payment_value", "review_score"]]
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
        df['order_purchase_timestamp'] = df['order_purchase_timestamp'].apply(
            lambda x: x.toordinal())
        # converting product_id's to integer values
        encoder = LabelEncoder()
        df['seller_id'] = encoder.fit_transform(df['seller_id'])
        df['customer_city'] = encoder.fit_transform(df['customer_city'])
        return df

