from ecommerce.env.environment import Environment
from ecommerce.dataflow.pipeline.sales_data_pipeline import SalesDataPipeline
from ecommerce.data_schema.customer_product_orders import CustomerProductOrderObject


def test_sales_data_pipeline() -> None:
    environment = Environment()
    pipeline = SalesDataPipeline(environment)
    result_df: CustomerProductOrderObject = pipeline.run()
    assert not result_df.df.empty
