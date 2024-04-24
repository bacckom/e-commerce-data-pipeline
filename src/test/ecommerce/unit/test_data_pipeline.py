import pytest

from ecommerce.env.environment import Environment
from ecommerce.dataflow.pipeline.sales_data_pipeline import SalesDataPipeline
from ecommerce.data_schema.customer_product_orders import CustomerProductOrderObject


@pytest.fixture
def customer_product_orders() -> CustomerProductOrderObject:
    environment = Environment()
    pipeline = SalesDataPipeline(environment)
    result_df: CustomerProductOrderObject = pipeline.run()
    return result_df


def test_sales_data_pipeline(customer_product_orders: CustomerProductOrderObject) -> None:
    assert not customer_product_orders.df.empty, "Data pipeline is not correct"
