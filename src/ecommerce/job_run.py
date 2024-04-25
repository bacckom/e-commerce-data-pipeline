import pandas as pd
from ecommerce.env.environment import Environment
from ecommerce.ml.sales_forecast import SalesForecaster
from ecommerce.dataflow.pipeline.sales_data_pipeline import SalesDataPipeline
from ecommerce.engine.spark_engine import create_spark_session
from ecommerce.dataflow.landing.agg.customer_product_order_loader import ProductOrdersLoader

if __name__ == "__main__":
    env = Environment()

    """ Data Pipelines run to prepare data for ML """
    pipeline = SalesDataPipeline(env)
    pipeline.run()

    """ Load result data for ML """
    spark = create_spark_session()
    product_sales_df = ProductOrdersLoader(env.configs, spark).load().df

    """ Pass data for running ML forecasting """
    forecaster = SalesForecaster(env.configs, product_sales_df)
    predictions = forecaster.forecast()

    print(f'Sales prediction 7 days: {pd.DataFrame(predictions).to_string(index=True)}')

