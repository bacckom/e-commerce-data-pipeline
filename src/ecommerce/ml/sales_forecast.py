import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from ecommerce.env.environment import Environment
from ecommerce.dataflow.landing.agg.customer_product_order_loader import ProductOrdersLoader


class SalesForecaster:

    def __init__(self, conf: Environment = None, product_sales_df: ProductOrdersLoader = None) -> None:
        self._conf = conf
        self._orders_df: pd.DataFrame = product_sales_df

    def forecast(self) -> pd.DataFrame:
        data = self._orders_df

        encoder = LabelEncoder()
        data['product_id'] = encoder.fit_transform(data['product_id'])

        input_columns = ['order_purchase_timestamp', 'product_id', 'seller_id', 'customer_city', 'review_score']
        target_column = 'payment_value'

        X = data[input_columns]
        y = data[target_column]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        model = XGBRegressor()
        model.fit(X_train, y_train)

        next_7_days = pd.date_range(start=max(data['order_purchase_timestamp']), periods=7).to_pydatetime().tolist()
        next_7_days = [x.toordinal() for x in next_7_days]

        product_id = data['product_id'].iloc[0]
        seller_id = data['seller_id'].iloc[0]
        city = data['customer_city'].iloc[0]
        review_score = 0

        predictions = []
        for i in range(7):
            prediction = model.predict(pd.DataFrame({
                'order_purchase_timestamp': [next_7_days[i]],
                'product_id': [product_id],
                'seller_id': [seller_id],
                'customer_city': [city],
                'review_score': [review_score+i]
            }))
            predictions.append(prediction[0])

        return predictions
