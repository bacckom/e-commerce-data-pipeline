from pathlib import Path

import pandas as pd
import pytest

from ecommerce.data_schema.orders import OrderObject
from ecommerce.data_schema.product import ProductObject


@pytest.fixture
def data_source_path() -> Path:
    source_path = Path(__file__).resolve().parent.parent.parent.joinpath("data_files")
    return source_path


@pytest.fixture
def product(data_source_path: Path) -> ProductObject:
    fp = data_source_path.joinpath("olist_products_dataset.csv")
    df = pd.read_csv(fp)
    return ProductObject(df)


@pytest.fixture
def orders(data_source_path: Path) -> OrderObject:
    fp = data_source_path.joinpath("olist_orders_dataset.csv")
    df = pd.read_csv(fp)
    return OrderObject(df)


