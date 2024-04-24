from pathlib import Path

from pydantic import BaseModel, Field


class TableInfo(BaseModel):
    table_name: str = Field(default=None, description="table name.")
    path: str = Field(default=None, description="data file path.")
    type: str = Field(default=None, description="execution type.")
    partition: str = Field(default=None, description="table partition.")


class SourceTableInfo(BaseModel):
    products: TableInfo
    customers: TableInfo
    order_items: TableInfo
    order_payments: TableInfo
    orders: TableInfo
    order_reviews: TableInfo


class TargetTableInfo(BaseModel):
    customer_product_orders: TableInfo


class Configuration(BaseModel):
    country_name: str = Field(default="")
    root_path: Path = Field(default="")
    source_tables: SourceTableInfo
    target_tables: TargetTableInfo
