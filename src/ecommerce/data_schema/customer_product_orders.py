import pandas as pd
import pandera as pa
from pandera import dtypes
from pandera.typing import Series

from ecommerce.data_schema.base_schema import BaseObject, BaseSchema


class CustomerProductOrderObject(BaseObject):
    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)
        self._df = pd.DataFrame(CustomerProductOrderSchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        return self._df.copy()


class CustomerProductOrderSchema(BaseSchema):
    product_id: Series[dtypes.String] = pa.Field(nullable=False)

    class Config:
        strict = False
        coerce = True