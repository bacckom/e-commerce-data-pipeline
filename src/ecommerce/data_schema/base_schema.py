from abc import ABC

import pandas as pd
import pandera as pa


class BaseSchema(pa.DataFrameModel):
    """Base data_schema for data_schema validation."""


class BaseObject(ABC):
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = pd.DataFrame(BaseSchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        return self._df.copy()
