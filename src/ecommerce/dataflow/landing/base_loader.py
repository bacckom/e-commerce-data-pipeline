from abc import ABC

from ecommerce.data_schema.base_schema import BaseObject


class BaseLoader(ABC):
    def load(self) -> BaseObject:
        raise NotImplementedError("Not implement.")
