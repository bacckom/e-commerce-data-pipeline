from abc import ABC

from ecommerce.data_schema.base_schema import BaseObject


class BasePipeline(ABC):
    def run(self) -> BaseObject:
        raise NotImplementedError("Not implement the service.")
