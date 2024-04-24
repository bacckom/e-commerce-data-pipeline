from abc import ABC


class BaseSink(ABC):
    def output(self) -> None:
        raise NotImplementedError("Not implement the output.")
