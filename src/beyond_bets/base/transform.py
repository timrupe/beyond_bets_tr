from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from beyond_bets.base.dataset import Dataset
from typing import Union


class Transform(ABC):
    def __init__(self):
        self._name: str = "<needs to be set>"
        self._inputs: dict[str, Union[Dataset | Transform]] = dict()

    def _ingest(self):
        if not isinstance(self._inputs, dict):
            raise TypeError
        elif not len(self._inputs):
            raise ValueError("Transform must have at least 1 input!")

        for k, v in self._inputs.items():
            if not isinstance(k, str):
                raise TypeError
            elif isinstance(v, Dataset):
                setattr(self, k, v.reader())
            elif isinstance(v, Transform):
                setattr(self, k, v.result())
            else:
                raise TypeError(f"Invalid ingest object type {type(v)} for {k}")

    @abstractmethod
    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        """Responsible for any transformation logic."""
        raise NotImplementedError

    def result(self, **kwargs: dict[str, any]) -> DataFrame:
        self._ingest()
        return self._transformation(**kwargs)

    @property
    def name(self) -> str:
        return self._name
