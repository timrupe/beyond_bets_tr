from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class Dataset(ABC):
    def __init__(self):
        self._name: str = "<needs to be set>"

    @abstractmethod
    def reader(self, **kwargs: dict[str, any]) -> DataFrame:
        """Returns a dataframe representing the appropriate version
        of the dataset."""
        raise NotImplementedError

    @abstractmethod
    def writer(self, df: DataFrame, **kwargs: dict[str, any]) -> DataFrame:
        """Persists the input dataframe to the appropriate location. Returns
        a reader to the persisted version."""
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name
