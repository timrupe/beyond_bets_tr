from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from beyond_bets.base.dataset import Dataset


spark = SparkSession.builder.getOrCreate()


class Numbers(Dataset):

    def __init__(self):
        super().__init__()
        self._name: str = "Numbers"

    def reader(self, n: int = 10, **kwargs) -> DataFrame:

        _n: int = n
        return spark.createDataFrame(
            [(i,) for i in range(1, n + 1)],
            schema=StructType(
                [
                    StructField("n", IntegerType(), False),
                ]
            ),
        )

    def writer(self, df: DataFrame, **kwargs: dict[str, any]) -> DataFrame:
        return self.reader(**kwargs)
