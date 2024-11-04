from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()


class BetGrader(Transform):

    def __init__(self):
        super().__init__()
        self._name: str = "PlayerDaily"

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:

        df = self.bets.repartition("market")

        seconds_in_15_mins = 900

        # NOTE that we are NOT counting any bets made at the exact same time
        window = Window.partitionBy("market").orderBy(F.unix_timestamp(F.col("timestamp"))).rangeBetween(-seconds_in_15_mins, Window.currentRow - 1)
        
        return (
            df.withColumn("avg_other_bets", F.avg(F.col("bet_amount")).over(window))
            .withColumn("sum_other_bets", F.sum(F.col("bet_amount")).over(window))
            .withColumn("num_other_bets", F.count(F.col("bet_amount")).over(window))
            .withColumn("bet_grade", F.col("bet_amount") / F.col("avg_other_bets"))
        )
