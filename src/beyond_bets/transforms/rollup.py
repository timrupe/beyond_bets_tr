from pyspark.sql import DataFrame, SparkSession
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from enum import Enum
from datetime import datetime, timedelta


spark = SparkSession.builder.getOrCreate()

class RollupClass(dict, Enum):
    player = {
        "displayName": "Player"
        ,"partitionColumn": "player_id"
        ,"groupByColumns": ["player_id"]
    }
    market = {
        "displayName": "Market"
        ,"partitionColumn": "market"
        ,"groupByColumns": ["market"]
    }
    playermarket = {
        "displayName": "PlayerMarket"
        ,"partitionColumn": "market"
        ,"groupByColumns": ["market", "player_id"]
    }

class RollupWindow(dict, Enum):
    daily = {
        "displayName": "Daily"
        ,"columnName": "date"
        ,"predicate": F.to_date(F.col("timestamp"))
    }
    hourly = {
        "displayName": "Hourly"
        ,"columnName": "hour"
        ,"predicate": F.date_trunc("hour", F.col("timestamp"))
    }
    pastWeek = {
        "displayName": "PastWeek"
        ,"columnName": "pastWeek"
        ,"predicate": F.col("timestamp") > datetime.today() - timedelta(days=7)
    }

class Rollup(Transform):

    def __init__(self):
        super().__init__()

        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:

        # TODO: validate kwargs

        rollupClass = kwargs["rollupClass"]
        rollupWindow = kwargs["rollupWindow"]

        self._name: str = rollupClass["displayName"] + rollupWindow["displayName"]

        groupByColumns = rollupClass["groupByColumns"] + [rollupWindow["columnName"]]

        return (
            self.bets.repartition(rollupClass["partitionColumn"])
            .withColumn(rollupWindow["columnName"], rollupWindow["predicate"])
            .groupBy(groupByColumns)
            .agg(F.sum(F.col("bet_amount")).alias("total_bets"))
        )
