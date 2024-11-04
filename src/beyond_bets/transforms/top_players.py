from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from beyond_bets.transforms.rollup import Rollup, RollupClass, RollupWindow
from pyspark.sql.window import Window


class TopPlayers():

    def __init__(self):
        super().__init__()
        self._name: str = "TopPlayersOfTheWeek"

    def get_top_players(self) -> DataFrame:
        ru = Rollup()

        window = Window.orderBy(F.asc("total_bets"))

        df = ru.result(rollupClass=RollupClass.player, rollupWindow=RollupWindow.pastWeek).where("pastWeek = true").drop("pastWeek")

        return(
            df.withColumn("player_percentile", F.percent_rank().over(window)).where("player_percentile > 0.9")
        )