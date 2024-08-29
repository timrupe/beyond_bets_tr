from pyspark.sql import functions as F
from beyond_bets.transforms.player_daily import PlayerDaily
from beyond_bets.transforms.market_hourly import MarketHourly


pd = PlayerDaily()
pd.result().orderBy(F.col("total_bets").desc()).limit(10).show()

mh = MarketHourly()
mh.result().orderBy(F.col("total_bets").desc()).limit(10).show()
