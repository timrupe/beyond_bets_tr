from pyspark.sql import functions as F
from beyond_bets.transforms.player_daily import PlayerDaily
from beyond_bets.transforms.market_hourly import MarketHourly
from beyond_bets.transforms.rollup import Rollup, RollupClass, RollupWindow
from beyond_bets.transforms.top_players import TopPlayers
from beyond_bets.transforms.bet_grader import BetGrader

## NOTE: I am assuming that only one of these are run at a time
##  If it was common to run multiple, we would want to create the datset ONCE and pass in via kwargs
## ALSO: if analysts were actually calling these, we would probably have each function be a wrapper to rollup.py

# ru = Rollup()

# # original PD call and one with roillup to compare
# pd = PlayerDaily()
# pd.result().orderBy(F.col("total_bets").desc()).limit(10).show()

# ru.result(rollupClass=RollupClass.player, rollupWindow=RollupWindow.daily).orderBy(F.col("total_bets").desc()).limit(10).show()

# # original MH call and one with roillup to compare
# mh = MarketHourly()
# mh.result().orderBy(F.col("total_bets").desc()).limit(10).show()

# ru.result(rollupClass=RollupClass.market, rollupWindow=RollupWindow.hourly).orderBy(F.col("total_bets").desc()).limit(10).show()

# #PlayerHourly call
# ru.result(rollupClass=RollupClass.player, rollupWindow=RollupWindow.hourly).orderBy(F.col("total_bets").desc()).limit(10).show()

# #MarketDaily call call
# ru.result(rollupClass=RollupClass.market, rollupWindow=RollupWindow.daily).orderBy(F.col("total_bets").desc()).limit(10).show()

# #PlayerMarkerDaily call
# ru.result(rollupClass=RollupClass.playermarket, rollupWindow=RollupWindow.daily).orderBy(F.col("total_bets").desc()).limit(10).show()

# #PlayerMarkerHourly call - not requested but it's a simple call
# ru.result(rollupClass=RollupClass.playermarket, rollupWindow=RollupWindow.hourly).orderBy(F.col("total_bets").desc()).limit(10).show()

# #TopPlayers call - uses rollup.py
# tp = TopPlayers()
# tp.get_top_players().orderBy(F.col("total_bets").desc()).limit(10).show()

#BetGrader call - does not use rollup.py
bg = BetGrader()
bg.result().orderBy(F.col("bet_grade").desc()).limit(10).show()