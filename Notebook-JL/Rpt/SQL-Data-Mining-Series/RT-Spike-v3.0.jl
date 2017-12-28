using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
#setTable(table)

include("../../../Lib/Include-Package-v2.1.jl")

# Create temp tables for Normal timeframe and Spike timeframe

urlNormal = UrlParamsInit()
urlSpike = UrlParamsInit()
urlCache = UrlParamsInit()
urlSummary = UrlParamsInit()

urlNormal.resourceTable = tableRt
urlNormal.rtView = "$(table)_rtnormal_view_prod"

urlSpike.resourceTable = tableRt
urlSpike.rtView = "$(table)_rtspike_view_prod"

timeNormal = anyTimeVar(2017,6,7,11,49,2017,6,7,11,59)
timeSpike  = anyTimeVar(2017,6,8,11,49,2017,6,8,11,59)

query("""create or replace view $(urlNormal.rtView) as (select * from $(urlNormal.resourceTable) where "timestamp" between $(timeNormal.startTimeMsUTC) and $(timeNormal.endTimeMsUTC))""")
query("""create or replace view $(urlSpike.rtView)  as (select * from $(urlSpike.resourceTable)  where "timestamp" between  $(timeSpike.startTimeMsUTC) and  $(timeSpike.endTimeMsUTC))""")

t1DF = query("""SELECT count(*) FROM $(urlNormal.rtView)""")
beautifyDF(t1DF)

t2DF = query("""SELECT count(*) FROM $(urlSpike.rtView)""")
beautifyDF(t2DF)

requestCountByGroupPrintTable(timeNormal,urlNormal,"Normal")
requestCountByGroupPrintTable(timeSpike,urlSpike,"Spike")

nonCacheRequestCountByGroupPrintTable(timeNormal,urlNormal,"Normal")
nonCacheRequestCountByGroupPrintTable(timeSpike,urlSpike,"Spike")

cacheHitRatioPrintTable(timeNormal,urlNormal,"Normal")
cacheHitRatioPrintTable(timeSpike,urlSpike,"Spike")

q = query(""" drop view if exists $(urlNormal.rtView);""")
q = query(""" drop view if exists $(urlSpike.rtView);""")
;
