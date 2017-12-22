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

query("""SELECT count(*) FROM $(urlNormal.rtView)""")

query("""SELECT count(*) FROM $(urlSpike.rtView)""")

requestCountByGroupSDMRS(timeNormal,urlNormal,"Normal")
requestCountByGroupSDMRS(timeSpike,urlSpike,"Spike")

nonCacheRequestCountByGroupSDMRS(timeNormal,urlNormal,"Normal")
nonCacheRequestCountByGroupSDMRS(timeSpike,urlSpike,"Spike")

cacheHitRatioSDMRS(timeNormal,urlNormal,"Normal")
cacheHitRatioSDMRS(timeSpike,urlSpike,"Spike")

q = query(""" drop view if exists $(urlNormal.rtView);""")
q = query(""" drop view if exists $(urlSpike.rtView);""")
;
