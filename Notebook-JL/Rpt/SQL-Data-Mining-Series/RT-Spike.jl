using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
#setTable(table)

include("../../../Lib/Include-Package.jl")

SP = ShowParamsInit()
ShowParamsValidate(SP)

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

t1DF = select("""select count(*) from $(urlNormal.resourceTable) where timestamp between $(timeNormal.startTimeMsUTC) and $(timeNormal.endTimeMsUTC))""")
beautifyDF(t1DF)

t2DF = select("""select count(*) from $(urlSpike.resourceTable)  where timestamp between  $(timeSpike.startTimeMsUTC) and  $(timeSpike.endTimeMsUTC))""")
beautifyDF(t2DF)

requestCountByGroupPrintTable(timeNormal,urlNormal,SP,"Normal")
requestCountByGroupPrintTable(timeSpike,urlSpike,SP,"Spike")

nonCacheRequestCountByGroupPrintTable(timeNormal,urlNormal,SP,"Normal")
nonCacheRequestCountByGroupPrintTable(timeSpike,urlSpike,SP,"Spike")

cacheHitRatioPrintTable(timeNormal,urlNormal,"Normal")
cacheHitRatioPrintTable(timeSpike,urlSpike,"Spike")

;
