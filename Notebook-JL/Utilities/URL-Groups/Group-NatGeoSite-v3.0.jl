using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)
#TV = weeklyTimeVariables(days=3)
TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "Nat Geo Site"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.showLines = 10
ShowParamsValidate(SP)

dumpDataFieldsV2Workflow(TV,UP,SP)
;