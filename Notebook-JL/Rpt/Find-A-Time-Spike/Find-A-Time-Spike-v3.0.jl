## Tables and Data Source setup

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
;

include("../../Lib/Include-Package-v2.1.jl")
;

#TV = timeVariables(2017,6,21,14,0,2017,6,21,14,59)
#TV = weeklyTimeVariables(days=2)
TV = yesterdayTimeVariables()
;

UP = UrlParamsInit("Find_A_Time_Spike")
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "News Article"   #productPageGroup
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
ShowParamsValidate(SP)

;

findATimeSpikeWorkflow(TV,UP,SP)
;
