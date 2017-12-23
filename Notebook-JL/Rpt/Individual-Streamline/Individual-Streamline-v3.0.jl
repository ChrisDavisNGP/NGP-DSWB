## Tables and Data Source setup

using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser
using JSON

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")
;

include("../../../lib/Include-Package-v2.1.jl")
;

#TV = timeVariables(2017,5,9,16,0,2017,5,9,16,59)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()
;

# This is the Mobile Only, Time Based report

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "Mobile"
UP.limitRows = 250
UP.pageGroup = "%"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.devView=false
SP.criticalPathOnly=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)

;

individualStreamlineWorkflow(TV,UP,SP)
;
