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

include("../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,5,9,16,0,2017,5,9,16,59)
TV = prevWorkWeekTimeVariables()
#TV = yesterdayTimeVariables()

# This is the Desktop Only, Time based report

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "Desktop"
UP.orderBy = "time"
UP.pageGroup = "%"   #productPageGroup
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

UP.limitRows = 125      # Consider the top 125 in the given category
UP.samplesMin = 100     # Use only pages above 100 samples
SP.showLines = 100      # The bigger this number the longer the script runs

individualStreamlineWorkflow(TV,UP,SP)
;
