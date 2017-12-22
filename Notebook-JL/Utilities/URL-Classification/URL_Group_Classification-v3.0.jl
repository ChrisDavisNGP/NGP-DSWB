using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")
include("../../../Lib/URL-Classification-Package-v2.0.jl")

TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 10000
#UP.limitRows = 250
UP.pageGroup = "%"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 20000
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

WellKnownUrlGroup = wellKnownUrlGroup();

printWellKnownUrlGroup(TV,UP,SP)

printCsvWellKnownUrlGroup(TV,UP,SP)
;
