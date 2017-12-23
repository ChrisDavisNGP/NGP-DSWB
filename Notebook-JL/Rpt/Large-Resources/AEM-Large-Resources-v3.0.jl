using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,6,7,10,0,2017,6,7,10,59)
#TV = weeklyTimeVariables(days=3)
TV = yesterdayTimeVariables()

UP = UrlParamsInit("AEM_Large_Resources")
UP.agentOs = "%"
UP.deviceType = "Mobile"
UP.limitRows = 250
UP.pageGroup = "Magazine AEM"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 600000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
#SP.showLines = 300
SP.showLines = 50
ShowParamsValidate(SP)

minimumEncoded = 500000

aemLargeResourcesWorkflow(TV,UP,SP,minimumEncoded)
;
