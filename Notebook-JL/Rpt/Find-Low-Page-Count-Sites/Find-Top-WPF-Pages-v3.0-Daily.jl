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

include("../../../Lib/Include-Package-v2.1.jl")
;

TV = weeklyTimeVariables(days=1)
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)
#TV = yesterdayTimeVariables()
;

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
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
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)
;

cleanupTableFTWP(TV,UP)
;

displayTopUrlsByCount(TV,UP,SP,"Adventure WPF";rowLimit=10000,beaconsLimit=1,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Animals WPF",rowLimit=10000,beaconsLimit=1,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Environment WPF",rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Games WPF",rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Images WPF",rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Movies WPF",rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Ocean WPF",rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Photography WPF",rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Science WPF",rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Travel WPF",rowLimit=10000,beaconsLimit=10,paginate=true);
;
