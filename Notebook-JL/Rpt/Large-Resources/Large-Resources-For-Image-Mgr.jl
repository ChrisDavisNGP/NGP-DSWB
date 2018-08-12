using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,6,17,10,0,2017,6,17,10,59);

UP = UrlParamsInit(scriptName)
UP.sizeMin = 200000
UP.timeLowerMs = 10       # 10 ms not 1 sec
UP.timeUpperMs = 9000000  # 9 million not 600k only care about size
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

largeResourcesForImageMgrWorkflow(TV,UP,SP)
;
