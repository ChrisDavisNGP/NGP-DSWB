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
#TV = timeVariables(2017,12,5,13,0,2017,12,5,14,59);

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

findAnyResourceWorkflow(TV,UP,SP)
;
