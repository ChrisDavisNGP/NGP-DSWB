## Tables and Data Source setup

using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,7,21,10,59,2017,7,21,12,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

displayTopUrlsByCount(TV,UP,SP,UP.pageGroup;rowLimit=10000,beaconsLimit=10,paginate=true);
;
