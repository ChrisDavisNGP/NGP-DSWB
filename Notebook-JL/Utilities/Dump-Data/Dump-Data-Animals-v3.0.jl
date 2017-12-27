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

# Packages
include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

displayGroup(TV,UP,SP)
;
