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

include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,5,9,16,0,2017,5,9,16,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

individualStreamlineWorkflow(TV,UP,SP)
;
