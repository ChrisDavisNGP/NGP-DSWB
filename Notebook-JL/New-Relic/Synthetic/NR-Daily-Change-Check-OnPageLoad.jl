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

include("../../../Lib/Include-Package.jl")

#Default is yesterday from 7 to 17
oldTV = pickTime()
TvTodayWorkDay = true
newTV = pickTime()
#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

CU = CurlParamsInit(scriptName)
CurlParamsValidate(CU)

NR = NrParamsInit()

dailyChangeCheckOnPageLoadWorkflow(SP,NR,CU)
;
