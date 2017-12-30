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

TV = pickTime()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.reportLevel = 10  # Set to 10 to see all non-debug info including long queries
ShowParamsValidate(SP)

beaconAndRtCountsWorkflow(TV,UP,SP)
;
