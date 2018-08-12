## Tables and Data Source setup

using QueryAPI
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "tenant_232301"
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")
include("../../../Lib/URL-Classification-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP);
WellKnownPath = wellKnownPathDictionary(SP);
WellKnownUrlGroup = wellKnownUrlGroup(SP);

criticalPathAggregationMain(TV,UP,SP)
;
