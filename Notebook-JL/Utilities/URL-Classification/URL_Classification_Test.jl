using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")
include("../../../Lib/URL-Classification-Package-v2.0.jl")

#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)
TV = weeklyTimeVariables(days=2)
#TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

toppageurl = testUrlClassifyToDF(TV,UP,SP)
display(size(toppageurl))

WellKnownHostDirectory = wellKnownHostEncyclopedia();
WellKnownPath = wellKnownPathDictionary();

scrubUrlToPrint(SP,toppageurl,:urlgroup);
classifyUrl(SP,toppageurl);
;
