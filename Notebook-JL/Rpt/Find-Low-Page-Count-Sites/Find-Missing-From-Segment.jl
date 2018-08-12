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
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

displayTitle(chart_title = "Missing Groups", showTimeStamp=false)
displayTopUrlsByCount(TV,UP,SP,"Channel Microsite";rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Cheeta NGM";        rowLimit=10000,beaconsLimit=100,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Food";              rowLimit=10000,beaconsLimit=30,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Interactive";      rowLimit=10000,beaconsLimit=15,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Help";              rowLimit=10000,beaconsLimit=30,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Phenomena";         rowLimit=10000,beaconsLimit=50,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Ocean";              rowLimit=10000,beaconsLimit=20,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Sponsored";         rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Tickets";           rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Travel WP";       rowLimit=10000,beaconsLimit=15,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Video AMP";         rowLimit=10000,beaconsLimit=0,paginate=true);
;
