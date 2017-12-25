using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,12,5,13,0,2017,12,5,14,59);

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.scrubUrlSections=100
ShowParamsValidate(SP)

UP.resRegEx = "%v1.9.3%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%v1.9.5%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%cdn1.spotible.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%fng-ads.fox.com/fw_ads%" # Oct 19 freewheel ads
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%player.foxdcg.com/ngp-freewheel%" # Oct 19 freewheel ads
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%pr-bh.ybp.yahoo.com%"
findAnyResourceWorkflow(TV,UP,SP)
;
