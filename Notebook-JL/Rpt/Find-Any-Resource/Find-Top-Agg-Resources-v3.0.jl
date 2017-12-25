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
#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,59);

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.scrubUrlSections=100
ShowParamsValidate(SP)

UP.resRegEx = "%www.nationalgeographic.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%news.nationalgeographic.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%adservice.google%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%googlesyndication.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%yahoo.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%innovid.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%moatads.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%fls.doubleclick%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%unrulymedia.com%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%googleapis.com%"   # Google Doubleclick related
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%2mdn.net%"   # Google Doubleclick related
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%doubleclick.net%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%monetate_off%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%monetate.net%"
findAnyResourceWorkflow(TV,UP,SP)

UP.resRegEx = "%MonetateTests%"
findAnyResourceWorkflow(TV,UP,SP)
;
