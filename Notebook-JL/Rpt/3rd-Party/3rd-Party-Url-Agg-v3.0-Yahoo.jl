## Tables and Data Source setup

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
;

include("../../Lib/Include-Package-v2.1.jl")
include("../../Lib/URL-Classification-Package-v2.0.jl")
;

#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()
;

UP = UrlParamsInit("3rd_Party_Url_Agg_Yahoo")
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 10
#UP.limitRows = 250
UP.pageGroup = "%"   #productPageGroup
UP.samplesMin = 10
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=true
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.devView=false
SP.criticalPathOnly=true
SP.debug=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)

;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

UP.pageGroup = "News Article"
UP.urlRegEx = "%news.nationalgeographic.com/2017/12/tourist-dies-rare-shark-attack-cocos-island-costa-rica-spd%"
UP.deviceType = "Desktop"
UP.agentOs = "%"
statsAndTreemaps(TV,UP,SP)
;

UP.pageGroup = "News Article"
UP.urlRegEx = "%news.nationalgeographic.com/2017/12/tourist-dies-rare-shark-attack-cocos-island-costa-rica-spd%"
UP.deviceType = "Desktop"
UP.agentOs = "%"
statsAndTreemaps(TV,UP,SP)
;
