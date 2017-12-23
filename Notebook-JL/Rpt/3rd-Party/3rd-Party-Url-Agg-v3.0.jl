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

include("../../../lib/Include-Package-v2.1.jl")
include("../../../lib/URL-Classification-Package-v2.0.jl")
;

#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,10)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()
;

UP = UrlParamsInit(scriptName)
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
SP.devView=true
SP.criticalPathOnly=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel = 1 # Higher reportLevel values show more treemaps
ShowParamsValidate(SP)

;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

UP.pageGroup = "News Article"
#UP.urlRegEx = "%news.nationalgeographic.com/2017/12/tourist-dies-rare-shark-attack-cocos-island-costa-rica-spd%"
UP.urlRegEx = "%"
#UP.deviceType = "Desktop"
UP.deviceType = "%"
UP.agentOs = "%"
statsAndTreemaps(TV,UP,SP)
;

#UP.pageGroup = "News Article"
#UP.urlRegEx = "%news.nationalgeographic.com/2017/12/tourist-dies-rare-shark-attack-cocos-island-costa-rica-spd%"
#UP.deviceType = "Mobile"
#UP.agentOs = "iOS"
#statsAndTreemaps(TV,UP,SP)
;

#UP.pageGroup = "News Article"
#UP.urlRegEx = "%news.nationalgeographic.com/2017/12/tourist-dies-rare-shark-attack-cocos-island-costa-rica-spd%"
#UP.deviceType = "Mobile"
#UP.agentOs = "Android OS"
#statsAndTreemaps(TV,UP,SP)
;
