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

#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,59)
#TV = weeklyTimeVariables(days=2)
TV = yesterdayTimeVariables(hours=1)
;

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 10
UP.pageGroup = "Video"   #productPageGroup
UP.samplesMin = 10
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%video.nationalgeographic.com/video/101-videos/volcanoes-101%"   #localUrl
UP.urlFull = "https://video.nationalgeographic.com/video/101-videos/volcanoes-101"
UP.usePageLoad=true
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel=2
SP.showLines=300
ShowParamsValidate(SP)
;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);
WellKnownUrlGroup = wellKnownUrlGroup();

#include("../../../lib/Include-Package-v2.1.jl")

UP.agentOs = "%"
UP.deviceType = "Desktop"
SP.showLines  = 100
criticalPathAggregationMain(TV,UP,SP)
;

UP.agentOs = "iOS"
UP.deviceType = "Mobile"
SP.showLines  = 100
criticalPathAggregationMain(TV,UP,SP)
;

UP.agentOs = "Android OS"
UP.deviceType = "Mobile"
SP.showLines  = 100
criticalPathAggregationMain(TV,UP,SP)
;
