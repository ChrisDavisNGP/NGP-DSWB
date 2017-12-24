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

include("../../../Lib/Include-Package-v2.1.jl")
include("../../../Lib/URL-Classification-Package-v2.0.jl")
;

#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,59)
#TV = weeklyTimeVariables(days=2)
TV = yesterdayTimeVariables()
;

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "News Article"   #productPageGroup
UP.samplesMin = 10
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%news.nationalgeographic.com/2017/11/jesus-tomb-archaeology-jerusalem-christianity-rome%"   #localUrl
UP.urlFull = "https://news.nationalgeographic.com/2017/11/jesus-tomb-archaeology-jerusalem-christianity-rome/"
UP.usePageLoad=true
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel=2
SP.showLines=3      # Control number treemaps
ShowParamsValidate(SP)
;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

SP.showLines=25
SP.debugLevel=0
individualStreamlineMain(TV,UP,SP)
;
