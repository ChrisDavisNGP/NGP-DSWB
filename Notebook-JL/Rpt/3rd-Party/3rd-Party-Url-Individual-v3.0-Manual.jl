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

#TV = timeVariables(2017,11,28,13,59,2017,11,28,14,59)
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
UP.urlRegEx = "%news.nationalgeographic.com/2017/11/jesus-tomb-archaeology-jerusalem-christianity-rome%"
UP.urlFull = "https://news.nationalgeographic.com/2017/11/jesus-tomb-archaeology-jerusalem-christianity-rome/"
UP.usePageLoad=true
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel=2
ShowParamsValidate(SP)
;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

localTableDF = defaultBeaconsToDF(TV,UP,SP)
println("Local Beacon Table count is ",size(localTableDF)[1])
;

# Stats on the data
statsDF = DataFrame()
statsDF = beaconStats(TV,UP,SP,localTableDF)
UP.timeLowerMs = round(statsDF[1:1,:median][1] * 0.90)
UP.timeUpperMs = round(statsDF[1:1,:median][1] * 1.10)
;

# medium
#UP.timeLowerMs = 4.0 * 1000
#UP.timeeUpperMs = 5.5 * 1000
localTableRtDF = getResourcesForBeaconCreateDF(TV,UP)
println("Matching Resource Table count is ",size(localTableRtDF)[1])
;

showAvailableSessions(TV,UP,SP,localTableDF,localTableRtDF)

try



    catch y
    println("studySession Exception ",y)
end
;
