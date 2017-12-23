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

#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()
;

# This is the Mobile Only, Time Based report
UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "Desktop"
UP.limitRows = 10
UP.pageGroup = "News Article"   #productPageGroup
UP.samplesMin = 10
UP.timeLowerMs = 2 * 1000.0
UP.timeUpperMs = 20 * 1000.0
UP.urlRegEx = "%/news.nationalgeographic.com/2017/11/ancient-fossil-forest-found-antarctica-gondwana-spd%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=true
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.devView=true
SP.criticalPathOnly=true
SP.debugLevel = 1   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel = 10
ShowParamsValidate(SP)

;

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

localTableDF = defaultBeaconsToDF(TV,UP,SP);

# Stats on the data
statsDF = DataFrame()
statsDF = beaconStats(TV,UP,SP,localTableDF)
UP.timeLowerMs = statsDF[1:1,:q25][1]
UP.timeUpperMs = statsDF[1:1,:q75][1]
;

#UP.timeLowerMs = 5.0 * 1000
#UP.timeUpperMs = 6.0 * 1000
localTableRtDF = getResourcesForBeaconCreateDF(TV,UP)
#println("$tableRt count is ",size(localTableRtDF))
;

showAvailableSessions(TV,UP,SP,localTableDF,localTableRtDF)

try

    executeSingleSession(TV,UP,SP,WellKnownHost,WellKnownPath,9284,"0497818d-118a-489f-924b-b6a2b84d5cca-ozvgig",1511442530699) #    Time=9284


catch y
    println("studySession Exception ",y)
end
;
