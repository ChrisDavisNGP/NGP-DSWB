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

#TV = timeVariables(2017,11,28,13,59,2017,11,28,14,59)
#TV = weeklyTimeVariables(days=2)
TV = yesterdayTimeVariables()
;

UP = UrlParamsInit("3rd_Party_Url_Individual_Yahoo")
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "News Article"   #productPageGroup
UP.samplesMin = 10
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "https://news.nationalgeographic.com/2017/10/algae-bloom-lake-erie-toxins-spd%"   #localUrl
UP.urlFull = "https://news.nationalgeographic.com/2017/10/algae-bloom-lake-erie-toxins-spd/"
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

executeSingleSession(TV,UP,SP,6409,"10ed478f-c35f-4e0e-a4fb-88999907b556-p0e9uj",1512320354060) #    Time=6409
executeSingleSession(TV,UP,SP,5326,"157a1365-e60d-4563-922d-134a596a2fca-p0e8l5",1512318718821) #    Time=5326
executeSingleSession(TV,UP,SP,4959,"450fc1f3-12d1-4176-aa11-cbeeccb2f247-p0eigq",1512331519156) #    Time=4959
executeSingleSession(TV,UP,SP,6453,"5b2b7978-6881-461e-a555-94db1355348d-p0egt5",1512329375979) #    Time=6453

    catch y
    println("studySession Exception ",y)
end

println(SP.debugLevel)
;
