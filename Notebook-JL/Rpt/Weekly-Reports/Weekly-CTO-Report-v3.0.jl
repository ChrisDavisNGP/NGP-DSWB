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

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)
TV = weeklyTimeVariables(days=7)
#TV = yesterdayTimeVariables()
;
UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "News Article"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)

try
    chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
catch y
    println("chartConcurrentSessionsAndBeaconsOverTime Exception ",y)
end

try
    chartLoadTimes(TV.startTime, TV.endTime, TV.datePart)
catch y
    println("chartLoadTimes Exception ",y)
end

showPeakTable(TV,UP,SP;showStartTime30=true,showStartTime90=false)

topUrlTableByTime(TV,UP,SP)

pageGroupQuartiles(TV,UP,SP);

chartActivityImpactByPageGroup(TV.startTime, TV.endTime;n=10);

try
    pageGroupTreemap(TV,UP,SP)
catch y
    println("pageGroupTreemap Exception ",y)
end

try
    deviceTypeTreemap(TV,UP,SP)
catch y
    println("deviceTypeTreemap Exception ",y)
end

try
    browserFamilyTreemap(TV,UP)
catch y
    println("browserFamilyTreemap Exception ",y)
end

try
    countryTreemap(TV,UP)
catch y
    println("chartConcurSessions Exception ",y)
end

;
