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

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

try
    chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
catch y
    println("chartConcurrentSessionsAndBeaconsOverTime Exception ",y)
end

try
    chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
catch y
    println("chartLoadTimes Exception ",y)
end

showPeakTable(TV,UP,SP;showStartTime30=true)

topUrlTableByTime(TV,UP,SP)

pageGroupQuartiles(TV,UP,SP);

chartActivityImpactByPageGroup(TV.startTimeUTC, TV.endTimeUTC;n=10);

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
    browserFamilyTreemap(TV,UP,SP)
catch y
    println("browserFamilyTreemap Exception ",y)
end

try
    countryTreemap(TV,UP,SP)
catch y
    println("chartConcurSessions Exception ",y)
end

;
