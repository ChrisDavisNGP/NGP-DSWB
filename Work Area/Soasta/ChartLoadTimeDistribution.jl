using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
table_rt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)

UP = UrlParamsInit(scriptName)
UP.pageGroup = "Channel" # primary page group
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

# Set the Conversion Metric
#conversionMetric = "custom_metrics_0" # conversion metric column
#setConversionMetric(conversionMetric);

defaultBeaconCreateView(TV,UP,SP)
btv = UP.btView
setTable(btv)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
# query("""SELECT count(*) FROM $btv""")

## Not Working - to debug

#To do figure out RTChart and RTAnaysis
#chartLoadTimeDistribution(startTime, endTime; url = url, isFirstParty = true, pageGroup = "conversionPageGroup")
 chartLoadTimeDistribution(TV.startTimeUTC, TV.endTimeUTC,url="nationalgeographic.com")
loadTimeTable = createLoadTime(startTime,endTime)

#try also chartLoadTimeStats(startTime,endTime)
#try also chartTopNLoadTimeDistributions(startTime,endTime)
#try also chartTopURLsByLoadTime(startTime::DateTime, endTime::DateTime)
