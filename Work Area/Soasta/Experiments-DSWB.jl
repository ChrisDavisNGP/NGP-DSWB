# Note we are off by four hours due to UTC.  The routines below will be available in DSWB V92 (due in Sept 2016)
# For now just leave the "+4" and do the rounding in your head.
# i.e.,  23+4 is the next day at 3 am "...,7,29,23+4,0" becomes "...,7,30,3,0"
# i.e.,  Here is a 9 to 5 day
#startTime = DateTime(2016,7,29,9+4,0)
#endTime = DateTime(2016,7,29,17+4,0)

startTime = DateTime(2016,8,29,9,0)
endTime = DateTime(2016,8,29,17,0)
startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

#st = DateTime(2016,7,25,12,30)
#et = DateTime(2016,7,25,13,30)

#startTime = datetimeToUTC(st, TimeZone("America/New York"))
#endTime = datetimeToUTC(et, TimeZone("America/New York"))

firstAndLast = getBeaconsFirstAndLast()
timeString = "$(padDateTime(startTime)) to $(padDateTime(endTime))"
println(startTime, ", ", endTime)

# Welcome to DSWB

using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
table_rt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)

#setTable(tableRt, tableType = "RESOURCE_TABLE")


customer = "Nat Geo"
productPageGroup = "Your Shot" # primary page group
#productPageGroup = "Your Shot" # primary page group
page_group_table = "$(table)_productPage_view"
#localTable is new name 7/29/16
localTable = "$(table)_productPage_view"

# Set the Conversion Metric
#conversionMetric = "custom_metrics_0" # conversion metric column
#setConversionMetric(conversionMetric);

datePart = :minute


# Create view to query only product page_group
query("""create or replace view $localTable as (select * from $table where page_group = '$(productPageGroup)' and "timestamp" between $startTimeMs and $endTimeMs)""")

setTable(localTable)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
query("""SELECT count(*) FROM $localTable""")


getBeaconCount()

getBeaconsFirstAndLast()

getBeaconCountByType()

getAggregateSessionDuration(startTime, endTime, :avg)

getAggregateSessionDuration(startTime, endTime, :median; byList=[:user_agent_device_type])

getAggregateSessionLength(startTime, endTime, :AVG; byList=[:user_agent_device_type])

getAggregateSessionLengthAndDurationByLoadTime(startTime, endTime, :stddev; country="US")

#broken

#getAverageSessionLengthsByResourceTimer(DateTime(), now(), timer=:TTFB)
#getAverageSessionLengthsByResourceTimer(startTime, endTime, timer=:TTFB)

getBeaconsBelowThresholdOverTime(startTime, endTime, :minute; pageGroup=productPageGroup, threshold = "2700")

#getBounceRateByDimension(startTime, endTime; dimension="minute", n=15, minPercentage=0.0, beaconType="page view", pageGroup=["Homepage","Product"], country=["US"], device=["Desktop"])

t1 = getBounceRateByDimension(startTime, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Desktop","Mobile"])
t2 = getBounceRateByDimension(startTime, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Desktop"])
t3 = getBounceRateByDimension(startTime, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Mobile"])

display(t1)
display(t2)
display(t3)

sessionsTable = "_sessions"
pagegroupTable = "_groups"
table = getGroupIdPercentages(startTime, endTime, sessionsTable, pagegroupTable; onlyStartingPages=true, isMissingPagegroupIdTable=true, isMissingSessionsTable=true)

httpAndHttps = getHttpVsHttps(startTime, endTime, :hour)

dimension1 = Symbol("page_group")
dimension2 = Symbol("user_agent_device_type")

tto = thinkTimesOverall                 = getMedianThinkTime(startTime, endTime)
ttpg = thinkTimesByPageGroup             = getMedianThinkTime(startTime, endTime; byList=[dimension1])
ttall = thinkTimeByPageGroupAndDeviceType = getMedianThinkTime(startTime, endTime; byList=[dimension1, dimension2])

display(tto)
display(ttpg)
display(ttall)

loadMedianiByYear = getMetricMediansByDatepart(startTime, endTime, :hour, pageGroup=productPageGroup)

getMinMedianMaxByDatepart(startTime, endTime, :hour)

getSessionsStats(startTime::DateTime, endTime::DateTime)
