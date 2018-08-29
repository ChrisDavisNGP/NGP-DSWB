# Note we are off by four hours due to UTC.  The routines below will be available in DSWB V92 (due in Sept 2016)
# For now just leave the "+4" and do the rounding in your head.
# i.e.,  23+4 is the next day at 3 am "...,7,29,23+4,0" becomes "...,7,30,3,0"
# i.e.,  Here is a 9 to 5 day
#start_time = DateTime(2016,7,29,9+4,0)
#endTime = DateTime(2016,7,29,17+4,0)

start_time = DateTime(2016,8,29,9,0)
endTime = DateTime(2016,8,29,17,0)
startTimeMs = datetimeToMs(start_time)
endTimeMs = datetimeToMs(endTime)

#st = DateTime(2016,7,25,12,30)
#et = DateTime(2016,7,25,13,30)

#start_time = datetimeToUTC(st, TimeZone("America/New York"))
#endTime = datetimeToUTC(et, TimeZone("America/New York"))

firstAndLast = getBeaconsFirstAndLast()
timeString = "$(padDateTime(start_time)) to $(padDateTime(endTime))"
println(start_time, ", ", endTime)

# Welcome to DSWB

using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
table_rt = "beacons_4744_rt"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
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


# Create view to query only product pagegroupname
select("""create or replace view $localTable as (select * from $table where pagegroupname = '$(productPageGroup)' and timestamp between $startTimeMs and $endTimeMs)""")

setTable(localTable)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacontypename = 'page view'
select("""SELECT count(*) FROM $localTable""")


getBeaconCount()

getBeaconsFirstAndLast()

getBeaconCountByType()

getAggregateSessionDuration(start_time, endTime, :avg)

getAggregateSessionDuration(start_time, endTime, :median; byList=[:devicetypename])

getAggregateSessionLength(start_time, endTime, :AVG; byList=[:devicetypename])

getAggregateSessionLengthAndDurationByLoadTime(start_time, endTime, :stddev; country="US")

#broken

#getAverageSessionLengthsByResourceTimer(DateTime(), now(), timer=:TTFB)
#getAverageSessionLengthsByResourceTimer(start_time, endTime, timer=:TTFB)

getBeaconsBelowThresholdOverTime(start_time, endTime, :minute; pageGroup=productPageGroup, threshold = "2700")

#getBounceRateByDimension(start_time, endTime; dimension="minute", n=15, minPercentage=0.0, beaconType="page view", pageGroup=["Homepage","Product"], country=["US"], device=["Desktop"])

t1 = getBounceRateByDimension(start_time, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Desktop","Mobile"])
t2 = getBounceRateByDimension(start_time, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Desktop"])
t3 = getBounceRateByDimension(start_time, endTime, n=15, minPercentage=0.5, beaconType="page view", pageGroup=[productPageGroup], device=["Mobile"])

display(t1)
display(t2)
display(t3)

sessionsTable = "_sessions"
pagegroupTable = "_groups"
table = getGroupIdPercentages(start_time, endTime, sessionsTable, pagegroupTable; onlyStartingPages=true, isMissingPagegroupIdTable=true, isMissingSessionsTable=true)

httpAndHttps = getHttpVsHttps(start_time, endTime, :hour)

dimension1 = Symbol("pagegroupname")
dimension2 = Symbol("devicetypename")

tto = thinkTimesOverall                 = getMedianThinkTime(start_time, endTime)
ttpg = thinkTimesByPageGroup             = getMedianThinkTime(start_time, endTime; byList=[dimension1])
ttall = thinkTimeByPageGroupAndDeviceType = getMedianThinkTime(start_time, endTime; byList=[dimension1, dimension2])

display(tto)
display(ttpg)
display(ttall)

loadMedianiByYear = getMetricMediansByDatepart(start_time, endTime, :hour, pageGroup=productPageGroup)

getMinMedianMaxByDatepart(start_time, endTime, :hour)

getSessionsStats(start_time::DateTime, endTime::DateTime)
