# Finding the Load Times

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

setTable(table_rt, tableType = "RESOURCE_TABLE")


customer = "Nat Geo"
productPageGroup = "Channel" # primary page group

#localTable
localTable = "$(table)_chart_view"

# Set the Conversion Metric
#conversionMetric = "custom_metrics_0" # conversion metric column
#setConversionMetric(conversionMetric);

datePart = :minute

# Note we are off by four hours due to UTC.  The routines below will be available in DSWB V92 (due in Sept 2016)
# For now just add 4 and do the rounding in your head. i.e.,  23+4 is the next day at 3 am

startTime = DateTime(2016,7,28,9+4,40)
endTime = DateTime(2016,7,28,17+4,10)
startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

#st = DateTime(2016,7,25,12,30)
#et = DateTime(2016,7,25,13,30)

#startTime = datetimeToUTC(st, TimeZone("America/New York"))
#endTime = datetimeToUTC(et, TimeZone("America/New York"))

firstAndLast = getBeaconsFirstAndLast()
timeString = "$(padDateTime(startTime)) to $(padDateTime(endTime))"
println(startTime, ", ", endTime)

# Create view to query only product page_group
query("""create or replace view $localTable as (select * from $table where page_group = '$(productPageGroup)' and "timestamp" between $startTimeMs and $endTimeMs)""")

setTable(localTable)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
query("""SELECT count(*) FROM $localTable""")


## Not Working - to debug

#To do figure out RTChart and RTAnaysis
setTable(table_rt)
chartLoadTimeDistribution(startTime, endTime,"nationalgeographic.com")
loadTimeTable = createLoadTime(startTime,endTime)

#try also chartLoadTimeStats(startTime,endTime)
#try also chartTopNLoadTimeDistributions(startTime,endTime)
#try also chartTopURLsByLoadTime(startTime::DateTime, endTime::DateTime)
