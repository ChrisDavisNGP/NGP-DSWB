using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)

# setTable(tableRt, tableType = "RESOURCE_TABLE")

# Packages
#include("/data/notebook_home/Production/Bodies/Find-A-Page-View-Spike-Body-v1.2.jl")
include("/data/notebook_home/Production/Lib/Include-Package-v1.0.jl")

# Time values (tv.) structure created in include above, so init time here
timeVariables(2017,2,12,10,0,2017,2,12,23,59);

customer = "Nat Geo"
productPageGroup = "Photography AEM" # primary page group
localTable = "$(table)_spike_view_prod"
localTableRt = "$(tableRt)_spike_view_prod"
debugLevel = 10;
;

try
    # Create view to query only product page_group
    query("""drop view if exists $localTable""")

    query("""\
    create or replace view $localTable as
    (select *,"timestamp" as listtime from $table where
    page_group = '$(productPageGroup)' and "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
    )""")
    ;

catch y
    println("Query exception ",y)
end

try
    setTable(localTable)
    firstAndLast = getBeaconsFirstAndLast()
catch y
    println("firstLast exception ",y)
end

localStatsDF = DataFrame()
statsDF = DataFrame()
medianThreshold = Int64
try
    localStatsDF = statsTableDF(localTable,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC);
    statsDF = basicStats(localStatsDF, productPageGroup, tv.startTimeMsUTC, tv.endTimeMsUTC)
    medianThreshold = statsDF[1:1,:median][1]

    displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(productPageGroup)", chart_info = [tv.timeString],showTimeStamp=false)
    beautifyDF(statsDF[:,:])
    #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
    #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))
catch y
    println("setupStats Exception ",y)
end

localStats2 = DataFrame()

try
    LowerBy3Stddev = statsDF[1:1,:LowerBy3Stddev][1]
    UpperBy3Stddev = statsDF[1:1,:UpperBy3Stddev][1]
    UpperBy25p = statsDF[1:1,:UpperBy25p][1]

    localStats2 = query("""\
        select
        "timestamp",
        timers_t_done,
        session_id
        from $table where
        page_group = '$(productPageGroup)'
        and "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        and timers_t_done > $(UpperBy25p)
    """)
catch y
    println("localStats2 Exception ",y)
end


;
#display(localStats2)

#studySession = "fd8c6953-ac8e-4ace-8ad7-51eb8d6eebc3-oifn7l"
#studyTime = 1482151207938
#waterFallFinder(table,studySession,studyTime,tv)

dv = localStats2[:timers_t_done]

statsArr(v) = [round(v,0),round(v/1000.0,3),round(v/60000,1)]

dv = dropna(dv)
stats = DataFrame()
stats[:unit] = ["milliseconds","seconds","minutes"]
stats[:count] = size(dv,1)
stats[:mean] = statsArr(mean(dv))
stats[:median] = statsArr(median(dv))
stats[:stddev] = statsArr(std(dv))
#stats[:variance] = statsArr(var(dv))
stats[:min] = statsArr(minimum(dv))
stats[:max] = statsArr(maximum(dv))

# Range by percent
tenpercent = stats[1,:median] * 0.25
rangeLowerBy25p = stats[1,:median] - tenpercent
if (rangeLowerBy25p < 1.0) rangeLowerBy25p = 1000 end
rangeUpperBy25p = stats[1,:median] + tenpercent

# Range 1 Std Dev
rangeLowerByStd = stats[1,:median] - (stats[1,:stddev] * 3)
if (rangeLowerByStd < 0.0) rangeLowerByStd = 1 end
rangeUpperByStd = stats[1,:median] + (stats[1,:stddev] * 3)

stats[:rangeLowerBy25p] = statsArr(rangeLowerBy25p)
stats[:rangeUpperBy25p] = statsArr(rangeUpperBy25p)
stats[:rangeLowerByStd] = statsArr(rangeLowerByStd)
stats[:rangeUpperByStd] = statsArr(rangeUpperByStd)

displayTitle(chart_title = "Table Data Stats Outside 3 Stddev for $(productPageGroup)", chart_info = [tv.timeString],showTimeStamp=false)
beautifyDF(stats[:,:])
#by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1)))
#c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
#drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))

setTable(localTable)
chartPercentageOfBeaconsBelowThresholdStackedBar(tv.startTimeUTC, tv.endTimeUTC, tv.datePart; table=localTable)

#setTable(table)
#chartPercentageOfBeaconsBelowThresholdStackedBar(tv.startTime, tv.endTime, tv.datePart; pageGroup=productPageGroup)

#println(medianThreshold)
#thresholdValues = [1000;10000;100000]
#chartResponseTimesVsTargets(startTime, endTime, :minute, thresholdValues)

#chartPercentageOfBeaconsBelowThresholdTimeSeries(tv.startTimeUTC, tv.endTimeUTC, tv.datePart; threshold=medianThreshold)
# See getBeaconsBelowThresholdOverTime(startTime::DateTime, endTime::DateTime, datepart::Symbol)
#
#::DataFrame - This function returns a DataFrame with the first column as the dateparts between the given start and end time.
#The second column gives the number of beacons whose timer was below the given threshold.
#The third column gives the number of beacons whose timer was above the given threshold.
#The fourth column gives the total count of beacons for that time.
#The fifth column gives the percentage of beacons below the threshold.
#The sixth column gives the percentage of beacons above the threshold.

#displayTitle(chart_title = "Median Load Times for $(productPageGroup)", chart_info = [timeString])
setTable(localTable)
chartLoadTimes(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
setTable(table)
chartLoadTimes(tv.startTime, tv.endTime, tv.datePart)
#chartLoadTimes(startTime, endTime, :second)

setTable(localTable)
chartSessionDurationQuantilesByDatepart(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)

chartTopURLsByLoadTime(tv.startTimeUTC, tv.endTimeUTC)

#usually no bounces

#chartBounceRateByDatepart(startTime, endTime, datePart)
#chartBounceRateByDimension(startTime, endTime; dimension="minute", n=15, minPercentage=0.01, beaconType="page view", pageGroup=[productPageGroup], country=["US"], device=["Desktop"])
#chartBounceRateByDimension(startTime, endTime; dimension="page_group", n=15, minPercentage=0.01, beaconType="page view", pageGroup=[productPageGroup], country=["US"], device=["Desktop"])


#displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup)", chart_info = [timeString])
chartConcurrentSessionsAndBeaconsOverTime(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)

dataNames = ["Current Long Page Views Completed"]
axisLabels = ["Timestamps", "Milliseconds to Finish"]

chart_title="Points above 3 Standard Dev"
chart_info=["These are the long points only limited to the first 500"]

colors = ["#EEC584", "rgb(85,134,140)"]

# kwargs
point_r = 2

drawC3Viz(localStats2[1:500,:];  dataNames=dataNames, axisLabels=axisLabels, chart_title=chart_title, chart_info=chart_info, colors=colors, point_r=point_r);

# Need Resource table here

#chartResourceResponseTimeDistribution(startTime, endTime)
