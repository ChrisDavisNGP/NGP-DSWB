function firstAndLast()
    limitedTable(localTable,table,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
    setTable(localTable)
    firstAndLast = getBeaconsFirstAndLast()
end

function sessionsBeacons()
    try
        #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup)", chart_info = [timeString])
        setTable(localTable)
        chartConcurrentSessionsAndBeaconsOverTime(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
    catch y
        println("chartConcurSessions Exception ",y)
    end
end

function loadTime()
    try
        #displayTitle(chart_title = "Median Load Times for $(productPageGroup)", chart_info = [timeString])
        setTable(localTable)
        chartLoadTimes(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
    catch y
        println("chartLoadTimes Exception ",y)
    end 
end

function topUrls()
    setTable(localTable)
    topUrlTable(localTable,productPageGroup,tv.timeString;limit=15)
end

function peakTable()
    setTable(localTable)
    showPeakTable(tv.timeString,productPageGroup,tv.startTimeUTC,tv.endTimeUTC)
end

function statsTable()
    try 
        setTable(localTable)
        localStatsDF = statsTableDF(localTable,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC);    
        statsDF = basicStats(localStatsDF, productPageGroup, tv.startTimeMsUTC, tv.endTimeMsUTC)
        medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(productPageGroup)", chart_info = [tv.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
        #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))
    catch y
        println("basicStats Exception ",y)
    end       
end

