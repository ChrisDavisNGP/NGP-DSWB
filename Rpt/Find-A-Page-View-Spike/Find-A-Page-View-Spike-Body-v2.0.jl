function sessionsBeaconsFAPVSB(TV::TimeVars,UP::UrlParams)
    try
        setTable(UP.btView)
        chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("sessionsBeacons Exception ",y)
    end
end

function loadTimeFAPVSB(TV::TimeVars,UP::UrlParams)
    try
        setTable(UP.btView)
        chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("loadTime Exception ",y)
    end
end

function topUrlsFAPVSB(TV::TimeVars,UP::UrlParams)
    setTable(UP.btView)
    topUrlTable(UP.btView,UP.pageGroup,TV.timeString;limit=15)
end

function peakTableFAPVSB(TV::TimeVars,UP::UrlParams)
    setTable(UP.btView)
    showPeakTable(TV.timeString,UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC)
end

function statsTableFAPVSB(TV::TimeVars,UP::UrlParams)
    try
        setTable(UP.btView)
        localStatsDF = statsTableDF(UP.btView,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC);
        statsDF = basicStats(localStatsDF, UP.pageGroup, TV.startTimeMsUTC, TV.endTimeMsUTC)
        medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(UP.pageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
        #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))
    catch y
        println("statsTableFAPVSB Exception ",y)
    end
end
