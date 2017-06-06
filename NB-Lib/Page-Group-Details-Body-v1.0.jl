function statsPGD()
    try
        localStatsDF = statsTableDF(localTable, productPageGroup, tv.startTimeMsUTC, tv.endTimeMsUTC);    
        statsDF = basicStats(localStatsDF, productPageGroup, tv.startTimeMsUTC, tv.endTimeMsUTC)

        displayTitle(chart_title = "Raw Data Stats $(productPageGroup) Based On Beacon Page Load Time", chart_info = [tv.timeString],showTimeStamp=false)
        beautifyDF(statsDF[2:2,:])
        return statsDF
    catch y
        println("setupStats Exception ",y)
    end
end

function peakPGD()
    showPeakTable(tv.timeString,productPageGroup,tv.startTimeUTC,tv.endTimeUTC;showStartTime30=false,showStartTime90=false,tableRange="Sample Set ")
end

function concurrentSessionsPGD(;showMobileOnly::Bool=false,showDesktopOnly::Bool=false)
    try
        if (!showMobileOnly && !showDesktopOnly)
            chartConcurrentSessionsAndBeaconsOverTime(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
        end

        if (showMobileOnly)
            timeString2 = timeString * " - Mobile Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup) - MOBILE ONLY", chart_info = [timeString2],showTimeStamp=false)
            setTable(localMobileTable)
            chartConcurrentSessionsAndBeaconsOverTime(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
            setTable(localTable)    
        end
        
        if (showDesktopOnly)
            timeString2 = timeString * " - Desktop Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup) - DESKTOP ONLY", chart_info = [timeString],showTimeStamp=false)
            setTable(localDesktopTable)
            chartConcurrentSessionsAndBeaconsOverTime(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
            setTable(localTable)    
        end

    catch y
        println("cell concurrentSessions Exception ",y)
    end
end

function loadTimesPGD(;showMobileOnly::Bool=false,showDesktopOnly::Bool=false)
    try

        #todo turn off title in chartLoadTimes
        #displayTitle(chart_title = "Median Load Times for $(productPageGroup)", chart_info = [timeString],showTimeStamp=false)
        if (!showMobileOnly && !showDesktopOnly)
            chartLoadTimes(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
        end

        #cannot use the other forms without creating the code for the charts.  Titles cannot be overwritten.
        if (showMobileOnly)
            displayTitle(chart_title = "Median Load Times for $(productPageGroup) - MOBILE ONLY", chart_info = [tv.timeString],showTimeStamp=false)
            setTable(localMobileTable)
            chartLoadTimes(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
            setTable(localTable)
        end
        
        if (showDesktopOnly)
            displayTitle(chart_title = "Median Load Times for $(productPageGroup) - DESKTOP ONLY", chart_info = [tv.timeString],showTimeStamp=false)
            setTable(localDesktopTable)
            chartLoadTimes(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
            setTable(localTable)
        end

    catch y
        println("cell chartLoadTimes Exception ",y)
    end
end

function topUrlPGD()
    topUrlTable(localTable,productPageGroup,tv.timeString; limit=10)
end

function thresholdChartPGD(medianThreshold::Float64)
    try
        chartPercentageOfBeaconsBelowThresholdStackedBar(tv.startTimeUTC, tv.endTimeUTC, tv.datePart; threshold = medianThreshold)
    catch y
        println("chartPercent exception ",y)
    end
end

function pageLoadPGD()
    sessionLoadPGD()
end
function sessionLoadPGD()
    try
        perfsessionLength = getAggregateSessionLengthAndDurationByLoadTime(tv.startTimeUTC, tv.endTimeUTC);

        c3 = drawC3Viz(perfsessionLength; columnNames=[:load_time,:total,:avg_length], axisLabels=["Session Load Times","Completed Sessions", "Average Session Length"],dataNames=["Completed Sessions", 
            "Average Session Length", "Average Session Duration"], mPulseWidget=false, chart_title="Session Stats for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["area","line"]);
    catch y
        println("cell getAggSessionLength Exception ",y)
    end
end

function loadTimesParamsUPGD()
    try
        chartMedianLoadTimesByDimension(tv.startTimeUTC,tv.endTimeUTC,dimension=:params_u,minPercentage=0.5)

        df = getTopURLsByLoadTime(tv.startTimeUTC, tv.endTimeUTC, minPercentage=0.5);

        sort!(df, cols=:Requests, rev=true)
        display("text/html", """
        <h2 style="color:#ccc">Top URLs By Load Time for $productPageGroup (Ordered by Requests)</h2>
            """)
        beautifyDF(df);
        catch y
        println("cell chartMedianLoadParamsU Exception ",y)
    end
end

function medianTimesPGD()
    try
        chartMedianLoadTimesByDimension(tv.startTimeUTC, tv.endTimeUTC,dimension=:geo_cc,minPercentage=0.6)
        chartMedianLoadTimesByDimension(tv.startTimeUTC, tv.endTimeUTC; dimension=:user_agent_device_type, n=15, orderBy="frontend", minPercentage=0.001)
        printDF = getMedianLoadTimesByDimension(tv.startTimeUTC, tv.endTimeUTC; dimension=:user_agent_device_type, n=15, orderBy="frontend", minPercentage=0.001)
        beautifyDF(printDF)    
    catch y
        println("cell chartMedianLoad Exception ",y)
    end
end

function customRefPGD()
    customReferralsTable(localTable,productPageGroup)
end

function stdRefPGD()
    standardReferrals(localTable,productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString; limit=15)
end

function medLoadHttpPGD()
    try
        chartMedianLoadTimesByDimension(tv.startTimeUTC,tv.endTimeUTC,dimension=:http_referrer,minPercentage=0.5)
        chartMedianLoadTimesByDimension(tv.startTimeUTC,tv.endTimeUTC,dimension=:params_r,minPercentage=0.5)
        chartTopN(tv.startTimeUTC, tv.endTimeUTC; variable=:landingPages)
    catch y
        println("cell chartSlowestUrls Exception ",y)
    end
end

function treemapsPGD()
    deviceTypeTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
    browserFamilyTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
    countryTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
end

function dpQuartilesPGD()
    datePartQuartiles(tv.startTimeUTC, tv.endTimeUTC, tv.datePart)
end

function sunburst()
    try
        result10 = getAllPaths(tv.startTimeUTC, tv.endTimeUTC; n=60, f=getAbandonPaths);
        drawSunburst(result10[1]; totalPaths=result10[3])
    catch y
        println("cell chartMedianLoad Exception ",y)
    end
end

function pgTreemap()
    pageGroupTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
end

function bouncesPGD()
    chartLoadTimeMediansAndBounceRatesByPageGroup(tv.startTimeUTC,tv.endTimeUTC)
end

function pgQuartPGD()
    pageGroupQuartiles(table,productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.startTimeMsUTC,tv.endTimeMsUTC,tv.timeString;limit=10,showTable=false);
end

function activityImpactPGD()
    chartActivityImpactByPageGroup(tv.startTimeUTC, tv.endTimeUTC;n=10);
end
