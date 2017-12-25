function basicFieldStats(localStatsDF::DataFrame,fieldStat::Symbol)
    try

        dv = localStatsDF[fieldStat]
        statsArr(v) = [round(v,0),round(v/1000.0,3),round(v/60000.0,1)]

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
        if (rangeLowerBy25p < 1.0) rangeLowerBy25p = 1000.0 end
        rangeUpperBy25p = stats[1,:median] + tenpercent

        # Range 1 Std Dev
        rangeLowerByStd = stats[1,:median] - (3 * stats[1,:stddev])
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1000.0 end
        rangeUpperByStd = stats[1,:median] + (3 * stats[1,:stddev])

        stats[:LowerBy25p] = statsArr(rangeLowerBy25p)
        stats[:UpperBy25p] = statsArr(rangeUpperBy25p)
        stats[:LowerBy3Stddev] = statsArr(rangeLowerByStd)
        stats[:UpperBy3Stddev] = statsArr(rangeUpperByStd)
        return stats
    catch y
        println("basicFieldStats Exception ",y)
    end
end

function basicStats(localStatsDF::DataFrame)
    try

        dv = localStatsDF[:timers_t_done]
        statsArr(v) = [round(v,0),round(v/1000.0,3),round(v/60000.0,1)]

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
        if (rangeLowerBy25p < 1.0) rangeLowerBy25p = 1000.0 end
        rangeUpperBy25p = stats[1,:median] + tenpercent

        # Range 1 Std Dev
        rangeLowerByStd = stats[1,:median] - (3 * stats[1,:stddev])
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1000.0 end
        rangeUpperByStd = stats[1,:median] + (3 * stats[1,:stddev])

        stats[:LowerBy25p] = statsArr(rangeLowerBy25p)
        stats[:UpperBy25p] = statsArr(rangeUpperBy25p)
        stats[:LowerBy3Stddev] = statsArr(rangeLowerByStd)
        stats[:UpperBy3Stddev] = statsArr(rangeUpperByStd)
        return stats
    catch y
        println("basicStats Exception ",y)
    end
end

function basicStatsFromDV(dv::DataVector)
    try

        statsArr(v) = round(v,0)

        dv = dropna(dv)
        stats = DataFrame()
        stats[:unit] = ["milliseconds"]
        stats[:count] = size(dv,1)
        stats[:mean] = statsArr(mean(dv))
        stats[:median] = statsArr(median(dv))
        stats[:stddev] = statsArr(std(dv))
        stats[:variance] = statsArr(var(dv))
        stats[:min] = statsArr(minimum(dv))
        stats[:max] = statsArr(maximum(dv))
        stats[:q25] = statsArr(quantile(dv,[0.25]))
        stats[:q75] = statsArr(quantile(dv,[0.75]))
        stats[:kurtosis] = round((kurtosis(dv)),1)
        stats[:skewness] = round((skewness(dv)),1)
        stats[:entropy] = round((entropy(dv)),0)
        stats[:modes] = (modes(dv)[1])

        # Range 1 Std Dev
        rangeLowerByStd = stats[1,:median] - (3 * stats[1,:stddev])
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1.0 end
        rangeUpperByStd = stats[1,:median] + (3 * stats[1,:stddev])

        stats[:LowerBy3Stddev] = (rangeLowerByStd)
        stats[:UpperBy3Stddev] = (rangeUpperByStd)
        return stats
    catch y
        println("basicStatsFromDV Exception ",y)
    end
end

function runningStats(year::Int64,month::Int64,day::Int64,hour::Int64,localStatsDF::DataFrame)
    try

        dv = localStatsDF[:timers_t_done]
        statsArr(v) = round(v/1000.0,3)

        dv = dropna(dv)
        stats = DataFrame()
        stats[:datetime] = DateTime(year,month,day,hour,0)
        stats[:year] = year
        stats[:month] = month
        stats[:day] = day
        stats[:hour] = hour
        stats[:unit] = ["seconds"]
        stats[:count] = size(dv,1)
        stats[:mean] = statsArr(mean(dv))
        stats[:median] = statsArr(median(dv))
        stats[:stddev] = statsArr(std(dv))
        stats[:variance] = statsArr(var(dv))
        stats[:min] = statsArr(minimum(dv))
        stats[:max] = statsArr(maximum(dv))
        stats[:q25] = statsArr(quantile(dv,[0.25]))
        stats[:q75] = statsArr(quantile(dv,[0.75]))
        stats[:kurtosis] = (kurtosis(dv))
        stats[:skewness] = (skewness(dv))
        stats[:entropy] = (entropy(dv))
        stats[:modes] = (modes(dv)[1])

        # Range 1 Std Dev
        rangeLowerByStd = stats[1,:median] - (3 * stats[1,:stddev])
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1 end
        rangeUpperByStd = stats[1,:median] + (3 * stats[1,:stddev])

        stats[:LowerBy3Stddev] = (rangeLowerByStd)
        stats[:UpperBy3Stddev] = (rangeUpperByStd)
        return stats
    catch y
        println("runningStats Exception ",y)
    end
end

function showLimitedStats(TV::TimeVars,statsDF::DataFrame,chartTitle::ASCIIString)
    try
        printStatsDF = names!(statsDF[:,:],
        [symbol("Page Views"),symbol("Mean(ms)"),symbol("Median(ms)"),symbol("Min(ms)"),symbol("Max(ms)"),symbol("25 Percentile"),symbol("75 Percentile")])

        displayTitle(chart_title = chartTitle, chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(printStatsDF[:,:])
    catch y
        println("showLimitedStats Exception ",y)
    end
end

function limitedStatsFromDV(dv::DataVector)
    try

        statsArr(v) = round(v,0)

        dv = dropna(dv)
        stats = DataFrame()
        stats[:count] = size(dv,1)
        if (stats[:count][1] == 0)
            stats[:mean] = 0
            stats[:median] = 0
            stats[:min] = 0
            stats[:max] = 0
            stats[:q25] = 0
            stats[:q75] = 0
            return stats
        end

        stats[:mean] = statsArr(mean(dv))
        stats[:median] = statsArr(median(dv))
        stats[:min] = statsArr(minimum(dv))
        stats[:max] = statsArr(maximum(dv))
        stats[:q25] = statsArr(quantile(dv,[0.25]))
        stats[:q75] = statsArr(quantile(dv,[0.75]))

        return stats

    catch y
        println("limitedStatsFromDV Exception ",y)
    end
end

function dataframeFieldStats(TV::TimeVars,UP::UrlParams,SP::ShowParams,localStatsDF::DataFrame,statsField::Symbol,statsFieldTitle::ASCIIString)
    try

        statsDF = basicFieldStats(localStatsDF,statsField)
        #medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Dataframe Stats $statsFieldTitle", chart_info = [TV.timeString],showTimeStamp=false)
        if SP.devView
            beautifyDF(statsDF[:,:])
        else
            beautifyDF(statsDF[2:2,:])
        end

        return statsDF

    catch y
        println("dataframeFieldStats Exception ",y)
    end
end


function beaconViewStats(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        setTable(UP.btView)
        localStatsDF = statsTableCreateDF(UP.btView,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC);

        if size(localStatsDF,1) == 0
            println("No data returned")
            return
        end

        statsDF = basicStats(localStatsDF)

        if size(statsDF,1) == 0
            println("No statsDF data")
            return
        end

        medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Stats for current view", chart_info = [TV.timeString],showTimeStamp=false)
        if SP.devView
            beautifyDF(statsDF[:,:])
        else
            beautifyDF(statsDF[2:2,:])
        end

        return statsDF

        #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(UP.pageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
        #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))
    catch y
        println("beaconViewStats Exception ",y)
    end
end


function beaconStats(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame;showAdditional::Bool=true)

    if (UP.usePageLoad)
        dv = localTableDF[:timers_t_done]
    else
        dv = localTableDF[:timers_domready]
    end

    # Get page views #, median, min, max and more
    statsDF = limitedStatsFromDV(dv)

    if (showAdditional)
        if (UP.usePageLoad)
            chartTitle = "Page Load Time Stats: $(UP.urlFull) for ($(UP.pageGroup),$(UP.deviceType),$(UP.agentOs))"
        else
            chartTitle = "Page Domain Ready Time Stats: $(UP.urlFull) for ($(UP.pageGroup),$(UP.deviceType),$(UP.agentOs))"
        end
        showLimitedStats(TV,statsDF,chartTitle)
    end
    return statsDF
end

function rawStatsSROS(TV::TimeVars,UP::UrlParams)

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = Int64
    try
        localStatsDF = statsTableCreateDF(UP.btView,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC);
        #statsDF = basicStats(localStatsDF, UP.pageGroup, TV.startTimeMsUTC, TV.endTimeMsUTC)
        statsDF = basicStats(localStatsDF)
        medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
    catch y
        println("rawStatsSROS Exception ",y)
    end
end

function createAllStatsDF(TV::TimeVars,UP::UrlParams)

    year1 = Dates.year(TV.startTimeUTC)
    month1 = Dates.month(TV.startTimeUTC)
    day1 = Dates.day(TV.startTimeUTC)
    hour1 = Dates.hour(TV.startTimeUTC)
    #minute1 = Dates.minute(TV.startTimeUTC)
    #study whole hours only
    minute1 = 0

    year2 = Dates.year(TV.endTimeUTC)
    month2 = Dates.month(TV.endTimeUTC)
    day2 = Dates.day(TV.endTimeUTC)
    hour2 = Dates.hour(TV.endTimeUTC)
    #minute2 = Dates.minute(TV.endTimeUTC)
    minute2 = 59

    AllStatsDF = DataFrame()

    # todo Month and Year

    i = 0
    for (day = day1:day2)
        #println("Day ",day)
        startHour = hour1
        endHour = hour2
        if (day == day1 && day != day2)
            endHour = 23
        end

        if (day != day1 && day != day2)
            startHour = 0
            endHour = 23
        end

        if (day != day1 && day == day2)
            startHour = 0
        end

        for (hour = startHour:endHour)
            #println("Year ",year1," Month ",month1," Day ",day," Hour ",hour," M1 ",minute1," M2 ",minute2)
            i += 1
            startTime = DateTime(year1,month1,day,hour,minute1)
            endTime   = DateTime(year2,month2,day,hour,minute2)

            startTimeMs = datetimeToMs(startTime)
            endTimeMs = datetimeToMs(endTime)

            localStatsDF = query("""\
                select
                    timers_t_done
                from $(UP.beaconTable)
                where
                    page_group ilike '$(UP.pageGroup)' and
                    "timestamp" between $startTimeMs and $endTimeMs and
                    timers_t_done >= 1 and timers_t_done < 600000
            """)
            statsDF = runningStats(year1,month1,day,hour,localStatsDF)
            #display(statsDF)
            if (i == 1)
                AllStatsDF = deepcopy(statsDF)
            else
                append!(AllStatsDF,statsDF)
            end
        end
    end

    beautifyDF(AllStatsDF)
    return AllStatsDF
end

function drawC3VizConverter(UP::UrlParams,AllStatsDF::DataFrame;graphType::Int64=1)

    if (graphType == 1)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:mean]
            drawDF[:data2] = AllStatsDF[:median]
            drawDF[:data3] = AllStatsDF[:stddev]

            c3 = drawC3Viz(drawDF; axisLabels=["Mean","Median", "Standard Dev"],dataNames=["Mean",
                "Median", "Standard Deviation"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line","line","line"])
        catch y
            println("drawC3VizConverter exception ",y)
        end
        return
    end

    if (graphType == 2)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:q25]
            drawDF[:data2] = AllStatsDF[:median]
            drawDF[:data3] = AllStatsDF[:q75]

        c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],
            dataNames=["Q25","Q50","Q75"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line","line"])
        catch y
            println("drawQ25Q75 exception ",y)
        end
        return
    end

    if (graphType == 3)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:kurtosis]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Kurtosis"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawKurt exception ",y)
        end
        return
    end

    if (graphType == 4)

        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data2] = AllStatsDF[:skewness]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Skewness"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawSkew exception ",y)
        end
    end

    if (graphType == 5)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:entropy]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Entropy"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawEntropy exception ",y)
        end
    end

    if (graphType == 6)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:modes]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Modes"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawModes exception ",y)
        end
    end
end

function localStatsFATS(TV::TimeVars,UP::UrlParams,statsDF::DataFrame)
    try
        LowerBy3Stddev = statsDF[1:1,:LowerBy3Stddev][1]
        UpperBy3Stddev = statsDF[1:1,:UpperBy3Stddev][1]
        UpperBy25p = statsDF[1:1,:UpperBy25p][1]

        localStats2 = query("""\
            select
                "timestamp", timers_t_done, session_id
            from $(UP.btView) where
                page_group ilike '$(UP.pageGroup)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                timers_t_done > $(UpperBy25p)
        """)

        return localStats2

    catch y
        println("localStatsFATS Exception ",y)
    end
end

function longTimesFATS(TV::TimeVars,UP::UrlParams,localStats2::DataFrame)
    try
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

        displayTitle(chart_title = "Table Data Stats Outside 3 Stddev for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        if (SP.devView)
            beautifyDF(stats[:,:])
        else
            beautifyDF(stats[2:2,:])
        end
    catch y
        println("longTimesFATS Exception",y)
    end
end
