#using Distributions

function buildTimeStats(localStatsDF::DataFrame,fieldStat::Symbol)
    try

        dv = Array{Float64}(localStatsDF[fieldStat])
        statsArr(v) = [round(v,0);round(v/1000.0,3);round(v/60000.0,1)]

        dv = dropna(dv)
        stats = DataFrame()
        stats[:unit] = ["milliseconds","seconds","minutes"]
        stats[:count] = size(dv,1)
        stats[:median] = statsArr(median(dv))
        stats[:rangeLower] = statsArr(0.0)
        stats[:rangeUpper] = statsArr(0.0)
        stats[:mean] = statsArr(mean(dv))
        stats[:stddev] = statsArr(std(dv))
        stats[:variance] = statsArr(var(dv))
        stats[:min] = statsArr(minimum(dv))
        stats[:max] = statsArr(maximum(dv))
        stats[:q25] = statsArr(quantile(dv,[0.25]))
        stats[:q75] = statsArr(quantile(dv,[0.75]))
        stats[:kurtosis] = statsArr(kurtosis(dv))
        stats[:skewness] = statsArr(skewness(dv))
        stats[:entropy] = statsArr(entropy(dv))
        stats[:modes] = statsArr(modes(dv)[1])

        return stats
    catch y
        println("buildTimeStats Exception ",y)
    end
end

function buildOtherStats(localStatsDF::DataFrame,fieldStat::Symbol,unit::ASCIIString)
    try

        dv = Array{Float64}(localStatsDF[fieldStat])
        statsArr(v) = [round(v,0)]

        dv = dropna(dv)
        stats = DataFrame()
        stats[:unit] = [unit]
        stats[:count] = size(dv,1)
        stats[:mean] = statsArr(mean(dv))
        stats[:median] = statsArr(median(dv))
        stats[:stddev] = statsArr(std(dv))
        stats[:variance] = statsArr(var(dv))
        stats[:min] = statsArr(minimum(dv))
        stats[:max] = statsArr(maximum(dv))
        stats[:rangeLower] = statsArr(0.0)
        stats[:rangeUpper] = statsArr(0.0)
        stats[:q25] = statsArr(quantile(dv,[0.25]))
        stats[:q75] = statsArr(quantile(dv,[0.75]))
        stats[:kurtosis] = statsArr(kurtosis(dv))
        stats[:skewness] = statsArr(skewness(dv))
        stats[:entropy] = statsArr(entropy(dv))
        stats[:modes] = statsArr(modes(dv)[1])

        return stats
    catch y
        println("buildOtherStats Exception ",y)
    end
end

function SetStatsRange(statsDF::DataFrame;
    useMedian::Bool=true,
    useStdDev::Bool=false, usePercent::Bool=false, useQuartile::Bool=false,
    stdDevLower::Float64=2.0, stdDevUpper::Float64=2.0,
    percentLower::Float64=0.90, percentUpper::Float64=1.10
)

    #println("useMedian=",useMedian," useStdDev=",useStdDev)

    if useMedian
        mid = statsDF[1:1,:median][1]
    else
        mid = statsDF[1:1,:mean][1]
    end

    if useStdDev
        stdVar = statsDF[1:1,:stddev][1]
        lowerSubtract = stdDevLower * stdVar
        upperSubtract = stdDevUpper * stdVar
        #println("mid=",mid," ls=",lowerSubtract," us=",upperSubtract," std=",stdVar)
        rl = mid - lowerSubtract
        ru = mid + upperSubtract
    elseif usePercent
        lowerSubtract = mid - (mid * percentLower)
        upperSubtract = (mid * percentUpper) - mid
        #println("mid=",mid," ls=",lowerSubtract," us=",upperSubtract," std=",stdVar)
        rl = mid - lowerSubtract
        ru = mid + upperSubtract
    elseif useQuartile
        rl = statsDF[1:1,:q25][1]
        ru = statsDF[1:1,:q75][1]
    end


    if rl < 1.0
        rl = 1.0
    end

    nrows = nrow(statsDF)
    for i in 1:nrows
        if i == 1
            denom = 1.0
        elseif i == 2
            denom = 1000.0
        else
            denom = 60000.0
        end
        statsDF[i:i,:rangeLower] = round(rl/denom)
        statsDF[i:i,:rangeUpper] = round(ru/denom)
    end

end

function displayStats(TV::TimeVars,statsDF::DataFrame,chartTitle::ASCIIString;showRowOne=true,showShort=true)
    try
        nrows = 1
        if !showRowOne
            nrows = nrow(statsDF)
        end

        if showShort
            ncols = 5
            prtDF = DataFrame(Any,nrows,ncols)

            for i in 1:nrows
                for j in 1:ncols
                    prtDF[i,j] = statsDF[i,j]
                end
            end

            prtDF = names!(prtDF,[Symbol("Unit");Symbol("Count");Symbol("Median");Symbol("Range Lower");Symbol("Range Upper")])

        else
            ncols = 16
            prtDF = DataFrame(Any,nrows,ncols)

            for i in 1:nrows
                for j in 1:ncols
                    prtDF[i,j] = statsDF[i,j]
                end
            end

            prtDF = names!(prtDF,[Symbol("Unit");Symbol("Count");Symbol("Median");Symbol("Range Lower");Symbol("Range Upper");
                Symbol("Mean");Symbol("Std Dev");Symbol("Variance");Symbol("Minimum");Symbol("Maximum");
                Symbol("Quartile 25");Symbol("Quartile 75");Symbol("Kurtosis");Symbol("Skewness");Symbol("Entropy");Symbol("Modes")
            ])

            #beautifyDF(statsDF[1:min(nrows,end),:])
        end

        displayTitle(chart_title=chartTitle,chart_info=[TV.timeString],showTimeStamp=false)
        beautifyDF(prtDF[1:min(nrows,end),:])

    catch y
        println("displayStats Exception ",y)
    end
end

function timeBeaconStats(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame;
    showAdditional::Bool=true, useStdDev::Bool=false, showShort::Bool=true,
    usePercent::Bool=false,useQuartile::Bool=false,
    percentLower::Float64=0.90,percentUpper::Float64=1.10
    )

    statsDF = DataFrame()

    if (UP.usePageLoad)
        statsDF = buildTimeStats(localTableDF,:timers_t_done)
    else
        statsDF = buildTimeStats(localTableDF,:timers_domready)
    end

    if nrow(statsDF) == 0
        return statsDF
    end

    if useStdDev || usePercent || useQuartile
        SetStatsRange(statsDF;useStdDev=useStdDev,usePercent=usePercent,useQuartile=useQuartile,percentLower=percentLower,percentUpper=percentUpper)

        # Store the range into the UP structure
        UP.timeLowerMs = convert(Int64,statsDF[1:1,:rangeLower][1])
        UP.timeUpperMs = convert(Int64,statsDF[1:1,:rangeUpper][1])
    else
        if SP.debugLevel > 0
            println("No Range was requested for Stats block")
        end
    end

    if (showAdditional)
        if (UP.usePageLoad)
            chartTitle = "Page Load Time Stats: $(UP.urlFull) for ($(UP.pageGroup),$(UP.deviceType),$(UP.agentOs))"
        else
            chartTitle = "Page Domain Ready Time Stats: $(UP.urlFull) for ($(UP.pageGroup),$(UP.deviceType),$(UP.agentOs))"
        end
        displayStats(TV,statsDF,chartTitle;showShort=showShort)
    end

    return statsDF
end

function anyBeaconStats(TV::TimeVars,localTableDF::DataFrame,useField::Symbol;
    showAdditional::Bool=true, useStdDev::Bool=false, showShort::Bool=true,
    usePercent::Bool=false,useQuartile::Bool=false,chartTitle::ASCIIString="SKIP"
    )

    statsDF = DataFrame()

    statsDF = buildTimeStats(localTableDF,useField)

    if nrow(statsDF) == 0
        return statsDF
    end

    SetStatsRange(statsDF;useStdDev=useStdDev,usePercent=usePercent,useQuartile=useQuartile)

    if (showAdditional && chartTitle != "SKIP")
        displayStats(TV,statsDF,chartTitle;showShort=showShort)
    end

    return statsDF
end

# old routines below try to replace

function beaconStats(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame;showAdditional::Bool=true)

    if SP.debugLevel > 8
        println("Starting beaconStats")
    end

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


function basicFieldStats(localStatsDF::DataFrame,fieldStat::Symbol)
    try

        dv = Array{Float64}(localStatsDF[fieldStat])
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

function basicStats(UP::UrlParams,localStatsDF::DataFrame)
    try

        if UP.usePageLoad
            dv = Array{Float64}(localStatsDF[:timers_t_done])
        else
            dv = Array{Float64}(localStatsDF[:timers_domready])
        end

        statsArr(v) = [round(v,0),round(v/1000.0,3),round(v/60000.0,1)]

        dv = dropna(dv)
        stats = DataFrame()
        stats[:unit] = ["milliseconds","seconds","minutes"]
        stats[:count] = size(dv,1)
        stats[:mean] = statsArr(mean(dv))
        stats[:median] = statsArr(median(dv))
        stats[:stddev] = statsArr(std(dv))
        stats[:variance] = statsArr(var(dv))
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

# Safer Version with array of floats
function basicStatsFromDV(dv::Array)
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
        println("basicStatsFromDV Array Exception ",y)
    end
end

function runningStats(UP::UrlParams,year::Int64,month::Int64,day::Int64,hour::Int64,localStatsDF::DataFrame)
    try

        if UP.usePageLoad
            dv = Array{Float64}(localStatsDF[:timers_t_done])
        else
            dv = Array{Float64}(localStatsDF[:timers_domready])
        end

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
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1.0 end
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
        [Symbol("Page Views"),Symbol("Mean(ms)"),Symbol("Median(ms)"),Symbol("Min(ms)"),Symbol("Max(ms)"),Symbol("25 Percentile"),Symbol("75 Percentile")])

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

function dataframeFieldStats(TV::TimeVars,SP::ShowParams,localStatsDF::DataFrame,statsField::Symbol,statsFieldTitle::ASCIIString)
    try
        if SP.debugLevel > 8
            println("Starting dataframeFieldStats")
        end

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
        localStatsDF = Array{Float64}(statsBtViewTableToDF(UP));

        if size(localStatsDF,1) == 0
            println("No data returned")
            return
        end

        statsDF = basicStats(UP,localStatsDF)

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

function fetchGraph7Stats(UP::UrlParams)

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    try
        localStatsDF = statsBtViewTableToExtraDF(UP);
        #statsDF = basicStats(UP,localStatsDF)
        #medianThreshold = statsDF[1:1,:median][1]

        #displayTitle(chart_title = "Raw Data Stats for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        #beautifyDF(statsDF[:,:])

        return localStatsDF

    catch y
        println("fetchGraph7Stats Exception ",y)
    end
end

function distributionStats(UP::UrlParams)
    try
        # Does not work

        #statsDV = Float64()
        #statsDV = [18585.0,9499.0,19617.0,9624.0,28572.0,4255.0,9198.0,21984.0,27154.0,34180.0,14190.0,5248.0,6802.0,55169.0,55917.0,15414.0,33405.0]
        localStatsDF = statsBtViewTableToDF(UP);
        if (UP.usePageLoad)
            statsDV = localStatsDF[:timers_t_done]
        else
            statsDV = localStatsDF[:timers_domready]
        end
        n = fit(Normal, statsDV)
        println("normal ",n)
        println(dof(n))
        #println(statsDV)
        #println(fit(Normal, statsDV))
        #paramsD = params(statDV)
        #println("paramsD ",paramsD)

        #displayTitle(chart_title = "Raw Data Stats for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        #beautifyDF(statsDF[:,:])
    catch y
        println("distributionStats Exception ",y)
    end
end

function createAllStatsDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    year1 = Dates.year(TV.startTime)
    month1 = Dates.month(TV.startTime)
    day1 = Dates.day(TV.startTime)
    hour1 = Dates.hour(TV.startTime)
    #minute1 = Dates.minute(TV.startTime)
    #study whole hours only
    minute1 = 0

    year2 = Dates.year(TV.endTime)
    month2 = Dates.month(TV.endTime)
    day2 = Dates.day(TV.endTime)
    hour2 = Dates.hour(TV.endTime)
    minute2 = 59

    AllStatsDF = DataFrame()

    # todo Month and Year

    i = 0
    initDataFrame = true

    for (day = day1:day2)
        if SP.debugLevel > 4
            println("Day ",day)
        end
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
            if SP.debugLevel > 4
                println("Year ",year1," Month ",month1," Day ",day," Hour ",hour," M1 ",minute1," M2 ",minute2)
            end
            i += 1
            startTime = DateTime(year1,month1,day,hour,minute1)
            endTime   = DateTime(year2,month2,day,hour,minute2)

            startTimeMs = datetimeToMs(startTime)
            endTimeMs = datetimeToMs(endTime)

            startTimeUTC = datetimeToUTC(startTime, TimeZone("America/New_York"))
            endTimeUTC = datetimeToUTC(endTime, TimeZone("America/New_York"))
            startTimeMsUTC = datetimeToMs(startTimeUTC)
            endTimeMsUTC = datetimeToMs(endTimeUTC)


            localStatsDF = statsBtViewByHourToDF(UP.btView,startTimeMsUTC,endTimeMsUTC)
            if size(localStatsDF,1) > 0
                statsDF = runningStats(UP,year1,month1,day,hour,localStatsDF)

                if size(statsDF,1) > 0
                    if SP.debugLevel > 8
                        beautifyDF(statsDF)
                    end
                    if (initDataFrame)
                        AllStatsDF = statsDF
                        initDataFrame = false
                    else
                        append!(AllStatsDF,statsDF)
                    end
                end
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

    if (graphType == 7)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:timersdone]
            drawDF[:data1] = AllStatsDF[:count]

            c3 = drawC3Viz(drawDF; axisLabels=["Page Load"],dataNames=["Page Load"],
                mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["bar"])
        catch y
            println("draw Timers exception ",y)
        end
        return
    end


end

function longTimesFATS(TV::TimeVars,UP::UrlParams,localStats2::DataFrame)
    try
        if (UP.usePageLoad)
            dv = localStats2[:timers_t_done]
        else
            dv = localStats2[:timers_domready]
        end

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
        if (rangeLowerByStd < 0.0) rangeLowerByStd = 1.0 end
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
