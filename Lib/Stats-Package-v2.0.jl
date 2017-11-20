function basicStats(localStatsDF::DataFrame,productPageGroup::ASCIIString,startTimeMs::Int64, endTimeMS::Int64)
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

function showLimitedStats(statsDF::DataFrame,chartTitle::ASCIIString)
    try
        printStatsDF = names!(statsDF[:,:],
        [symbol("Page Views"),symbol("Mean(ms)"),symbol("Median(ms)"),symbol("Min(ms)"),symbol("Max(ms)"),symbol("25 Percentile"),symbol("75 Percentile")])

        displayTitle(chart_title = chartTitle, chart_info = [tv.timeString],showTimeStamp=false)
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

#function beaconStatsPBI(localTableDF::DataFrame,fullUrl::ASCIIString,deviceType::ASCIIString;showAdditional::Bool=true,usePageLoad::Bool=true)
function beaconStats(UP::UrlParams,SP::ShowParams;showAdditional::Bool=true)

    if (UP.usePageLoad)
        dv = localTableDF[:timers_t_done]
    else
        dv = localTableDF[:timers_domready]
    end

    # Get page views #, median, min, max and more
    statsDF = limitedStatsFromDV(dv)

    if (showAdditional)
        if (UP.usePageLoad)
            chartTitle = "Page Load Time Stats: $(UP.urlFull) for $(UP.deviceType)"
        else
            chartTitle = "Page Domain Ready Time Stats: $(UP.urlFull) for $(UP.deviceType)"
        end
        showLimitedStats(statsDF,chartTitle)
    end
    return statsDF
end
