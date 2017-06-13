function showPeakTable(timeString::ASCIIString, productPageGroup::ASCIIString, startTime::DateTime, endTime::DateTime; showStartTime30::Bool=false, showStartTime90::Bool=false,tableRange::ASCIIString="7 Day ")
    try
        startTime30 = DateTime(endTime - Day(30))
        startTime90 = DateTime(endTime - Day(90))

        chartTitle = "Peak Arrivals"
        if productPageGroup != "NONE"
            chartTitle *= " for $(productPageGroup)"
        end
        displayTitle(chart_title = chartTitle, chart_info = [timeString], showTimeStamp=false)

        peakArrivals = getPeak(startTime, endTime, [ :day, :hour, :minute])
        peakArrivals[1:1,1] = tableRange * "Peak Day"
        peakArrivals[2:2,1] = tableRange * "Peak Hour"
        peakArrivals[3:3,1] = tableRange * "Peak Minute"

        if (showStartTime30)
            peakArrivals30 = getPeak(startTime30, endTime, [ :day, :hour, :minute])
            push!(peakArrivals,["30 Day Peak Day";peakArrivals30[1:1,2];peakArrivals30[1:1,3]])
            push!(peakArrivals,["30 Day Peak Hour";peakArrivals30[2:2,2];peakArrivals30[2:2,3]])
            push!(peakArrivals,["30 Day Peak Minute";peakArrivals30[3:3,2];peakArrivals30[3:3,3]])
        end

        if (showStartTime90)
            peakArrivals90 = getPeak(startTime90, endTime, [ :day, :hour, :minute])
            push!(peakArrivals,["90 Day Peak Day";peakArrivals90[1:1,2];peakArrivals90[1:1,3]])
            push!(peakArrivals,["90 Day Peak Hour";peakArrivals90[2:2,2];peakArrivals90[2:2,3]])
            push!(peakArrivals,["90 Day Peak Minute";peakArrivals90[3:3,2];peakArrivals90[3:3,3]])
        end

        #dup? displayTitle(chart_title = "All Page Views",showTimeStamp=false)
        beautifyDF(peakArrivals, transformer=(column, value, rowindex, colindex) -> column == symbol("Page Views") ? format(value, commas=true) : value)
    catch y
        println("showPeakTable Exception ",y)
    end
end        
