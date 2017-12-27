function showPeakTable(TV::TimeVars,UP::UrlParams,SP::ShowParams; showStartTime30::Bool=false, tableRange::ASCIIString="7 Day ")
    try
        if SP.debugLevel > 8
            println("Starting showPeakTable")
        end

        startTime30 = DateTime(TV.endTimeUTC - Day(30))
        startTime90 = DateTime(TV.endTimeUTC - Day(90))

        usePageGroup = false
        chartTitle = "Peak Arrivals"
        if UP.pageGroup != "NONE"
            chartTitle *= " for $(UP.pageGroup)"
            usePageGroup = true
        end

        if UP.deviceType != "%"
            chartTitle *= " Dev=$(UP.deviceType)"
        end

        if UP.agentOs != "%"
            chartTitle *= " OS=$(UP.agentOs)"
        end

        # todo SQLFilter for agentOs and other fields
        # todo Pagegroup parameter for non-empty page groups

        displayTitle(chart_title = chartTitle, chart_info = [TV.timeString], showTimeStamp=false)

        myFilter = SQLFilter[
            ilike("user_agent_device_type",UP.deviceType),
            ilike("user_agent_os",UP.agentOs)
            ]

        if (usePageGroup)
            peakArrivals = getPeak(TV.startTimeUTC, TV.endTimeUTC, [ :day, :hour, :minute]; pageGroup=UP.pageGroup, filters=myFilter)
        else
            peakArrivals = getPeak(TV.startTimeUTC, TV.endTimeUTC, [ :day, :hour, :minute]; filters=myFilter)
        end

        peakArrivals[1:1,1] = tableRange * "Peak Day"
        peakArrivals[2:2,1] = tableRange * "Peak Hour"
        peakArrivals[3:3,1] = tableRange * "Peak Minute"

        if (showStartTime30)
            if (usePageGroup)
                peakArrivals30 = getPeak(startTime30, TV.endTimeUTC, [ :day, :hour, :minute]; pageGroup=UP.pageGroup, filters=myFilter)
            else
                peakArrivals30 = getPeak(startTime30, TV.endTimeUTC, [ :day, :hour, :minute]; filters=myFilter)
            end

            push!(peakArrivals,["30 Day Peak Day";peakArrivals30[1:1,2];peakArrivals30[1:1,3]])
            push!(peakArrivals,["30 Day Peak Hour";peakArrivals30[2:2,2];peakArrivals30[2:2,3]])
            push!(peakArrivals,["30 Day Peak Minute";peakArrivals30[3:3,2];peakArrivals30[3:3,3]])
        end

        #dup? displayTitle(chart_title = "All Page Views",showTimeStamp=false)
        beautifyDF(peakArrivals, transformer=(column, value, rowindex, colindex) -> column == Symbol("Page Views") ? format(value, commas=true) : value)
    catch y
        println("showPeakTable Exception ",y)
    end
end
