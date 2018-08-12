function datePartQuartiles(TV::TimeVars)

    try
        #Quartiles require more room so bigger datePart
        datePart = :minute
        if TV.datePart == :minute
            datePart = :hour
        elseif TV.datePart == :hour
            datePart = :day
        end

        chartSessionDurationQuantilesByDatepart(TV.startTimeUTC, TV.endTimeUTC, datePart)

    catch y
        println("datepartQuartiles Exception ",y)
    end
end

function pageGroupQuartiles(TV::TimeVars,UP::UrlParams,SP::ShowParams,showQuartiles::Int64=10)
    try
        if SP.debugLevel > 8
            println("Starting pageGroupQuartiles")
        end

        table = UP.beaconTable
        pageGroupPercentages = getGroupPercentages(TV.startTimeUTC, TV.endTimeUTC)
        pageGroups = pageGroupPercentages[:page_group][1:min(10,end)]
        pageGroups = "'"*join(pageGroups,"','")*"'"
        maxResources = showQuartiles
        pageGroupQuartilesDF = select("""\
            SELECT DISTINCT page_group,
                MIN(timers_t_done) OVER (PARTITION BY page_group) AS minimum,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY timers_t_done) OVER (PARTITION BY page_group) AS lower_quartile,
                MEDIAN(timers_t_done) OVER (PARTITION BY page_group) AS "median",
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timers_t_done) OVER (PARTITION BY page_group) AS upper_quartile,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY timers_t_done) OVER (PARTITION BY page_group) AS maximum,
                COUNT(*)OVER (PARTITION BY page_group)
                --MAX(timers_t_done) OVER (PARTITION BY page_group) AS maximum
            FROM $table
            WHERE
                page_group IN ($(pageGroups)) and
                beacon_type = 'page view' and
                "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                params_rt_quit IS NULL and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                user_agent_os ilike '$(UP.agentOs)' and
                timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
            ORDER BY count DESC
            limit $(showQuartiles)
        """);

        displayTitle(chart_title = "Top Page Group Times", chart_info = [TV.timeString],showTimeStamp=false)
        plotsDF = drawBoxPlots(pageGroupQuartilesDF, yAxisLabel = "Milliseconds");
    catch y
        println("pageGroupQuartiles Exception ",y)
    end
end
