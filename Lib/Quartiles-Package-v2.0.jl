function datePartQuartiles(startTime::DateTime, endTime::DateTime, datePart::Symbol)

    try
        chartSessionDurationQuantilesByDatepart(startTime, endTime, datePart)
    catch y
        println("datepartQuartiles Exception ",y)
    end
end

function pageGroupQuartiles(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        table = UP.beaconTable
        pageGroupPercentages = getGroupPercentages(TV.startTime, TV.endTime)
        pageGroups = pageGroupPercentages[:page_group][1:min(10,end)]
        pageGroups = "'"*join(pageGroups,"','")*"'"
        maxResources = UP.limitRows
        pageGroupQuartiles = select("""
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
        limit $(UP.limitRows)
        """);

        if (SP.devView)
            # pgAndCt = DataArray([]);
            # for x in eachrow(pageGroupQuartiles)
            #     newRow = [x[:page_group]*" \n \("*string(format(x[:count], commas=true))*"\)"]
            #     pgAndCt = vcat(pgAndCt, newRow)
            # end
            # pageGroupQuartiles[:page_group] = pgAndCt;
            # display(pageGroupQuartiles)
        end

        displayTitle(chart_title = "Top $(UP.limitRows) Page Group Time Quartiles", chart_info = ["Between $(TV.startTime) and $(TV.endTime)"],showTimeStamp=false)
        plotsDF = drawBoxPlots(pageGroupQuartiles, yAxisLabel = "Milliseconds");
    catch y
        println("pageGroupQuartiles Exception ",y)
    end
end
