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
        pageGroups = pageGroupPercentages[:pagegroupname][1:min(10,end)]
        pageGroups = "'"*join(pageGroups,"','")*"'"
        maxResources = showQuartiles
        pageGroupQuartilesDF = select("""\
            SELECT DISTINCT pagegroupname,
                MIN(pageloadtime) OVER (PARTITION BY pagegroupname) AS minimum,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pageloadtime) OVER (PARTITION BY pagegroupname) AS lower_quartile,
                MEDIAN(pageloadtime) OVER (PARTITION BY pagegroupname) AS "median",
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pageloadtime) OVER (PARTITION BY pagegroupname) AS upper_quartile,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY pageloadtime) OVER (PARTITION BY pagegroupname) AS maximum,
                COUNT(*)OVER (PARTITION BY pagegroupname)
                --MAX(pageloadtime) OVER (PARTITION BY pagegroupname) AS maximum
            FROM $table
            WHERE
                pagegroupname IN ($(pageGroups)) and
                beacontypename = 'page view' and
                timestamp between $(TV.startTimeMs) and $(TV.endTimeMs) and
                paramsrtquit IS NULL and
                paramsu ilike '$(UP.urlRegEx)' and
                devicetypename ilike '$(UP.deviceType)' and
                operatingsystemname ilike '$(UP.agentOs)' and
                pageloadtime >= $(UP.timeLowerMs) and pageloadtime < $(UP.timeUpperMs)
            ORDER BY count DESC
            limit $(showQuartiles)
        """);

        displayTitle(chart_title = "Top Page Group Times", chart_info = [TV.timeString],showTimeStamp=false)
        plotsDF = drawBoxPlots(pageGroupQuartilesDF, yAxisLabel = "Milliseconds");
    catch y
        println("pageGroupQuartiles Exception ",y)
    end
end
