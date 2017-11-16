function datePartQuartiles(startTime::DateTime, endTime::DateTime, datePart::Symbol)

    try
        chartSessionDurationQuantilesByDatepart(startTime, endTime, datePart)
    catch y
        println("datepartQuartiles Exception ",y)
    end
end

function pageGroupQuartiles(table::ASCIIString,productPageGroup::ASCIIString,startTime::DateTime,endTime::DateTime,startTimeMs::Int64,endTimeMs::Int64,timeString::ASCIIString;showTable::Bool=false,limit::Int64=15)
    
    try 
        pageGroupPercentages = getGroupPercentages(startTime, endTime)
        pageGroups = pageGroupPercentages[:page_group][1:min(10,end)]
        pageGroups = "'"*join(pageGroups,"','")*"'"
        maxResources = limit
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
            WHERE "timestamp" BETWEEN $startTimeMs AND $endTimeMs
            AND params_rt_quit IS NULL
            AND timers_t_done BETWEEN 1 AND 600000
            AND page_group IN ($(pageGroups))
            ORDER BY count DESC
            limit $limit
        """);

        if (showTable)
            # pgAndCt = DataArray([]);
            # for x in eachrow(pageGroupQuartiles)
            #     newRow = [x[:page_group]*" \n \("*string(format(x[:count], commas=true))*"\)"]
            #     pgAndCt = vcat(pgAndCt, newRow)
            # end
            # pageGroupQuartiles[:page_group] = pgAndCt;
            # display(pageGroupQuartiles)
        end

        displayTitle(chart_title = "Top $(limit) Page Group Time Quartiles", chart_info = ["Between $startTime and $endTime"],showTimeStamp=false)
        plotsDF = drawBoxPlots(pageGroupQuartiles, yAxisLabel = "Milliseconds");
    catch y
        println("pageGroupQuartiles Exception ",y)
    end
end
