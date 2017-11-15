function defaultTableFATS(TV::TimeVars,UP::UrlParams)
    try
        localTable = UP.btView

        # Create view to query only product page_group
        query("""drop view if exists $localTable""")

        query("""\
            create or replace view $localTable as
                (select *,"timestamp" as listtime from $table where
                page_group = '$(UP.pageGroup)' and 
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
        )""")
        ;

    catch y
        println("Query exception ",y)
    end
end

function rawStatsFATS(TV::TimeVars,UP::UrlParams)

    localStatsDF = DataFrame()
    medianThreshold = Int64
    try
        localStatsDF = statsTableDF(UP.btView,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC);
        statsDF = basicStats(localStatsDF, UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
        medianThreshold = statsDF[1:1,:median][1]

        displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
        #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))

        return statsDF

    catch y
        println("rawStatsFATS Exception ",y)
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
        beautifyDF(stats[:,:])
        #by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1)))
        #c3 = drawC3Viz(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))); columnNames=[:timers_t_done], axisLabels=["Page Load Times"],dataNames=["Completed Sessions"], mPulseWidget=false, chart_title="Page Load for $(productPageGroup) Page Group", y2Data=["data2"], vizTypes=["line"])
        #drawHistogram(by(localTableDF, :timers_t_done, df->DataFrame(N=size(df,1))))
    catch y
        println("longTimesFATS Exception",y)
    end
end

function graphLongTimesFATS(localStats2::DataFrame)
    dataNames = ["Current Long Page Views Completed"]
    axisLabels = ["Timestamps", "Milliseconds to Finish"]

    chart_title="Points above 3 Standard Dev"
    chart_info=["These are the long points only limited to the first 500"]

    colors = ["#EEC584", "rgb(85,134,140)"]

    # kwargs
    point_r = 2

    drawC3Viz(localStats2[1:500,:];  dataNames=dataNames, axisLabels=axisLabels, chart_title=chart_title, chart_info=chart_info, colors=colors, point_r=point_r);
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
                page_group = '$(UP.pageGroup)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                timers_t_done > $(UpperBy25p)
        """)

        return localStats2

    catch y
        println("localStatsFATS Exception ",y)
    end
end
