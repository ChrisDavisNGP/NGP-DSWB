function bigPages1SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        localTable = UP.btView

        statsDF = DataFrame()

        localDF = query("""SELECT params_dom_sz FROM $localTable""")
        dv = localDF[:params_dom_sz]
        statsDF = basicStatsFromDV(dv)
        statsDF[:unit] = "KBytes"
        minSizeBytes = statsDF[1:1,:UpperBy3Stddev][1]

        displayTitle(chart_title = "Domain Size in KB", showTimeStamp=false)
        beautifyDF(statsDF)

        return minSizeBytes

    catch y
        println("setupLocalTable Exception ",y)
    end
end

function bigPages2SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Int64)

    try
        localTable = UP.btView
        displayTitle(chart_title = "Big Pages (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesDF = query("""\
            select
                params_dom_sz,
                timers_t_page load_time,
                params_u urlgroup
            from $localTable
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            order by params_dom_sz desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(bigPagesDF;limit=SP.scrubUrlChars)
        beautifyDF(names!(bigPagesDF[1:min(SP.showLines,end),:],[symbol("Size");symbol("Load Time (ms)");symbol("URL")]))
    catch y
        println("bigPages2SRFLP Exception ",y)
    end
end

function bigPages3SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Int64)

    try
        localTable = UP.btView

        displayTitle(chart_title = "Big Pages By Average Size (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigAveragePagesDF = query("""\
            select
                count(*),
                avg(params_dom_sz) as size,
                avg(timers_t_page) as load,
                params_u as urlgroup
            from $localTable
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            group by urlgroup
            order by size desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(bigAveragePagesDF;limit=SP.scrubUrlChars)
        beautifyDF(names!(bigAveragePagesDF[1:min(SP.showLines,end),:],[symbol("Count");symbol("Size");symbol("Load Time (ms)");symbol("URL")]))
    catch y
        println("bigPages3SRFLP Exception ",y)
    end
end

function bigPages4SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Int64)

    try
        localTable = UP.btView

        displayTitle(chart_title = "Big Pages With Session ID (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesSessionsDF = query("""\
            select
                params_dom_sz dom_size,
                session_id,
                "timestamp",
                params_u urlgroup
            from $localTable
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes) and
                session_id IS NOT NULL
            order by dom_size desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(bigPagesSessionsDF;limit=SP.scrubUrlChars)
        beautifyDF(names!(bigPagesSessionsDF[1:min(end,SP.showLines),:],[symbol("Size");symbol("Session ID");symbol("Timestamp");symbol("URL")]))
    catch y
        println("bigPages4SRFLP Exception ",y)
    end
end

function bigPages5SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Int64)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
            select
                count(*) cnt,
                $localTable.params_dom_sz dom_size,
                $localTable.session_id s_id,
                $localTable."timestamp"
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where
                $localTable.params_dom_sz IS NOT NULL and
                $localTable.params_dom_sz > $(minSizeBytes) and
                $localTable.session_id IS NOT NULL
            group by $localTable.params_dom_sz, $localTable.session_id, $localTable."timestamp"
            order by $localTable.params_dom_sz desc
            limit $(SP.showLines)
        """);

        displayTitle(chart_title = "Big Pages With Timestamp (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(names!(joinTables[1:min(end,SP.showLines),:],[symbol("Page Views");symbol("Size");symbol("Session ID");symbol("TimeStamp")]))

    catch y
        println("bigPages5SRFLP Exception ",y)
    end
end

function bigPages6SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Int64)

    try
        localTable = UP.btView
        table = UP.beaconTable

        joinTables = query("""\
            select
                $localTable.params_dom_sz dom_size,
                $localTable.session_id,
                $localTable."timestamp",
                $tableRt.start_time,
                $tableRt.encoded_size,
                $tableRt.transferred_size,
                $tableRt.decoded_size,
                $tableRt.url urlgroup
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where
                $localTable.params_dom_sz IS NOT NULL and
                $localTable.params_dom_sz > $(minSizeBytes) and
                $localTable.session_id IS NOT NULL
            order by $localTable.params_dom_sz
            limit $(SP.showLines)
        """);

        displayTitle(chart_title = "Big Pages Details (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        scrubUrlToPrint(joinTables;limit=SP.scrubUrlChars)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("bigPages6SRFLP Exception ",y)
    end
end
