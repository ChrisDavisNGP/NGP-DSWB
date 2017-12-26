#
# Functions which return a data frame
#

function allPageUrlTableCreateDF(TV::TimeVars,UP::UrlParams)
    try
        bt = UP.beaconTable
        rt = UP.resourceTable

        if (UP.usePageLoad)
            toppageurl = query("""\
            select
                'None' as urlpagegroup,
                avg($rt.start_time),
                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.start_time) END) as total,
                avg($rt.redirect_end-$rt.redirect_start) as redirect,
                avg(CASE WHEN ($rt.dns_start = 0 and $rt.request_start = 0) THEN (0) WHEN ($rt.dns_start = 0) THEN ($rt.request_start-$rt.fetch_start) ELSE ($rt.dns_start-$rt.fetch_start) END) as blocking,
                avg($rt.dns_end-$rt.dns_start) as dns,
                avg($rt.tcp_connection_end-$rt.tcp_connection_start) as tcp,
                avg($rt.response_first_byte-$rt.request_start) as request,
                avg(CASE WHEN ($rt.response_first_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.response_first_byte) END) as response,
                avg(0) as gap,
                avg(0) as critical,
                CASE WHEN (position('?' in $rt.url) > 0) then trim('/' from (substring($rt.url for position('?' in substring($rt.url from 9)) +7))) else trim('/' from $rt.url) end as urlgroup,
                count(*) as request_count,
                'Label' as label,
                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE (($rt.response_last_byte-$rt.start_time)/1000.0) END) as load,
                avg($bt.timers_t_done) as beacon_time
            FROM $rt join $bt on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
            WHERE
                $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                $bt.session_id IS NOT NULL and
                $bt.page_group ilike '$(UP.pageGroup)' and
                $bt.params_u ilike '$(UP.urlRegEx)' and
                $bt.user_agent_device_type ilike '$(UP.deviceType)' and
                $bt.user_agent_os ilike '$(UP.agentOs)' and
                $bt.timers_t_done >= $(UP.timeLowerMs) and $bt.timers_t_done <= $(UP.timeUpperMs) and
                $bt.params_rt_quit IS NULL
            group by urlgroup,urlpagegroup,label
            """);
        else
            toppageurl = query("""\
            select
                'None' as urlpagegroup,
                avg($rt.start_time),
                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.start_time) END) as total,
                avg($rt.redirect_end-$rt.redirect_start) as redirect,
                avg(CASE WHEN ($rt.dns_start = 0 and $rt.request_start = 0) THEN (0) WHEN ($rt.dns_start = 0) THEN ($rt.request_start-$rt.fetch_start) ELSE ($rt.dns_start-$rt.fetch_start) END) as blocking,
                avg($rt.dns_end-$rt.dns_start) as dns,
                avg($rt.tcp_connection_end-$rt.tcp_connection_start) as tcp,
                avg($rt.response_first_byte-$rt.request_start) as request,
                avg(CASE WHEN ($rt.response_first_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.response_first_byte) END) as response,
                avg(0) as gap,
                avg(0) as critical,
                CASE WHEN (position('?' in $rt.url) > 0) then trim('/' from (substring($rt.url for position('?' in substring($rt.url from 9)) +7))) else trim('/' from $rt.url) end as urlgroup,
                count(*) as request_count,
                'Label' as label,
                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE (($rt.response_last_byte-$rt.start_time)/1000.0) END) as load,
                avg($bt.timers_domready) as beacon_time
            FROM $rt join $bt on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
                where
                $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                $bt.session_id IS NOT NULL and
                $bt.page_group ilike '$(UP.pageGroup)' and
                $bt.params_u ilike '$(UP.urlRegEx)' and
                $bt.user_agent_device_type ilike '$(UP.deviceType)' and
                $bt.user_agent_os ilike '$(UP.agentOs)' and
                $bt.timers_domready >= $(UP.timeLowerMs) and $bt.timers_domready <= $(UP.timeUpperMs) and
                $bt.params_rt_quit IS NULL
            group by urlgroup,urlpagegroup,label
            """);
        end

        return toppageurl
    catch y
        println("allPageUrlTableCreateDF Exception ",y)
    end
end

function allSessionUrlTableCreateDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString)

    rt = UP.resourceTable

    try
        toppageurl = query("""\
        select
            'None' as urlpagegroup,
            avg(start_time),
            avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as total,
            avg(redirect_end-redirect_start) as redirect,
            avg(CASE WHEN (dns_start = 0 and request_start = 0) THEN (0) WHEN (dns_start = 0) THEN (request_start-fetch_start) ELSE (dns_start-fetch_start) END) as blocking,
            avg(dns_end-dns_start) as dns,
            avg(tcp_connection_end-tcp_connection_start) as tcp,
            avg(response_first_byte-request_start) as request,
            avg(CASE WHEN (response_first_byte = 0) THEN (0) ELSE (response_last_byte-response_first_byte) END) as response,
            avg(0) as gap,
            avg(0) as critical,
            CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
            count(*) as request_count,
            'Label' as label,
            avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE ((response_last_byte-start_time)/1000.0) END) as load,
            0 as beacon_time
        FROM $(rt)
        where
            session_id = '$(studySession)' and
            $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
        group by urlgroup,urlpagegroup,label
        """);

        return toppageurl
    catch y
        println("allSessionUrlTableCreateDF Exception ",y)
    end
end

function sessionUrlTableCreateDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)

    rt = UP.resourceTable
    try
        toppageurl = query("""\
        select
            'None' as urlpagegroup,
            start_time,
            CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END as total,
            (redirect_end-redirect_start) as redirect,
            CASE WHEN (dns_start = 0 and request_start = 0) THEN (0) WHEN (dns_start = 0) THEN (request_start-fetch_start) ELSE (dns_start-fetch_start) END as blocking,
            (dns_end-dns_start) as dns,
            (tcp_connection_end-tcp_connection_start) as tcp,
            (response_first_byte-request_start) as request,
            CASE WHEN (response_first_byte = 0) THEN (0) ELSE (response_last_byte-response_first_byte) END as response,
            0 as gap,
            0 as critical,
            CASE when  (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
            1 as request_count,
            'Label' as label,
            CASE WHEN (response_last_byte = 0) THEN (0) ELSE ((response_last_byte-start_time)/1000.0) END as load,
            0 as beacon_time
        FROM $(rt)
        where
            session_id = '$(studySession)' and
            "timestamp" = '$(studyTime)'
        order by start_time asc
        """);

        return toppageurl
    catch y
        println("sessionUrlTableCreateDF Exception ",y)
    end
end

function getResourcesForBeaconCreateDF(TV::TimeVars,UP::UrlParams)

    bt = UP.beaconTable
    rt = UP.resourceTable

    try

        localTableRtDF = query("""\
            select $rt.*
            FROM $bt join $rt
            on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
            where
            $bt.params_u ilike '$(UP.urlRegEx)'
            and $bt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            and $bt.session_id IS NOT NULL
            and $bt.page_group ilike '$(UP.pageGroup)'
            and $bt.timers_t_done >= $(UP.timeLowerMs) and $bt.timers_t_done < $(UP.timeUpperMs)
            and $bt.params_rt_quit IS NULL
            and $bt.user_agent_device_type ilike '$(UP.deviceType)'
            and $bt.user_agent_os ilike '$(UP.agentOs)'
            order by $rt.session_id, $rt."timestamp", $rt.start_time
            """)



        return localTableRtDF
    catch y
        println("urlDetailRtTables Exception ",y)
    end
end

function statsTableCreateDF(bt::ASCIIString,pageGroup::ASCIIString,startTimeMs::Int64, endTimeMs::Int64)
    try
        localStats = query("""\
        select timers_t_done
        FROM $bt where
        page_group ilike '$(pageGroup)' and
        "timestamp" between $startTimeMs and $endTimeMs and
        params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableCreateDF Exception ",y)
    end
end

function treemapsLocalTableRtCreateDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable
    rt = UP.resourceTable

    try
        localTableRtDF = query("""\
            select $rt.*
            FROM $bt join $rt
                on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
            where
                $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                $bt.session_id IS NOT NULL and
                $bt.page_group ilike '$(UP.pageGroup)' and
                $bt.params_u ilike '$(UP.urlRegEx)' and
                $bt.timers_t_done >= $(UP.timeLowerMs) and $bt.timers_t_done < $(UP.timeUpperMs) and
                $bt.user_agent_device_type ilike '$(UP.deviceType)' and
                $bt.user_agent_os ilike '$(UP.agentOs)' and
                $bt.params_rt_quit IS NULL
            order by $rt.session_id, $rt."timestamp", $rt.start_time
        """)
        return localTableRtDF
    catch y
        println("treemapsLocalTableRtCreateDF Exception ",y)
    end
end
#
#  Functions which create views
#

function test1GNGSSDM(UP::UrlParams,SP::ShowParams)

    try

        test1Table = query("""\
            select URL, count(*)
                FROM $(UP.btView)
                GROUP BY url
                Order by count(*) desc
        """)

        beautifyDF(test1Table[1:min(SP.showLines,end),:])
    catch y
        println("test1GNGSSDM Exception ",y)
    end
end

function testUserAgentGNGSSDM(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = query("""\
            select count(*),user_agent_raw
            FROM $(UP.btView)
            where
                beacon_type = 'page view'
            group by user_agent_raw
            order by count(*) desc
        limit $(SP.showLines)
    """)

        beautifyDF(CleanupTable[1:min(SP.showLines,end),:])

    catch y
        println("testUserAgentGNGSSDM Exception ",y)
    end
end

function test2GNGSSDM(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = query("""\
            select count(*), URL, params_u
            FROM $(UP.btView)
            where
                beacon_type = 'page view'
            GROUP BY url,params_u
            Order by count(*) desc
    """)

        beautifyDF(CleanupTable[1:min(SP.showLines,end),:])

    catch y
        println("test2GNGSSDM Exception ",y)
    end
end

function test3GNGSSDM(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = query("""\
            select
                count(*) as "Page Views",
                params_u as "URL Landing In Nat Geo Site Default Group"
            FROM $(UP.btView)
            where
                beacon_type = 'page view' and
                params_u <> 'http://www.nationalgeographic.com/' and
                params_u like 'http://www.nationalgeographic.com/?%'
            GROUP BY params_u
            Order by count(*) desc
        """)

        beautifyDF(CleanupTable[1:min(SP.showLines,end),:])

    catch y
        println("test3GNGSSDM Exception ",y)
    end
end

function gatherSizeData(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        bt = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
        select
            CASE WHEN (position('?' in $bt.params_u) > 0) then trim('/' from (substring($bt.params_u for position('?' in substring($bt.params_u from 9)) +7))) else trim('/' from $bt.params_u) end as urlgroup,
            $bt.session_id,
            $bt."timestamp",
            sum($rt.encoded_size) as encoded,
            sum($rt.transferred_size) as transferred,
            sum($rt.decoded_size) as decoded,
            count(*)
        FROM $bt join $rt on $bt.session_id = $rt.session_id and $bt."timestamp" = $rt."timestamp"
            where $rt.encoded_size > 1
            group by urlgroup,$bt.session_id,$bt."timestamp"
            order by encoded desc
        """);

        scrubUrlToPrint(SP,joinTablesDF,:urlgroup)
        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])

        return joinTablesDF
    catch y
        println("gatherSizeData Exception ",y)
    end
end

function joinTablesDetailsPrint(TV::TimeVars,UP::UrlParams,SP::ShowParams,joinTableSummary::DataFrame,row::Int64)
    try
        btv = UP.btView
        rt = UP.resourceTable

        topSessionId = joinTableSummary[row:row,:session_id][1]
        topTimeStamp = joinTableSummary[row:row,:timestamp][1]
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        joinTablesDetails = query("""\
            select
                $rt.start_time,
                $rt.encoded_size,
                $rt.transferred_size,
                $rt.decoded_size,
                $rt.url as urlgroup
            FROM $btv join $rt
                on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
            where
                $btv.session_id = '$(topSessionId)' and
                $btv."timestamp" = $(topTimeStamp) and
                $rt.encoded_size > 1000000 and
                $rt.url not like '%/interactive-assets/%'
            order by $rt.start_time
        """);

        recordsFound = nrow(joinTablesDetails)
        if (recordsFound > 0)
            displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDetails,:urlgroup)
            beautifyDF(joinTablesDetails[1:end,:])
        end
    catch y
        println("joinTablesDetailsPrint Exception ",y)
    end
end

function statsTableDF2(TV::TimeVars,UP::UrlParams)
    try
        btv = UP.btView

        localStats = query("""\
            select timers_t_done
            from $btv
            where
                page_group ilike '$(UP.pageGroup)' and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                user_agent_os ilike '$(UP.agentOs)' and
                "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableDF2 Exception ",y)
    end
end

function topPageViewsUDB(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView

        topurl = query("""\
            select
                count(*),
                CASE when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) else trim('/' from params_u) end urlgroup
            FROM $(btv)
            where
                beacon_type = 'page view'
            group by urlgroup
            order by count(*) desc
            limit $(SP.showLines)
        """);

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function topUrlPageViewsUDB(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView

        topurl = query("""\
            select count(*),params_u
            FROM $(btv)
            where
                beacon_type = 'page view'
            group by params_u
            order by count(*) desc
            limit $(SP.showLines)
        """)

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString,"URL: $(UP.urlRegEx)"])
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function bigPages1SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView

        statsDF = DataFrame()

        localDF = query("""SELECT params_dom_sz FROM $btv""")
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

function bigPages2SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        displayTitle(chart_title = "Big Pages (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesDF = query("""\
            select
                params_dom_sz,
                timers_t_page load_time,
                params_u urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            order by params_dom_sz desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(SP,bigPagesDF,:urlgroup)
        beautifyDF(names!(bigPagesDF[1:min(SP.showLines,end),:],[symbol("Size");symbol("Load Time (ms)");symbol("URL")]))
    catch y
        println("bigPages2SRFLP Exception ",y)
    end
end

function bigPages3SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView

        displayTitle(chart_title = "Big Pages By Average Size (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigAveragePagesDF = query("""\
            select
                count(*),
                avg(params_dom_sz) as size,
                avg(timers_t_page) as load,
                params_u as urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            group by urlgroup
            order by size desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(SP,bigAveragePagesDF,:urlgroup)
        beautifyDF(names!(bigAveragePagesDF[1:min(SP.showLines,end),:],[symbol("Count");symbol("Size");symbol("Load Time (ms)");symbol("URL")]))
    catch y
        println("bigPages3SRFLP Exception ",y)
    end
end

function bigPages4SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView

        displayTitle(chart_title = "Big Pages With Session ID (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesSessionsDF = query("""\
            select
                params_dom_sz dom_size,
                session_id,
                "timestamp",
                params_u urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes) and
                session_id IS NOT NULL
            order by dom_size desc
            limit $(SP.showLines)
        """);

        scrubUrlToPrint(SP,bigPagesSessionsDF,:urlgroup)
        beautifyDF(names!(bigPagesSessionsDF[1:min(end,SP.showLines),:],[symbol("Size");symbol("Session ID");symbol("Timestamp");symbol("URL")]))
    catch y
        println("bigPages4SRFLP Exception ",y)
    end
end

function bigPages5SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select
                count(*) cnt,
                $btv.params_dom_sz dom_size,
                $btv.session_id s_id,
                $btv."timestamp"
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
            where
                $btv.params_dom_sz IS NOT NULL and
                $btv.params_dom_sz > $(minSizeBytes) and
                $btv.session_id IS NOT NULL
            group by $btv.params_dom_sz, $btv.session_id, $btv."timestamp"
            order by $btv.params_dom_sz desc
            limit $(SP.showLines)
        """);

        displayTitle(chart_title = "Big Pages With Timestamp (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(names!(joinTablesDF[1:min(end,SP.showLines),:],[symbol("Page Views");symbol("Size");symbol("Session ID");symbol("TimeStamp")]))

    catch y
        println("bigPages5SRFLP Exception ",y)
    end
end

function bigPages6SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select
                $btv.params_dom_sz dom_size,
                $btv.session_id,
                $btv."timestamp",
                $rt.start_time,
                $rt.encoded_size,
                $rt.transferred_size,
                $rt.decoded_size,
                $rt.url urlgroup
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
            where
                $btv.params_dom_sz IS NOT NULL and
                $btv.params_dom_sz > $(minSizeBytes) and
                $btv.session_id IS NOT NULL
            order by $btv.params_dom_sz
            limit $(SP.showLines)
        """);

        displayTitle(chart_title = "Big Pages Details (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        scrubUrlToPrint(SP,joinTablesDF,:urlgroup)
        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("bigPages6SRFLP Exception ",y)
    end
end

function bigPageSizeDetails(TV,UP,SP,fileType::ASCIIString;minEncoded::Int64=1000)

    # Create the summary table

    btv = UP.btView
    rt = UP.resourceTable

    try
        joinTablesDF = query("""\
        select
            avg($rt.encoded_size) as encoded,
            avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            $btv.user_agent_os,
            $btv.user_agent_family,
            count(*)
        from $btv join $rt
            on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
        where $rt.encoded_size > $(minEncoded) and
            $rt.url not like '%/interactive-assets/%' and
           ($rt.url ilike '$(fileType)' or $rt.url ilike '$(fileType)?%') and
            $btv.user_agent_device_type ilike '$(UP.deviceType)' and
            $btv.user_agent_os ilike '$(UP.agentOs)'
        group by
            $btv.user_agent_family,
            $btv.user_agent_os
        order by encoded desc, transferred desc, decoded desc
        """);

        displayTitle(chart_title = "$(UP.deviceType) Big Pages Details (Min $(minEncoded) Bytes), File Type $(fileType)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("bigPageSizeDetails 1 Exception ",y)
    end

    # Create the details table

    try

        joinTablesDF = query("""\
        select
            avg($rt.encoded_size) as encoded,
            avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            count(*),
            CASE WHEN (position('?' in $btv.params_u) > 0) then trim('/' from (substring($btv.params_u for position('?' in substring($btv.params_u from 9)) +7))) else trim('/' from $btv.params_u) end as urlgroup,
            $rt.url
        from $btv join $rt
            on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
        where $rt.encoded_size > $(minEncoded) and
            $rt.url not like '%/interactive-assets/%' and
            ($rt.url ilike '$(fileType)' or $rt.url ilike '$(fileType)?%') and
            $btv.user_agent_device_type ilike '$(UP.deviceType)' and
            $btv.user_agent_os ilike '$(UP.agentOs)'
        group by
            $btv.params_u,$rt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("bigPageSizeDetails 2 Exception ",y)
    end

end

function lookForLeftOversALR(UP::UrlParams,linesOutput::Int64)

    joinTablesDF = DataFrame()

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
        select
            $btv.user_agent_os,
            $btv.user_agent_family,
            $btv.user_agent_device_type,
            $rt.url,
            avg($rt.encoded_size) as encoded,
            avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            count(*)
        from $btv join $rt
        on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
        where $rt.encoded_size > 1 and
        $rt.url not ilike '%/interactive-assets/%' and
        $rt.url not ilike '%png' and
        $rt.url not ilike '%svg' and
        $rt.url not ilike '%jpg' and
        $rt.url not ilike '%mp3' and
        $rt.url not ilike '%mp4' and
        $rt.url not ilike '%gif' and
        $rt.url not ilike '%wav' and
        $rt.url not ilike '%jog' and
        $rt.url not ilike '%js' and
        $rt.url not ilike '%.js?%' and
        $rt.url not ilike '%css' and
        $rt.url not ilike '%ttf' and
        $rt.url not ilike '%woff%'
        group by
            $btv.user_agent_family,
            $btv.user_agent_os,
            $btv.user_agent_device_type,
            $rt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTablesDF[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversALR Exception ",y)
    end
    #display(joinTablesDF)
end

function lookForLeftOversDetailsALR(UP::UrlParams,linesOutput::Int64)

    joinTablesDF = DataFrame()

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select
                $rt.url,
                avg($rt.encoded_size) as encoded,
                avg($rt.transferred_size) as transferred,
                avg($rt.decoded_size) as decoded,
                $btv.compression_types,$btv.domain,$btv.geo_netspeed,$btv.mobile_connection_type,$btv.params_scr_bpp,$btv.params_scr_dpx,$btv.params_scr_mtp,$btv.params_scr_orn,params_scr_xy,
                $btv.user_agent_family,$btv.user_agent_major,$btv.user_agent_minor,$btv.user_agent_mobile,$btv.user_agent_model,$btv.user_agent_os,$btv.user_agent_osversion,$btv.user_agent_raw,
                $btv.user_agent_manufacturer,$btv.user_agent_device_type,$btv.user_agent_isp,$btv.geo_isp,$btv.params_ua_plt,$btv.params_ua_vnd,
                $rt.initiator_type,$rt.height,$rt.width,$rt.x,$rt.y,
                count(*)
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
            where $rt.encoded_size > 1 and $rt.url not like '%/interactive-assets/%'
            group by $rt.url,
                $btv.compression_types,$btv.domain,$btv.geo_netspeed,$btv.mobile_connection_type,$btv.params_scr_bpp,$btv.params_scr_dpx,$btv.params_scr_mtp,$btv.params_scr_orn,params_scr_xy,
                $btv.user_agent_family,$btv.user_agent_major,$btv.user_agent_minor,$btv.user_agent_mobile,$btv.user_agent_model,$btv.user_agent_os,$btv.user_agent_osversion,$btv.user_agent_raw,
                $btv.user_agent_manufacturer,$btv.user_agent_device_type,$btv.user_agent_isp,$btv.geo_isp,$btv.params_ua_plt,$btv.params_ua_vnd,
                $rt.initiator_type,$rt.height,$rt.width,$rt.x,$rt.y
            order by encoded desc
        """);

        beautifyDF(joinTablesDF[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversDetailsALR Exception ",y)
    end
end

function requestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    rc = query("""\

        select
            count(*) reqcnt, substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        group by urlgroup
        order by reqcnt desc
        LIMIT 15
    """)

    linesOut = 15
    displayTitle(chart_title = "$(typeStr): Request Counts By URL Group", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(rc[1:min(linesOut,end),:])

end

function blockingRequestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    br = query("""\

        select
            count(*) reqcnt, sum(request_start-start_time) totalblk, (sum(request_start-start_time)/count(*)) avgblk,substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (request_start-start_time) > 0
        group by urlgroup
        order by totalblk desc
         LIMIT 30
    """)

    displayTitle(chart_title = "$(typeStr): Blocking Requests By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    linesOut = 30
    beautifyDF(br[1:min(linesOut,end),:])
end

function nonCacheRequestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    nc = query("""\
        select
            count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) > 0
        group by urlgroup
        order by count(*) desc
        LIMIT 15
    """)

    displayTitle(chart_title = "$(typeStr): Non Cache Requests Total By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    linesOut = 15
    beautifyDF(nc[1:min(linesOut,end),:])
end

function cacheHitRatioSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    cached = query("""\
        select
            count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) = 0
        group by urlgroup
        order by count(*) desc
        LIMIT 250
    """)

    ratio = query("""\
        select
            substring(url for position('/' in substring(url from 9)) +7) urlgroup, count(*) notCachedCount, 0 cachedCount, 0.0 ratio
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) > 0
        group by urlgroup
        order by count(*) desc
        LIMIT 250
    """)

    for x in eachrow(ratio)
        cnt = cached[Bool[isequal(x[:urlgroup],y) for y in cached[:urlgroup]],:count]
        if isempty(cnt)
            cnt = [1]
        end
        x[:cachedcount] = cnt[1]
        x[:ratio] = (cnt[1] / (x[:notcachedcount] + cnt[1])) * 100.0
    end

    displayTitle(chart_title = "$(typeStr): Cache Hit Ratio By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(names!(ratio[1:min(30, end),[1:4;]],[symbol("Url Group"), symbol("Not Cached Cnt"), symbol("Cached Cnt"), symbol("Cached Ratio")]))
end

function resourceImages(TV::TimeVars,UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
        select
            avg($rt.encoded_size) as encoded,
            avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            count(*),
            $rt.url
        from $btv join $rt
            on $btv.session_id = $rt.session_id and $btv."timestamp" = $rt."timestamp"
        where $rt.encoded_size > $(UP.sizeMin) and
            ($rt.url ilike '$(fileType)' or $rt.url ilike '$(fileType)?%') and
            $rt.url ilike 'http://www.nationalgeographic.com%'
        group by $rt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        if (SP.debugLevel > 4)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
        end

        return joinTablesDF
    catch y
        println("resourceImage Exception ",y)
    end
end

# Select * from beaconTable into data frame

function defaultBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    if (SP.debugLevel > 4)
        println("Time MS UTC: $(TV.startTimeMsUTC),$(TV.endTimeMsUTC)")
        println("urlRegEx $(UP.urlRegEx)")
        println("dev=$(UP.deviceType), os=$(UP.agentOs), page grp=$(UP.pageGroup)")
        println("time Range: $(UP.timeLowerMs),$(UP.timeUpperMs)")
    end

    try
        localTableDF = query("""\
            select * from $bt
            where
            "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            session_id IS NOT NULL and
            params_rt_quit IS NULL and
            params_u ilike '$(UP.urlRegEx)' and
            user_agent_device_type ilike '$(UP.deviceType)' and
            user_agent_os ilike '$(UP.agentOs)' and
            page_group ilike '$(UP.pageGroup)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
        """)

        if (SP.debugLevel > 8)
            standardChartTitle(TV,UP,SP,"Debug8: defaultBeaconsToDF All Columns")
            beautifyDF(localTableDF[1:min(3,end),:])
        end

        return localTableDF
    catch y
        println("defaultBeaconsToDF Exception ",y)
    end
end

function displayMatchingResourcesByParentUrl(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select count(*), params_u as parenturl
            from $rt
            where
                params_u ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by parenturl
            order by count(*) desc
            limit $(UP.limitRows)
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Any Parent Url (params_u) for pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Any Parent Url (params_u) for pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByParentUrl Exception ",y)
    end
end

function displayMatchingResourcesByUrl(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select count(*), url
            from $rt
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url
            order by count(*) desc
            limit $(UP.limitRows)
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Any resource Url for pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Any resource Url for pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByUrl Exception ",y)
    end
end

function displayMatchingResourcesByUrls(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btw = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select count(*), $rt.params_u as parenturl, $rt.url
            from $btw join $rt
                on $btw.session_id = $rt.session_id and $btw."timestamp" = $rt."timestamp"
            where
            $rt.url ilike '$(UP.resRegEx)'
            group by $rt.params_u, $rt.url, $btw.url
            order by count(*) desc
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Beacon table and resources joined with resource Url pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Beacon table and resources joined with resource Url pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByUrls Exception ",y)
    end
end

function displayMatchingResourcesAllFields(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select *
            from $rt
            where
              url ilike '$(UP.resRegEx)' and
              "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            limit 3
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = " All fields for resources with Url pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
        else
            displayTitle(chart_title = "All fields for resources with Url pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesAllFields Exception ",y)
    end
end

function displayMatchingResourcesStats(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = query("""\
            select
                count(*),
                avg(start_time) as "start",
                avg(fetch_start) as "fetch",
                avg(dns_end-dns_start) as "dnstimems",
                avg(tcp_connection_end-tcp_connection_start) as "tcptimems",
                avg(request_start) as "request",
                avg(response_first_byte) as "responsefirstbyte",
                avg(response_last_byte) as "responselastbyte",
                max(response_last_byte) as "maxresponselastbyte",
                params_u as parenturl, url,
                avg(redirect_end - redirect_start) as "redirecttimems",
                avg(secure_connection_start) as "secureconnection"
            from $rt
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by parenturl,url
            order by count(*) desc
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Average Times for Url pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:count,"on column count")
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:responselastbyte,"on Average Response Last Byte")
            dataframeFieldStats(TV,UP,SP,joinTablesDF,:maxresponselastbyte,"on Maximum Response Last Byte")
        else
            displayTitle(chart_title = "Average Times for Url pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesStats Exception ",y)
    end
end

function topUrlTableByCount(TV::TimeVars,UP::UrlParams,SP::ShowParams; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
        try

            ltName = UP.btView

            if (SP.debugLevel > 4)
                dbgtopurl = query("""\

                select *
                FROM $(ltName)
                where
                    beacon_type = 'page view'
                    limit 10
                """);

                println(nrow(dbgtopurl))
                beautifyDF(dbgtopurl)
            end

            topurl = query("""\

            select count(*),
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                    end urlgroup
                FROM $(ltName)
                where
                    beacon_type = 'page view'
                    group by urlgroup
                    order by count(*) desc
                    limit $(rowLimit)
            """);

            #println(nrow(topurl))
            #beautifyDF(topurl)

            if (nrow(topurl) == 0)
                displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) URLs for $(UP.pageGroup) - No Page Views", showTimeStamp=false)
                return
            else
                displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) URLs for $(UP.pageGroup)", chart_info = ["Note: If you see AEM URL's in this list tell Chris Davis",TV.timeString],showTimeStamp=false)
            end

            scrubUrlToPrint(SP,topurl,:urlgroup)
            #println(nrow(topurl))

            newDF = topurl[Bool[x > beaconsLimit for x in topurl[:count]],:]
            printDF = names!(newDF[:,:],[symbol("Views"),symbol("Url - $(UP.pageGroup)")])

            #beautifyDF(printDF)

            if (paginate)
                paginatePrintDf(printDF)
            else
                beautifyDF(printDF[:,:])
            end

    catch y
        println("topUrlbtByCount Exception ",y)
    end

end
