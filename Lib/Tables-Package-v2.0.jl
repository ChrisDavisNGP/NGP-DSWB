#
# Functions which return a data frame
#

function allPageUrlTableDF(TV::TimeVars,UP::UrlParams)
    try
        table = UP.beaconTable
        tableRt = UP.resourceTable

        if (UP.usePageLoad)
            toppageurl = query("""\
            select
                'None' as urlpagegroup,
                avg($tableRt.start_time),
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.start_time) END) as total,
                avg($tableRt.redirect_end-$tableRt.redirect_start) as redirect,
                avg(CASE WHEN ($tableRt.dns_start = 0 and $tableRt.request_start = 0) THEN (0) WHEN ($tableRt.dns_start = 0) THEN ($tableRt.request_start-$tableRt.fetch_start) ELSE ($tableRt.dns_start-$tableRt.fetch_start) END) as blocking,
                avg($tableRt.dns_end-$tableRt.dns_start) as dns,
                avg($tableRt.tcp_connection_end-$tableRt.tcp_connection_start) as tcp,
                avg($tableRt.response_first_byte-$tableRt.request_start) as request,
                avg(CASE WHEN ($tableRt.response_first_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.response_first_byte) END) as response,
                avg(0) as gap,
                avg(0) as critical,
                CASE WHEN (position('?' in $tableRt.url) > 0) then trim('/' from (substring($tableRt.url for position('?' in substring($tableRt.url from 9)) +7))) else trim('/' from $tableRt.url) end as urlgroup,
                count(*) as request_count,
                'Label' as label,
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE (($tableRt.response_last_byte-$tableRt.start_time)/1000.0) END) as load,
                avg($table.timers_t_done) as beacon_time
            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
            WHERE
                $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
                and $table.session_id IS NOT NULL
                and $table.page_group ilike '$(UP.pageGroup)'
                and $table.params_u ilike '$(UP.urlRegEx)'
                and $table.user_agent_device_type ilike '$(UP.deviceType)'
                and $table.timers_t_done >= $(UP.timeLowerMs) and $table.timers_t_done <= $(UP.timeUpperMs)
                and $table.params_rt_quit IS NULL
                group by urlgroup,urlpagegroup,label
                """);
        else
            toppageurl = query("""\
            select
                'None' as urlpagegroup,
                avg($tableRt.start_time),
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.start_time) END) as total,
                avg($tableRt.redirect_end-$tableRt.redirect_start) as redirect,
                avg(CASE WHEN ($tableRt.dns_start = 0 and $tableRt.request_start = 0) THEN (0) WHEN ($tableRt.dns_start = 0) THEN ($tableRt.request_start-$tableRt.fetch_start) ELSE ($tableRt.dns_start-$tableRt.fetch_start) END) as blocking,
                avg($tableRt.dns_end-$tableRt.dns_start) as dns,
                avg($tableRt.tcp_connection_end-$tableRt.tcp_connection_start) as tcp,
                avg($tableRt.response_first_byte-$tableRt.request_start) as request,
                avg(CASE WHEN ($tableRt.response_first_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.response_first_byte) END) as response,
                avg(0) as gap,
                avg(0) as critical,
                CASE WHEN (position('?' in $tableRt.url) > 0) then trim('/' from (substring($tableRt.url for position('?' in substring($tableRt.url from 9)) +7))) else trim('/' from $tableRt.url) end as urlgroup,
                count(*) as request_count,
                'Label' as label,
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE (($tableRt.response_last_byte-$tableRt.start_time)/1000.0) END) as load,
                avg($table.timers_domready) as beacon_time
            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
                where
                $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
                and $table.session_id IS NOT NULL
                and $table.page_group ilike '$(UP.pageGroup)'
                and $table.params_u ilike '$(UP.urlRegEx)'
                and $table.user_agent_device_type ilike '$(UP.deviceType)'
                and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
                and $table.params_rt_quit IS NULL
                group by urlgroup,urlpagegroup,label
                """);
        end

        return toppageurl
    catch y
        println("allPageUrlTableDF Exception ",y)
    end
end

function allSessionUrlTableDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString)

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
            $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
        group by urlgroup,urlpagegroup,label
        """);

        return toppageurl
    catch y
        println("allSessionUrlTableDF Exception ",y)
    end
end

function sessionUrlTableDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)

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
        println("sessionUrlTableDF Exception ",y)
    end
end

#function estimateBeacons(table::ASCIIString, startTimeMs::Int64, endTimeMs::Int64;
#    pageGroup::ASCIIString="%", localUrl::ASCIIString="%", deviceType::ASCIIString="%", rangeLowerMs::Float64=1000.0, rangeUpperMs::Float64=600000.0
#    )
function estimateBeacons(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        localTableDF = query("""\
            select * from $(UP.beaconTable)
            where
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            session_id IS NOT NULL and
            params_rt_quit IS NULL and
            params_u ilike '$(UP.urlRegEx)' and
            user_agent_device_type ilike '$(UP.deviceType)' and
            page_group ilike '$(UP.pageGroup)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
        """)
        return localTableDF
    catch y
        println("urlDetailTables Exception ",y)
    end
end


#function getResourcesForBeacon(table::ASCIIString, tableRt::ASCIIString,
#    pageGroup::ASCIIString="%", localUrl::ASCIIString="%", deviceType::ASCIIString="%", rangeLowerMs::Float64=1000.0, rangeUpperMs::Float64=600000.0
#    )
function getResourcesForBeacon(TV::TimeVars,UP::UrlParams)

    try

        localTableRtDF = query("""\
            select $(UP.resourceTable).* from $(UP.beaconTable) join $(UP.resourceTable)
            on $(UP.resourceTable).session_id = $(UP.beaconTable).session_id and $(UP.resourceTable)."timestamp" = $(UP.beaconTable)."timestamp"
            where
            $(UP.beaconTable).params_u ilike '$(UP.urlRegEx)'
            and $(UP.resourceTable)."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            and $(UP.beaconTable).session_id IS NOT NULL
            and $(UP.beaconTable).page_group = '$(UP.pageGroup)'
            and $(UP.beaconTable).timers_t_done >= $(UP.timeLowerMs) and $(UP.beaconTable).timers_t_done < $(UP.timeUpperMs)
            and $(UP.beaconTable).params_rt_quit IS NULL
            and $(UP.beaconTable).user_agent_device_type ilike '$(UP.deviceType)'
            order by $(UP.resourceTable).session_id, $(UP.resourceTable)."timestamp", $(UP.resourceTable).start_time
            """)



        return localTableRtDF
    catch y
        println("urlDetailRtTables Exception ",y)
    end
end

function statsTableDF(table::ASCIIString,pageGroup::ASCIIString,startTimeMs::Int64, endTimeMs::Int64)
    try
        localStats = query("""\
        select timers_t_done from $table where
        page_group = '$(pageGroup)' and
        "timestamp" between $startTimeMs and $endTimeMs and
        params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableDF Exception ",y)
    end
end

function treemapsLocalTableDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    try
        localTableDF = query("""\
        select *
        from $bt
        where
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            session_id IS NOT NULL and
            page_group ilike '$(UP.pageGroup)' and
            params_u ilike '$(UP.urlRegEx)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs) and
            user_agent_device_type ilike '$(UP.deviceType)' and
            user_agent_os ilike '$(UP.agentOs)' and
            params_rt_quit IS NULL
        """)
        return localTableDF
    catch y
        println("treemapsLocalTableDF Exception ",y)
    end
end

function treemapsLocalTableRtDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable
    rt = UP.resourceTable

    try
        localTableRtDF = query("""\
            select $rt.*
            from $bt join $rt
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
        println("treemapsLocalTableRtDF Exception ",y)
    end
end
#
#  Functions which create views
#

function limitedTable(TV::TimeVars,UP::UrlParams)
    try
        query("""\
            drop view if exists $(UP.btView)
        """)

        query("""\
            create or replace view $(UP.btView) as
            (select * from $(UP.beaconTable) where
            page_group = '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs) and
            params_rt_quit IS NULL
        )""")
    catch y
        println("limitedTable Exception ",y)
    end
end

function pageGroupDetailsTables(TV::TimeVars,UP::UrlParams,localMobileTable::ASCIIString,localDesktopTable::ASCIIString)
      try

        query("""\
            drop view if exists $(UP.btView)
        """)

        query("""\
            create or replace view $(UP.btView) as
            (select * from $(UP.beaconTable)
            where page_group = '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
            )
        """)

        query("""\
            create or replace view $localMobileTable as
            (select * from $(UP.beaconTable)
            where page_group = '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            user_agent_device_type = 'Mobile'
            )
        """)

        query("""\
            create or replace view $localDesktopTable as
            (select * from $(UP.beaconTable)
            where page_group = '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            user_agent_device_type = 'Desktop'
            )
        """);

    catch y
        println("pageGroupDetailTables Exception ",y)
    end
end

function urlBeaconTable(localTable::ASCIIString,table::ASCIIString,productPageGroup::ASCIIString,startTimeMs::Int64, endTimeMs::Int64,params_u::ASCIIString)
    try
        query("""\
            drop view if exists $localTable
        """)

    query("""\
        create or replace view $localTable as
        (select * from $table
        where page_group = '$(productPageGroup)' and
        "timestamp" between $startTimeMs and $endTimeMs and
        params_u ilike '$(localUrl)'
        )
    """)
    catch y
        println("urlDetailTables Exception ",y)
    end
end

function urlResourceTable(localTableRt::ASCIIString,tableRt::ASCIIString,localTable::ASCIIString,productPageGroup::ASCIIString,startTimeMs::Int64, endTimeMs::Int64)
    try
        query("""\
            drop view if exists $localTableRt
        """)

        query("""\
            create or replace view $localTableRt as (
            select $tableRt.* from $localTable join $tableRt on $tableRt.session_id = $localTable.session_id
            where $tableRt."timestamp" between $startTimeMs and $endTimeMs and $localTable.session_id IS NOT NULL
            order by $tableRt.session_id, $tableRt."timestamp", $tableRt.start_time
            )
        """)
    catch y
        println("urlResourceTables Exception ",y)
    end
end

function groupSamplesTableDF(table::ASCIIString,productPageGroup::ASCIIString)
    try

        samplesDF = query("""\
                select * from $table where page_group ilike '$(productPageGroup)' and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and beacon_type = 'page view'
        """);

        return samplesDF
    catch y
        println("groupSamplesTableDF Exception",y)
    end
end

function defaultBeaconView(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        table = UP.beaconTable
        localTable = UP.btView
        timeLowerMs = UP.timeLowerMs > 0 ? UP.timeLowerMs : 1000
        timeUpperMs = UP.timeUpperMs > 0 ? UP.timeUpperMs : 600000
        if (SP.debugLevel > 0)
            println("Low=",timeLowerMs," High=", timeUpperMs)
        end

        query("""\
            create or replace view $localTable as (
                select * from $table
                    where
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group ilike '$(UP.pageGroup)' and
                        params_u ilike '$(UP.urlRegEx)' and
                        user_agent_device_type ilike '$(UP.deviceType)' and
                        $table.user_agent_os ilike '$(UP.agentOs)' and
                        timers_t_done >= $timeLowerMs and timers_t_done < $timeUpperMs
            )
        """)
        if (SP.debugLevel > 0)
            cnt = query("""SELECT count(*) FROM $localTable""")
            println("$localTable count is ",cnt[1,1])
        end
    catch y
        println("defaultBeaconView Exception ",y)
    end
end

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
            select
                count(*),user_agent_raw
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
            select
                count(*), URL, params_u
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

        joinTables = query("""\
        select
            CASE WHEN (position('?' in $bt.params_u) > 0) then trim('/' from (substring($bt.params_u for position('?' in substring($bt.params_u from 9)) +7))) else trim('/' from $bt.params_u) end as urlgroup,
            $bt.session_id,
            $bt."timestamp",
            sum($rt.encoded_size) as encoded,
            sum($rt.transferred_size) as transferred,
            sum($rt.decoded_size) as decoded,
            count(*)
        from $bt join $rt on $bt.session_id = $rt.session_id and $bt."timestamp" = $rt."timestamp"
            where $rt.encoded_size > 1
            group by urlgroup,$bt.session_id,$bt."timestamp"
            order by encoded desc
        """);

        scrubUrlToPrint(joinTables;limit=SP.scrubUrlChars)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])

        return joinTables
    catch y
        println("gatherSizeData Exception ",y)
    end
end

function joinTablesDetailsPrint(TV::TimeVars,UP::UrlParams,SP::ShowParams,joinTableSummary::DataFrame,row::Int64)
    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        topSessionId = joinTableSummary[row:row,:session_id][1]
        topTimeStamp = joinTableSummary[row:row,:timestamp][1]
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        joinTablesDetails = query("""\
            select
                $tableRt.start_time,
                $tableRt.encoded_size,
                $tableRt.transferred_size,
                $tableRt.decoded_size,
                $tableRt.url as urlgroup
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where
                $localTable.session_id = '$(topSessionId)' and
                $localTable."timestamp" = $(topTimeStamp) and
                $tableRt.encoded_size > 1000000 and
                $tableRt.url not like '%/interactive-assets/%'
            order by $tableRt.start_time
        """);

        recordsFound = nrow(joinTablesDetails)
        if (recordsFound > 0)
            displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(joinTablesDetails;limit=SP.scrubUrlChars)
            beautifyDF(joinTablesDetails[1:end,:])
        end
    catch y
        println("joinTablesDetailsPrint Exception ",y)
    end
end

function statsTableDF2(TV::TimeVars,UP::UrlParams)
    try
        table = UP.btView

        localStats = query("""\
            select timers_t_done
            from $table
            where
                page_group ilike '$(UP.pageGroup)' and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableDF2 Exception ",y)
    end
end

function defaultResourceView(TV::TimeVars,UP::UrlParams)

    try
        localTableRt = UP.rtView
        localRt = UP.resourceTable
        localTable = UP.btView

        query("""create or replace view $localTableRt as (
            select $tableRt.*
            from $localTable join $tableRt
                on $tableRt.session_id = $localTable.session_id
            where
                $tableRt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                $localTable.session_id IS NOT NULL
            order by $tableRt.session_id, $tableRt."timestamp", $tableRt.start_time
        )""")

        # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
        # where beacon_type = 'page view'
        localTableRtDF = query("""SELECT * FROM $localTableRt""")
        #Hide output from final report
        println("$localTableRt count is ",size(localTableRtDF))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function topPageViewsUDB(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        localTable = UP.btView

        topurl = query("""\
            select
                count(*),
                CASE when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) else trim('/' from params_u) end urlgroup
            FROM $(localTable)
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
        localTable = UP.btView

        topurl = query("""\
            select
                count(*),params_u
            FROM $(localTable)
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
