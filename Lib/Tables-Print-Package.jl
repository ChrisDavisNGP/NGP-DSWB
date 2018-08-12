#
#  Functions which print tables only
#

function urlCountPrintTable(UP::UrlParams,SP::ShowParams)

    try

        test1Table = select("""\
            select URL, count(*)
            FROM $(UP.btView)
            GROUP BY url
            Order by count(*) desc
        """)

        beautifyDF(test1Table[1:min(SP.showLines,end),:])
    catch y
        println("urlCountPrintTable Exception ",y)
    end
end

function agentCountPrintTable(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = select("""\
            select count(*),user_agent_raw
            FROM $(UP.btView)
            where
                beacon_type = 'page view'
            group by user_agent_raw
            order by count(*) desc
        limit $(UP.limitQueryRows)
    """)

        beautifyDF(CleanupTable[1:min(SP.showLines,end),:])

    catch y
        println("agentCountPrintTable Exception ",y)
    end
end

function urlParamsUCountPrintTable(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = select("""\
            select count(*), URL, params_u
            FROM $(UP.btView)
            where
                beacon_type = 'page view'
            GROUP BY url,params_u
            Order by count(*) desc
    """)

        beautifyDF(CleanupTable[1:min(SP.showLines,end),:])

    catch y
        println("urlParamsUCountPrintTable Exception ",y)
    end
end

function paramsUCountPrintTable(UP::UrlParams,SP::ShowParams)

    try
        CleanupTable = select("""\
            select count(*) as "Page Views",params_u as "URL Landing In Nat Geo Site Default Group"
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
        println("paramsUCountPrintTable Exception ",y)
    end
end

function joinTablesDetailsPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,joinTableSummary::DataFrame,row::Int64)
    try
        btv = UP.btView
        rt = UP.resourceTable

        topSessionId = joinTableSummary[row:row,:session_id][1]
        topTimeStamp = joinTableSummary[row:row,:timestamp][1]
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        joinTablesDetails = select("""\
            select $rt.start_time,$rt.encoded_size,$rt.transferred_size,$rt.decoded_size,$rt.url as urlgroup
            FROM $btv join $rt
                on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
            where
                $btv.session_id = '$(topSessionId)' and
                $btv.timestamp = $(topTimeStamp) and
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
        println("joinTablesDetailsPrintTable Exception ",y)
    end
end

function countUrlgroupPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView

        topurl = select("""\
            select count(*),CASE when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) else trim('/' from params_u) end urlgroup
            FROM $(btv)
            where
                beacon_type = 'page view'
            group by urlgroup
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function countParamUBtViewPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView

        topurl = select("""\
            select count(*),params_u
            FROM $(btv)
            where
                beacon_type = 'page view'
            group by params_u
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """)

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString,"URL: $(UP.urlRegEx)"])
        beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function bigPages2PrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        displayTitle(chart_title = "Big Pages (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesDF = select("""\
            select params_dom_sz,timers_t_page load_time,params_u urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            order by params_dom_sz desc
            limit $(UP.limitQueryRows)
        """);

        scrubUrlToPrint(SP,bigPagesDF,:urlgroup)
        beautifyDF(names!(bigPagesDF[1:min(SP.showLines,end),:],[Symbol("Size");Symbol("Load Time (ms)");Symbol("URL")]))
    catch y
        println("bigPages2PrintTable Exception ",y)
    end
end

function bigPages3PrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView

        displayTitle(chart_title = "Big Pages By Average Size (Min $(minSizeBytes) KB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        bigAveragePagesDF = select("""\
            select count(*),avg(params_dom_sz) as size,avg(timers_t_page) as load,params_u as urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes)
            group by urlgroup
            order by size desc
            limit $(UP.limitQueryRows)
        """);

        scrubUrlToPrint(SP,bigAveragePagesDF,:urlgroup)
        beautifyDF(names!(bigAveragePagesDF[1:min(SP.showLines,end),:],[Symbol("Count");Symbol("Size");Symbol("Load Time (ms)");Symbol("URL")]))
    catch y
        println("bigPages3PrintTable Exception ",y)
    end
end

function bigPages4PrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView

        displayTitle(chart_title = "Big Pages With Session ID (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        bigPagesSessionsDF = select("""\
            select params_dom_sz dom_size,session_id,timestamp,params_u urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL and
                params_dom_sz > $(minSizeBytes) and
                session_id IS NOT NULL
            order by dom_size desc
            limit $(UP.limitQueryRows)
        """);

        scrubUrlToPrint(SP,bigPagesSessionsDF,:urlgroup)
        beautifyDF(names!(bigPagesSessionsDF[1:min(end,SP.showLines),:],[Symbol("Size");Symbol("Session ID");Symbol("Timestamp");Symbol("URL")]))
    catch y
        println("bigPages4PrintTable Exception ",y)
    end
end

function bigPages5PrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select count(*) cnt,$btv.params_dom_sz dom_size,$btv.session_id s_id,$btv.timestamp
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
            where
                $btv.params_dom_sz IS NOT NULL and
                $btv.params_dom_sz > $(minSizeBytes) and
                $btv.session_id IS NOT NULL
            group by $btv.params_dom_sz, $btv.session_id, $btv.timestamp
            order by $btv.params_dom_sz desc
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Big Pages With Timestamp (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(names!(joinTablesDF[1:min(end,SP.showLines),:],[Symbol("Page Views");Symbol("Size");Symbol("Session ID");Symbol("TimeStamp")]))

    catch y
        println("bigPages5PrintTable Exception ",y)
    end
end

function bigPages6PrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,minSizeBytes::Float64)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select $btv.params_dom_sz dom_size,$btv.session_id,$btv.timestamp,$rt.start_time,$rt.encoded_size,
                $rt.transferred_size,
                $rt.decoded_size,
                $rt.url urlgroup
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
            where
                $btv.params_dom_sz IS NOT NULL and
                $btv.params_dom_sz > $(minSizeBytes) and
                $btv.session_id IS NOT NULL
            order by $btv.params_dom_sz
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Big Pages Details (Min $(minSizeBytes) KB)", chart_info = [TV.timeString], showTimeStamp=false)
        scrubUrlToPrint(SP,joinTablesDF,:urlgroup)
        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("bigPages6PrintTable Exception ",y)
    end
end

function bigPagesSizePrintTable(TV,UP,SP,fileType::ASCIIString;minEncoded::Int64=1000)

    # Create the summary table

    btv = UP.btView
    rt = UP.resourceTable

    try
        joinTablesDF = select("""\
        select avg($rt.encoded_size) as encoded,avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            $btv.user_agent_os,
            $btv.user_agent_family,
            count(*)
        from $btv join $rt
            on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
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
        println("bigPagesSizePrintTable 1 Exception ",y)
    end

    # Create the details table

    try

        joinTablesDF = select("""\
        select avg($rt.encoded_size) as encoded,avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            count(*),
            CASE WHEN (position('?' in $btv.params_u) > 0) then trim('/' from (substring($btv.params_u for position('?' in substring($btv.params_u from 9)) +7))) else trim('/' from $btv.params_u) end as urlgroup,
            $rt.url
        from $btv join $rt
            on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
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
        println("bigPagesSizePrintTable 2 Exception ",y)
    end

end

function lookForLeftOversPrintTable(UP::UrlParams,SP::ShowParams)

    joinTablesDF = DataFrame()

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = select("""\
        select $btv.user_agent_os,$btv.user_agent_family,$btv.user_agent_device_type,
            $rt.url,
            avg($rt.encoded_size) as encoded,
            avg($rt.transferred_size) as transferred,
            avg($rt.decoded_size) as decoded,
            count(*)
        from $btv join $rt
        on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
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

        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("lookForLeftOversPrintTable Exception ",y)
    end
    #display(joinTablesDF)
end

function lookForLeftOversDetailsPrintTable(UP::UrlParams,SP::ShowParams)

    joinTablesDF = DataFrame()

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select $rt.url,avg($rt.encoded_size) as encoded,avg($rt.transferred_size) as transferred,
                avg($rt.decoded_size) as decoded,
                $btv.compression_types,$btv.domain,$btv.geo_netspeed,$btv.mobile_connection_type,$btv.params_scr_bpp,$btv.params_scr_dpx,$btv.params_scr_mtp,$btv.params_scr_orn,params_scr_xy,
                $btv.user_agent_family,$btv.user_agent_major,$btv.user_agent_minor,$btv.user_agent_mobile,$btv.user_agent_model,$btv.user_agent_os,$btv.user_agent_osversion,$btv.user_agent_raw,
                $btv.user_agent_manufacturer,$btv.user_agent_device_type,$btv.user_agent_isp,$btv.geo_isp,$btv.params_ua_plt,$btv.params_ua_vnd,
                $rt.initiator_type,$rt.height,$rt.width,$rt.x,$rt.y,
                count(*)
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
            where $rt.encoded_size > 1 and $rt.url not like '%/interactive-assets/%'
            group by $rt.url,
                $btv.compression_types,$btv.domain,$btv.geo_netspeed,$btv.mobile_connection_type,$btv.params_scr_bpp,$btv.params_scr_dpx,$btv.params_scr_mtp,$btv.params_scr_orn,params_scr_xy,
                $btv.user_agent_family,$btv.user_agent_major,$btv.user_agent_minor,$btv.user_agent_mobile,$btv.user_agent_model,$btv.user_agent_os,$btv.user_agent_osversion,$btv.user_agent_raw,
                $btv.user_agent_manufacturer,$btv.user_agent_device_type,$btv.user_agent_isp,$btv.geo_isp,$btv.params_ua_plt,$btv.params_ua_vnd,
                $rt.initiator_type,$rt.height,$rt.width,$rt.x,$rt.y
            order by encoded desc
        """);

        beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
    catch y
        println("lookForLeftOversDetailsPrintTable Exception ",y)
    end
end

function requestCountByGroupPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,typeStr::ASCIIString)

    rc = select("""\

        select count(*) reqcnt, substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        group by urlgroup
        order by reqcnt desc
        LIMIT $(UP.limitQueryRows)
    """)

    displayTitle(chart_title = "$(typeStr): Request Counts By URL Group", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(rc[1:min(SP.showLines,end),:])

end

function nonCacheRequestCountByGroupPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams,typeStr::ASCIIString)

    nc = select("""\
        select count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) > 0
        group by urlgroup
        order by count(*) desc
        LIMIT $(UP.limitQueryRows)
    """)

    displayTitle(chart_title = "$(typeStr): Non Cache Requests Total By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(nc[1:min(SP.showLines,end),:])
end

function cacheHitRatioPrintTable(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    cached = select("""\
        select count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) = 0
        group by urlgroup
        order by count(*) desc
        LIMIT $(UP.limitQueryRows)
    """)

    ratio = select("""\
        select substring(url for position('/' in substring(url from 9)) +7) urlgroup, count(*) notCachedCount, 0 cachedCount, 0.0 ratio
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) > 0
        group by urlgroup
        order by count(*) desc
        LIMIT $(UP.limitQueryRows)
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
    beautifyDF(names!(ratio[1:min(30, end),[1:4;]],[Symbol("Url Group"), Symbol("Not Cached Cnt"), Symbol("Cached Cnt"), Symbol("Cached Ratio")]))
end

# Select * from beaconTable into data frame

function displayMatchingResourcesByParentUrlPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select count(*), params_u as parenturl
            from $rt
            where
                params_u ilike '$(UP.resRegEx)' and
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by parenturl
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Any Parent Url (params_u) for pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            joinTablesDF = names!(joinTablesDF,[Symbol("Resource Count"),Symbol("Parent URLs for Resources")])
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            joinTablesDF = names!(joinTablesDF,[Symbol("count"),Symbol("parenturl")])
            dataframeFieldStats(TV,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Any Parent Url (params_u) for pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByParentUrlPrintTable Exception ",y)
    end
end

function displayMatchingResourcesByUrlRtPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select count(*), url
            from $rt
            where
                url ilike '$(UP.resRegEx)' and
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Any resource Url for pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Any resource Url for pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByUrlRtPrintTable Exception ",y)
    end
end

function displayMatchingResourcesByUrlBtvRtPrintTables(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select count(*), $rt.params_u as parenturl, $rt.url
            from $btv join $rt
                on $btv.session_id = $rt.session_id and $btv.timestamp = $rt.timestamp
            where
            $rt.url ilike '$(UP.resRegEx)'
            group by $rt.params_u, $rt.url, $btv.url
            order by count(*) desc
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Beacon table and resources joined with resource Url pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:],maxRows=SP.showLines)
            dataframeFieldStats(TV,SP,joinTablesDF,:count,"on column count")
        else
            displayTitle(chart_title = "Beacon table and resources joined with resource Url pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesByUrlBtvRtPrintTables Exception ",y)
    end
end

function displayMatchingResourcesAllFieldsPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select *
            from $rt
            where
              url ilike '$(UP.resRegEx)' and
              timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
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
        println("displayMatchingResourcesAllFieldsPrintTable Exception ",y)
    end
end

function displayMatchingResourcesStatsPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        rt = UP.resourceTable

        joinTablesDF = select("""\
            select count(*),avg(start_time) as "start",
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
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by parenturl,url
            order by count(*) desc
        """);

        if (size(joinTablesDF)[1] > 0)
            displayTitle(chart_title = "Average Times for Url pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
            scrubUrlToPrint(SP,joinTablesDF,:url)
            scrubUrlToPrint(SP,joinTablesDF,:parenturl)
            beautifyDF(joinTablesDF[1:min(SP.showLines,end),:])
            dataframeFieldStats(TV,SP,joinTablesDF,:count,"on column count")
            dataframeFieldStats(TV,SP,joinTablesDF,:responselastbyte,"on Average Response Last Byte")
            dataframeFieldStats(TV,SP,joinTablesDF,:maxresponselastbyte,"on Maximum Response Last Byte")
        else
            displayTitle(chart_title = "Average Times for Url pattern $(UP.resRegEx) is empty", showTimeStamp=false)
        end

    catch y
        println("displayMatchingResourcesStatsPrintTable Exception ",y)
    end
end

function topUrlTableByCountPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
        try

            ltName = UP.btView

            if (SP.debugLevel > 4)
                dbgtopurl = select("""\

                select *
                FROM $(ltName)
                where
                    beacon_type = 'page view'
                    limit 10
                """);

                println(nrow(dbgtopurl))
                beautifyDF(dbgtopurl)
            end

            topurl = select("""\

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
            printDF = names!(newDF[:,:],[Symbol("Views"),Symbol("Url - $(UP.pageGroup)")])

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

function largePageDetailsPrintTable(localTable::ASCIIString,tableRt::ASCIIString,joinTableSummary::DataFrame,row::Int64)
    try
        topSessionId = joinTableSummary[row:row,:session_id][1]
        topTimeStamp = joinTableSummary[row:row,:timestamp][1]
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        joinTablesDetails = select("""\
            select $tableRt.start_time,$tableRt.encoded_size,$tableRt.transferred_size,
                $tableRt.decoded_size,
                $tableRt.url as urlgroup
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and
                $localTable.timestamp = $tableRt.timestamp
            where
                $localTable.session_id = '$(topSessionId)' and
                $localTable.timestamp = $(topTimeStamp) and
                $tableRt.encoded_size > 1000000
            order by $tableRt.start_time
        """);

        displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
        scrubUrlToPrint(SP,joinTablesDetails,:urlgroup)
        beautifyDF(joinTablesDetails[1:end,:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceScreenPrintTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    tvUpSpDumpDebug(TV,UP,SP,"resourceScreenPrintTable")

    try
        joinTables = select("""\
            select count(*),initiator_type,height,width,x,y,url
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by initiator_type,height,width,x,y,url
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Screen Details For Resource Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceScreenPrintTable Exception ",y)
    end
end
