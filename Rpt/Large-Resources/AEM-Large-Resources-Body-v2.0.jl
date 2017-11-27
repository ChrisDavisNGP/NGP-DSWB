function defaultLocalTableAemOnlyALR(TV::TimeVars,UP::UrlParams)
    try
        table = UP.beaconTable
        localTable = UP.btView

        query("""\
            create or replace view $localTable as (
                select * from $table
                    where
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group in ('News Articles','Travel AEM','Photography AEM','Nat Geo Homepage','Magazine','Magazine AEM') and
                        params_u ilike '$(UP.urlRegEx)' and
                        user_agent_device_type ilike '$(UP.deviceType)'
            )
        """)
        cnt = query("""SELECT count(*) FROM $localTable""")
        println("$localTable count is ",cnt[1,1])
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function defaultLocalTableALR(TV::TimeVars,UP::UrlParams)
    try
        table = UP.beaconTable
        localTable = UP.btView

        query("""\
            create or replace view $localTable as (
                select * from $table
                    where
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group ilike '$(UP.pageGroup)' and
                        params_u ilike '$(UP.urlRegEx)' and
                        user_agent_device_type ilike '$(UP.deviceType)'
            )
        """)
        cnt = query("""SELECT count(*) FROM $localTable""")
        println("$localTable count is ",cnt[1,1])
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function resourceSummary(UP::UrlParams,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            $localTable.user_agent_os,
            $localTable.user_agent_family,
            count(*)
        from $localTable join $tableRt
            on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(minEncoded) and
            $tableRt.url ilike '$(fileType)'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os
        order by encoded desc, transferred desc, decoded desc
        """);

        displayTitle(chart_title = "Mobile Big Pages Details (Min $(minEncoded) KB), File Type $(fileType)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSizes2(UP::UrlParams,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*),
            CASE WHEN (position('?' in $localTable.params_u) > 0) then trim('/' from (substring($localTable.params_u for position('?' in substring($localTable.params_u from 9)) +7))) else trim('/' from $localTable.params_u) end as urlgroup,
            $tableRt.url
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(minEncoded) and
        $tableRt.url not like '%/interactive-assets/%' and ($tableRt.url ilike '$(fileType)' or $tableRt.url ilike '$(fileType)?%')
        group by
        $localTable.params_u,$tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSizes12(UP::UrlParams,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            $localTable.user_agent_os,
            $localTable.user_agent_family,
            $tableRt.url,
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*)
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(minEncoded) and
        $tableRt.url not like '%/interactive-assets/%' and $tableRt.url ilike '$(fileType)'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os,
            $tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function lookForLeftOversALR(UP::UrlParams,linesOutput::Int64)

    joinTables = DataFrame()

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            $localTable.user_agent_os,
            $localTable.user_agent_family,
            $localTable.user_agent_device_type,
            $tableRt.url,
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*)
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > 1 and
        $tableRt.url not ilike '%/interactive-assets/%' and
        $tableRt.url not ilike '%png' and
        $tableRt.url not ilike '%svg' and
        $tableRt.url not ilike '%jpg' and
        $tableRt.url not ilike '%mp3' and
        $tableRt.url not ilike '%mp4' and
        $tableRt.url not ilike '%gif' and
        $tableRt.url not ilike '%wav' and
        $tableRt.url not ilike '%jog' and
        $tableRt.url not ilike '%js' and
        $tableRt.url not ilike '%.js?%' and
        $tableRt.url not ilike '%css' and
        $tableRt.url not ilike '%ttf' and
        $tableRt.url not ilike '%woff%'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os,
            $localTable.user_agent_device_type,
            $tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversALR Exception ",y)
    end
    #display(joinTables)
end

function lookForLeftOversDetailsALR(UP::UrlParams,linesOutput::Int64)

    joinTables = DataFrame()

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
            select
                $tableRt.url,
                avg($tableRt.encoded_size) as encoded,
                avg($tableRt.transferred_size) as transferred,
                avg($tableRt.decoded_size) as decoded,
                $localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
                $localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
                $localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd,
                $tableRt.initiator_type,$tableRt.height,$tableRt.width,$tableRt.x,$tableRt.y,
                count(*)
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where $tableRt.encoded_size > 1 and $tableRt.url not like '%/interactive-assets/%'
            group by $tableRt.url,
                $localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
                $localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
                $localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd,
                $tableRt.initiator_type,$tableRt.height,$tableRt.width,$tableRt.x,$tableRt.y
            order by encoded desc
        """);

        beautifyDF(joinTables[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversDetailsALR Exception ",y)
    end
end
