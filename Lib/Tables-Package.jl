#
# Functions which return a data frame
#

function defaultResourcesToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    tvUpSpDumpDebug(TV,UP,SP,"defaultResourcesToDF")

    rt = UP.resourceTable
    bt = UP.beaconTable

    try
        localTableDF = query("""\
            select $rt.*
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
        """)

        tableDumpDFDebug(TV,UP,SP,localTableDF)

        return localTableDF
    catch y
        println("defaultResourcesToDF Exception ",y)
    end
end




function defaultBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    tvUpSpDumpDebug(TV,UP,SP,"defaultBeaconsToDF")

    bt = UP.beaconTable

    try
        localTableDF = query("""\
            select *
            from $bt
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

        tableDumpDFDebug(TV,UP,SP,localTableDF)

        return localTableDF
    catch y
        println("defaultBeaconsToDF Exception ",y)
    end
end

function defaultLimitedBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    if (SP.debugLevel > 4)
        println("defaultLimitedBeaconsToDF Starting")
        println("Time MS UTC: $(TV.startTimeMsUTC),$(TV.endTimeMsUTC)")
        println("urlRegEx $(UP.urlRegEx)")
        println("dev=$(UP.deviceType), os=$(UP.agentOs), page grp=$(UP.pageGroup)")
        println("time Range: $(UP.timeLowerMs),$(UP.timeUpperMs)")
    end

    try
        localTableDF = query("""\
            select *
            from $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                session_id IS NOT NULL and
                params_rt_quit IS NULL and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                user_agent_os ilike '$(UP.agentOs)' and
                page_group ilike '$(UP.pageGroup)' and
                timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
            limit $(UP.limitQueryRows)
        """)

        if (SP.debugLevel > 8)
            standardChartTitle(TV,UP,SP,"Debug8: defaultLimitedBeaconsToDF All Columns, 3 Rows")
            beautifyDF(localTableDF[1:min(3,end),:])
        end

        return localTableDF
    catch y
        println("defaultLimitedBeaconsToDF Exception ",y)
    end
end

function critAggLimitedBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams,NR::NrParams)

    if CU.syntheticMonitor == "no name"
        critAggLimitedBeaconsToDFSoasta(TV,UP,SP)
    else
        critAggLimitedBeaconsToDFNR(TV,SP,CU,NR)
    end

end

function critAggLimitedBeaconsToDFNR(TV::TimeVars,SP::ShowParams,CU::CurlParams,NR::NrParams)

    try
        jsonTimeString = curlCritAggLimitedBeaconsToDFNR(TV,SP,CU)
        timeDict = curlSyntheticJson(SP,jsonTimeString)

        fillNrResults(SP,NR,timeDict["results"])

        if SP.debugLevel > 6
            beautifyDF(NR.results.row[1:min(3,end),:])
        end

        localTableDF = DataFrame(jobid=ASCIIString[],timestamp=Int64[],onpageload=Int64[],onpagecontentload=Int64[])
        for row in eachrow(NR.results.row)
            push!(localTableDF,[row[:jobId];row[:timestamp];round(row[:onPageLoad],0);round(row[:onPageContentLoad],0)])
        end

        localTableDF = names!(localTableDF,[Symbol("session_id");Symbol("timestamp");Symbol("timers_t_done");Symbol("timers_domready")])

        if SP.debugLevel > 6
            beautifyDF(localTableDF)
        end
        return localTableDF

    catch y
        println("critAggLimitedBeaconsToDFNR Exception ",y)
    end

end

function critAggLimitedBeaconsToDFSoasta(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    if (SP.debugLevel > 4)
        println("critAggLimitedBeaconsToDFSoasta Starting")
        println("Time MS UTC: $(TV.startTimeMsUTC),$(TV.endTimeMsUTC)")
        println("urlRegEx $(UP.urlRegEx)")
        println("dev=$(UP.deviceType), os=$(UP.agentOs), page grp=$(UP.pageGroup)")
        println("time Range: $(UP.timeLowerMs),$(UP.timeUpperMs)")
    end

    try
        localTableDF = query("""\
            select "timestamp",session_id,timers_t_done,timers_domready
            from $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                session_id IS NOT NULL and
                params_rt_quit IS NULL and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                user_agent_os ilike '$(UP.agentOs)' and
                page_group ilike '$(UP.pageGroup)' and
                timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
            limit $(UP.limitQueryRows)
        """)

        if (SP.debugLevel > 6)
            standardChartTitle(TV,UP,SP,"Debug8: critAggLimitedBeaconsToDF All Columns")
            beautifyDF(localTableDF[1:min(10,end),:])
        end

        return localTableDF
    catch y
        println("critAggLimitedBeaconsToDF Exception ",y)
    end
end

function errorBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    if (SP.debugLevel > 4)
        println("errorBeaconsToDF")
        println("Time MS UTC: $(TV.startTimeMsUTC),$(TV.endTimeMsUTC)")
        println("urlRegEx $(UP.urlRegEx)")
        println("dev=$(UP.deviceType), os=$(UP.agentOs), page grp=$(UP.pageGroup)")
        println("time Range: $(UP.timeLowerMs),$(UP.timeUpperMs)")
    end

    try
        localTableDF = query("""\
            select *
            from $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                user_agent_os ilike '$(UP.agentOs)' and
                page_group ilike '$(UP.pageGroup)' and
                beacon_type = 'error'
            limit $(UP.limitQueryRows)
        """)

        if (SP.debugLevel > 8)
            standardChartTitle(TV,UP,SP,"Debug8: errorBeaconsToDF All Columns")
            beautifyDF(localTableDF[1:min(3,end),:])
        end

        return localTableDF
    catch y
        println("errorBeaconsToDF Exception ",y)
    end
end

function allPageUrlTableToDF(TV::TimeVars,UP::UrlParams)
    try
        bt = UP.beaconTable
        rt = UP.resourceTable

        if (UP.usePageLoad)
            toppageurl = query("""\
            select 'None' as urlpagegroup,avg($rt.start_time),
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
            select 'None' as urlpagegroup,avg($rt.start_time),
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
        println("allPageUrlTableToDF Exception ",y)
    end
end

function allSessionUrlTableToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString)

    if SP.debugLevel > 8
        println("Starting allSessionUrlTableToDF")
    end

    rt = UP.resourceTable

    try
        toppageurl = query("""\
        select 'None' as urlpagegroup,avg(start_time),
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
        println("allSessionUrlTableToDF Exception ",y)
    end
end

function sessionUrlTableToDF(UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)

    if SP.debugLevel > 8
        println("Starting sessionUrlTableToDF: studySession= ",studySession," studyTime=",studyTime)
    end

    rt = UP.resourceTable
    try
        toppageurl = query("""\
        select 'None' as urlpagegroup,start_time,
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
        println("sessionUrlTableToDF Exception ",y)
    end
end

function getResourcesForBeaconToDF(TV::TimeVars,UP::UrlParams)

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

function treemapsLocalTableRtToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if SP.debugLevel > 8
        println("Starting treemapsLocalTableRtToDF")
    end

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
        println("treemapsLocalTableRtToDF Exception ",y)
    end
end

function gatherSizeDataToDF(UP::UrlParams,SP::ShowParams)
    try
        bt = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
        select CASE WHEN (position('?' in $bt.params_u) > 0) then trim('/' from (substring($bt.params_u for position('?' in substring($bt.params_u from 9)) +7))) else trim('/' from $bt.params_u) end as urlgroup,
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
        println("gatherSizeDataToDF Exception ",y)
    end
end

function statsBtViewByHourToDF(btv::ASCIIString,startTimeMsUTC::Int64, endTimeMsUTC::Int64)
    try
        localStats = query("""\
            select timers_t_done
            FROM $btv
            where
                "timestamp" between $startTimeMsUTC and $endTimeMsUTC
        """);
        return localStats
    catch y
        println("statsBtTableToDF Exception ",y)
    end
end

function statsBtViewTableToExtraDF(UP::UrlParams)
    try
        btv = UP.btView

        localStats = query("""\
        select
        case
          when timers_t_done between     0 and  1000 then '    0-1000'
          when timers_t_done between  1001 and  2000 then ' 1001-2000'
          when timers_t_done between  2001 and  3000 then ' 2001-3000'
          when timers_t_done between  3001 and  4000 then ' 3001-4000'
          when timers_t_done between  4001 and  5000 then ' 4001-5000'
          when timers_t_done between  5001 and  6000 then ' 5001-6000'
          when timers_t_done between  6001 and  7000 then ' 6001-7000'
          when timers_t_done between  7001 and  8000 then ' 7001-8000'
          when timers_t_done between  8001 and  9000 then ' 8001-9000'
          when timers_t_done between  9001 and 10000 then ' 9001-10000'
          when timers_t_done between 10001 and 11000 then '10001-11000'
          when timers_t_done between 11001 and 12000 then '11001-12000'
          when timers_t_done between 12001 and 13000 then '12001-13000'
          when timers_t_done between 13001 and 14000 then '13001-14000'
          when timers_t_done between 14001 and 15000 then '14001-15000'
          when timers_t_done between 15001 and 16000 then '15001-16000'
          when timers_t_done between 16001 and 17000 then '16001-17000'
          when timers_t_done between 17001 and 18000 then '17001-18000'
          when timers_t_done between 18001 and 19000 then '18001-19000'
          when timers_t_done between 19001 and 20000 then '19001-20000'
          when timers_t_done between 20001 and 21000 then '20001-21000'
          when timers_t_done between 21001 and 22000 then '21001-22000'
          when timers_t_done between 22001 and 23000 then '22001-23000'
          when timers_t_done between 23001 and 24000 then '23001-24000'
          when timers_t_done between 24001 and 25000 then '24001-25000'
          when timers_t_done between 25001 and 26000 then '25001-26000'
          when timers_t_done between 26001 and 27000 then '26001-27000'
          when timers_t_done between 27001 and 28000 then '27001-28000'
          when timers_t_done between 28001 and 29000 then '28001-29000'
          when timers_t_done between 29001 and 30000 then '29001-30000'
          when timers_t_done between 30001 and 31000 then '30001-31000'
          when timers_t_done between 31001 and 32000 then '31001-32000'
          when timers_t_done between 32001 and 33000 then '32001-33000'
          when timers_t_done between 33001 and 34000 then '33001-34000'
          when timers_t_done between 34001 and 35000 then '34001-35000'
          when timers_t_done between 35001 and 36000 then '35001-36000'
          when timers_t_done between 36001 and 37000 then '36001-37000'
          when timers_t_done between 37001 and 38000 then '37001-38000'
          when timers_t_done between 38001 and 39000 then '38001-39000'
          when timers_t_done between 39001 and 40000 then '39001-40000'
          when timers_t_done between 40001 and 41000 then '40001-41000'
          when timers_t_done between 41001 and 42000 then '41001-42000'
          when timers_t_done between 42001 and 43000 then '42001-43000'
          when timers_t_done between 43001 and 44000 then '43001-44000'
          when timers_t_done between 44001 and 45000 then '44001-45000'
          when timers_t_done between 45001 and 46000 then '45001-46000'
          when timers_t_done between 46001 and 47000 then '46001-47000'
          when timers_t_done between 47001 and 48000 then '47001-48000'
          when timers_t_done between 48001 and 49000 then '48001-49000'
          when timers_t_done between 49001 and 50000 then '49001-50000'
          when timers_t_done between 50001 and 51000 then '50001-51000'
          when timers_t_done between 51001 and 52000 then '51001-52000'
          when timers_t_done between 52001 and 53000 then '52001-53000'
          when timers_t_done between 53001 and 54000 then '53001-54000'
          when timers_t_done between 54001 and 55000 then '54001-55000'
          when timers_t_done between 55001 and 56000 then '55001-56000'
          when timers_t_done between 56001 and 57000 then '56001-57000'
          when timers_t_done between 57001 and 58000 then '57001-58000'
          when timers_t_done between 58001 and 59000 then '58001-59000'
          when timers_t_done between 59001 and 60000 then '59001-60000'
        else
            '60001+'
        end as timersdone,
        count(1)
            from $btv
        group by 1
        order by 1 asc
            """);

        return localStats
    catch y
        println("statsBtViewTableToDF Exception ",y)
    end
end


function statsBtViewTableToDF(UP::UrlParams)
    try
        btv = UP.btView

        localStats = query("""select timers_t_done from $btv""");

        return localStats
    catch y
        println("statsBtViewTableToDF Exception ",y)
    end
end

function resourceImagesOnNatGeoToDF(UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    try
        btv = UP.btView
        rt = UP.resourceTable

        joinTablesDF = query("""\
        select avg($rt.encoded_size) as encoded,
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

function estimateFullBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  try
      table = UP.beaconTable
      rt = UP.resourceTable

      if (UP.usePageLoad)
          localTableDF = query("""\
          select CASE WHEN (position('?' in $rt.url) > 0) then trim('/' from (substring($rt.url for position('?' in substring($rt.url from 9)) +7))) else trim('/' from $rt.url) end as urlgroup,
            count(*) as request_count,
            avg($table.timers_t_done) as beacon_time,
            sum($rt.encoded_size) as encoded_size
          FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
              where
              $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
              and $table.session_id IS NOT NULL
              and $table.page_group ilike '$(UP.pageGroup)'
              and $table.params_u ilike '$(UP.urlRegEx)'
              and $table.user_agent_device_type ilike '$(UP.deviceType)'
              and $table.user_agent_os ilike '$(UP.agentOs)'
              and $table.timers_t_done >= $(UP.timeLowerMs) and $table.timers_t_done <= $(UP.timeUpperMs)
              and $table.params_rt_quit IS NULL
              and $table.errors IS NULL
          group by urlgroup,$table.session_id,$table."timestamp",errors
          """);
      else

          if (SP.debugLevel > 8)
              debugTableDF = query("""\
              select *
              FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
                  where
                  $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
                  and $table.session_id IS NOT NULL
                  and $table.page_group ilike '$(UP.pageGroup)'
                  and $table.params_u ilike '$(UP.urlRegEx)'
                  and $table.user_agent_device_type ilike '$(UP.deviceType)'
                  and $table.user_agent_os ilike '$(UP.agentOs)'
                  and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
                  and $table.params_rt_quit IS NULL
                  limit 3
                  """);

              beautifyDF(debugTableDF[1:min(30,end),:])
              println("pg=",UP.pageGroup," url=",UP.urlRegEx," dev=",UP.deviceType," dr lower=",UP.timeLowerMs," dr upper=",UP.timeUpperMs);

          end

          localTableDF = query("""\
          select CASE WHEN (position('?' in $table.params_u) > 0) then trim('/' from (substring($table.params_u for position('?' in substring($table.params_u from 9)) +7))) else trim('/' from $table.params_u) end as urlgroup,
              count(*) as request_count,
              avg($table.timers_domready) as beacon_time,
              sum($rt.encoded_size) as encoded_size
          FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
              where
              $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
              and $table.session_id IS NOT NULL
              and $table.page_group ilike '$(UP.pageGroup)'
              and $table.params_u ilike '$(UP.urlRegEx)'
              and $table.user_agent_device_type ilike '$(UP.deviceType)'
              and $table.user_agent_os ilike '$(UP.agentOs)'
              and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
              and $table.params_rt_quit IS NULL
              and $table.errors IS NULL
          group by urlgroup,$table.session_id,$table."timestamp",errors
              """);


          if (nrow(localTableDF) == 0)
              return localTableDF
          end

          # Clean Up Bad Samples
          # Currently request < 10

          iRow = 0
          reqVector = localTableDF[:request_count]

          for reqCount in reqVector
              iRow = iRow + 1
              if (reqCount < 10)
                  if (SP.debugLevel > 8)
                      beautifyDF(localTableDF[iRow:iRow,:])
                  end
                 deleterows!(localTableDF,iRow)
              end
          end

          if (SP.debugLevel > 6)
              beautifyDF(localTableDF[1:min(30,end),:])
          end
      end

      return localTableDF
  catch y
      println("urlDetailTables Exception ",y)
  end
end

function testUrlClassifyToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if SP.debugLevel > 8
        println("Starting testUrlClassifyToDF")
    end

    try
        rt = UP.resourceTable

        localTableRtDF = query("""\
            select 'None' as urlpagegroup,
                CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup
            FROM $rt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by urlgroup,urlpagegroup
            limit $(UP.limitQueryRows)
         """)

        if (SP.debugLevel > 6)
            beautifyDF(localTableRtDF[1:min(10,end),:])
        end

        return localTableRtDF
    catch y
      println("urlDetailTables Exception ",y)
    end
end

function localStatsFATS(TV::TimeVars,UP::UrlParams,statsDF::DataFrame)
    try
        LowerBy3Stddev = statsDF[1:1,:LowerBy3Stddev][1]
        UpperBy3Stddev = statsDF[1:1,:UpperBy3Stddev][1]
        UpperBy25p = statsDF[1:1,:UpperBy25p][1]

        localStats2 = query("""\
            select "timestamp", timers_t_done, session_id
            from $(UP.btView) where
                page_group ilike '$(UP.pageGroup)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                timers_t_done > $(UpperBy25p)
        """)

        return localStats2

    catch y
        println("localStatsFATS Exception ",y)
    end
end
