#
# Functions which return a data frame
#

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
            limit $(UP.limitRows)
        """)

        if (SP.debugLevel > 8)
            standardChartTitle(TV,UP,SP,"Debug8: defaultLimitedBeaconsToDF All Columns")
            beautifyDF(localTableDF[1:min(3,end),:])
        end

        return localTableDF
    catch y
        println("defaultLimitedBeaconsToDF Exception ",y)
    end
end

function errorBeaconsToDF(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable

    if (SP.debugLevel > 4)
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
            limit $(UP.limitRows)
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
        println("Starting allSessionUrlTableToDF")
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

        localStats = query("""select "timestamp",timers_t_done,timers_domready from $btv""");

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
            limit $(UP.limitRows)
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
