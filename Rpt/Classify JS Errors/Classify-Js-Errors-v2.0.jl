type LocalVars
    linesOutput::Int64
end

function defaultErrorTableCJE(TV::TimeVars,UP::UrlParams,SP::ShowParams)

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
                        errors is not null
            )
        """)
        
        if (SP.debugLevel > 0)
            cnt = query("""SELECT count(*) FROM $localTable""")
            println("$localTable count is ",cnt[1,1])
        end
    catch y
        println("defaultTableFARB Exception ",y)
    end
end

function resourceSummaryFARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                count(*), $tableRt.url, $tableRt.params_u, $localTable.url
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where 
            $tableRt.url ilike '$(UP.resRegEx)'
            group by $tableRt.url, $tableRt.params_u, $localTable.url
            order by count(*) desc
        """);

        displayTitle(chart_title = "Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummaryFARB Exception ",y)
    end
end 

function resourceSummary2FARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try        
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                count(*), url
            from $tableRt
            where 
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url
            order by count(*) desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummary2FARB Exception ",y)
    end
end 

function resourceSummary3FARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                *
            from $tableRt
            where 
              url ilike '$(UP.resRegEx)' and
              "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            limit 3
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummary3FARB Exception ",y)
    end
end 

function resourceSummary4FARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                count(*),
                avg(start_time) as "Start Time",
                avg(fetch_start) as "Fetch S",
                avg(dns_end-dns_start) as "DNS ms",
                avg(tcp_connection_end-tcp_connection_start) as "TCP ms",
                avg(request_start) as "Req S",
                avg(response_first_byte) as "Req FB",	
                avg(response_last_byte) as "Req LB",
                max(response_last_byte) as "Max Req LB",
                url, params_u,
                avg(redirect_start) as "Redirect S",
                avg(redirect_end) as "Redirect E",
                avg(secure_connection_start) as "Secure Conn S"
            from $tableRt
            where 
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url, params_u
            order by count(*) desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummary4FARB Exception ",y)
    end
end 

function resourceSummary5FARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                (response_last_byte-start_time) as "Time Taken",
                (start_time) as "Start Time",
                (fetch_start) as "Fetch S",
                (dns_end-dns_start) as "DNS ms",
                (tcp_connection_end-tcp_connection_start) as "TCP ms",
                (request_start) as "Req S",
                (response_first_byte) as "Req FB",	
                (response_last_byte) as "Req LB",
                url, params_u,
                (redirect_start) as "Redirect S",
                (redirect_end) as "Redirect E",
                (secure_connection_start) as "Secure Conn S"
            from $tableRt
            where 
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                (response_last_byte-start_time) > 5000
            order by "Time Taken" desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummary5FARB Exception ",y)
    end
end 

function resourceSummary6FARB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
            select 
                (response_last_byte-start_time) as "Time Taken",
                (start_time) as "Start Time",
                (fetch_start) as "Fetch S",
                (dns_end-dns_start) as "DNS ms",
                (tcp_connection_end-tcp_connection_start) as "TCP ms",
                (request_start) as "Req S",
                (response_first_byte) as "Req FB",	
                (response_last_byte) as "Req LB",
                url, params_u,
                (redirect_start) as "Redirect S",
                (redirect_end) as "Redirect E",
                (secure_connection_start) as "Secure Conn S"
            from $tableRt
            where 
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                start_time > 10000
            order by start_time desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(LV.linesOutput,end),:])
    catch y
        println("resourceSummary6FARB Exception ",y)
    end
end 

