type LocalVars
    linesOutput::Int64
end

function defaultBeaconViewUDB(TV::TimeVars,UP::UrlParams)
    
    try
        localTable = UP.btView
        table = UP.beaconTable

        query("""create or replace view $localTable as (
            select * 
            from $table 
            where 
                page_group = '$(UP.pageGroup)' and 
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and 
                params_u ilike '$(UP.urlRegEx)'
        )""")

        setTable(localTable)

        # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
        # where beacon_type = 'page view'
        localTableDF = query("""SELECT * FROM $localTable""")
        #Hide output from final report
        println("$localTable count is ",size(localTableDF))
    catch y
        println("setupLocalTable Exception ",y)
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

function topPageViewsUDB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
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
            limit $(LV.linesOutput)
        """);

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function topUrlPageViewsUDB(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
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
            limit $(LV.linesOutput)
        """)

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString,"URL: $(UP.urlRegEx)"])
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

