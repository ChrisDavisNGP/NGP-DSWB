function defaultTableFLP(TV::TimeVars,UP::UrlParams)
    
    try
        table = UP.beaconTable
        localTable = UP.btView
        
        query("""\
            create or replace view $localTable as (
                select * from $table 
                    where 
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and 
                        page_group ilike '$(UP.pageGroup)' and
                        params_u ilike '$(UP.urlRegEx)'
            )
        """)
        
        cnt = query("""SELECT count(*) FROM $localTable""")
        println("$localTable count is ",cnt[1,1])
    catch y
        println("defaultTableFLP Exception ",y)
    end
end

function joinTableTableFLP(TV::TimeVars,UP::UrlParams)
    
    try
        localTable = UP.btView
        tableRt = UP.resourceTable
        
        joinTables = query("""\
        select 
            CASE WHEN (position('?' in $localTable.params_u) > 0) then trim('/' from (substring($localTable.params_u for position('?' in substring($localTable.params_u from 9)) +7))) else trim('/' from $localTable.params_u) end as urlgroup,
            $localTable.session_id,
            $localTable."timestamp",
            sum($tableRt.encoded_size) as encoded,
            sum($tableRt.transferred_size) as transferred,
            sum($tableRt.decoded_size) as decoded,
            count(*)
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > 1
        group by $localTable.params_u,$localTable.session_id,$localTable."timestamp"
        order by encoded desc
        """);

        #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
        scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOutput,end),:])
        return joinTables
    catch y
        println("joinTableTableFLP Exception ",y)
    end
end

function joinTableSummaryFLP(joinTables::DataFrame)

    joinTableSummary[:urlgroup] = "delete"
    joinTableSummary[:session_id] = ""
    joinTableSummary[:timestamp] = 0
    joinTableSummary[:encoded] = 0
    joinTableSummary[:transferred] = 0
    joinTableSummary[:decoded] = 0
    joinTableSummary[:count] = 0

    sort!(joinTables,cols=[order(:encoded,rev=true)])
    for subDf in groupby(joinTables,:urlgroup)
        #beautifyDF(subDf[1:1,:])
        i = 1
        for row in eachrow(subDf)
            if (i == 1) 
                i +=1
                push!(joinTableSummary,[row[:urlgroup],row[:session_id],row[:timestamp],row[:encoded],row[:transferred],row[:decoded],row[:count]])
            end            
        end
    end

    i = 1
    for x in joinTableSummary[:,:urlgroup]
        if x == "delete"
            deleterows!(joinTableSummary,i)
        end
        i += 1
    end
    sort!(joinTableSummary,cols=[order(:encoded,rev=true)])
    return joinTableSummary
end

function detailsPrintFLP(TV::TimeVars,UP::UrlParams,joinTableSummary::DataFrame,row::Int64)
    try
        topSessionId = joinTableSummary[row:row,:session_id][1]
        topTimeStamp = joinTableSummary[row:row,:timestamp][1]
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        localTable = UP.btView
        tableRt = UP.resourceTable
        
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
                $tableRt.encoded_size > 1000000
            order by $tableRt.start_time
        """);

        displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
        scrubUrlToPrint(joinTablesDetails,limit=250)
        beautifyDF(joinTablesDetails[1:end,:])
    catch y
        println("detailsPrintFLP Exception ",y)
    end
end

function statsTableDF2FLP(UP::UrlParams,productPageGroup::ASCIIString,localUrl::ASCIIString,deviceType::ASCIIString,startTimeMs::Int64, endTimeMs::Int64)
    try
        #println(localUrl)
        table = UP.beaconTable
        
        localStats = query("""\
            select timers_t_done 
            from $table 
            where 
                page_group ilike '$(productPageGroup)' and
                params_u ilike '$(localUrl)' and
                user_agent_device_type ilike '$(deviceType)' and        
                "timestamp" between $startTimeMs and $endTimeMs and
                params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableDF Exception ",y)
    end
end

function statsDetailsPrintFLP(TV::TimeVars,UP::UrlParams,joinTableSummary::DataFrame,row::Int64)
    try
        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]
        productPageGroup = UP.pageGroup
        
        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])
        
        statsFullDF2 = statsTableDF2FLP(UP,productPageGroup,topUrl,"Desktop",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2FLP(UP,productPageGroup,topUrl,"Mobile",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2FLP(UP,productPageGroup,topUrl,"Tablet",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
            dispDMT[3:3,:Unit] = statsDF2[2:2,:unit]
            dispDMT[3:3,:Count] = statsDF2[2:2,:count]
            dispDMT[3:3,:Mean] = statsDF2[2:2,:mean]
            dispDMT[3:3,:Median] = statsDF2[2:2,:median]
            dispDMT[3:3,:Min] = statsDF2[2:2,:min]
            dispDMT[3:3,:Max] = statsDF2[2:2,:max]
        end

        displayTitle(chart_title = "Large Request Stats for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(dispDMT)
    catch y
        println("statsTableDF2FLP Exception ",y)
    end
end
