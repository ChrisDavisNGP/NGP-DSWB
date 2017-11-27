function gatherSizeDataALI(UP::UrlParams,SP::ShowParams)
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

        beautifyDF(joinTables[1:min(SP.showLines,end),:])

        return joinTables
    catch y
        println("gatherSizeDataALI Exception ",y)
    end
end

function tableSummaryALI(joinTableSummary::DataFrame,joinTables::DataFrame,SP::ShowParams)

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

    beautifyDF(joinTableSummary[1:min(SP.showLines,end),[:urlgroup,:encoded,:transferred,:decoded]])

    return joinTableSummary
end

function detailsPrint(UP::UrlParams,joinTableSummary::DataFrame,row::Int64)
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
            displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [tv.timeString], showTimeStamp=false)
            scrubUrlToPrint(joinTablesDetails,limit=250)
            beautifyDF(joinTablesDetails[1:end,:])
        end
    catch y
        println("bigTable5 Exception ",y)
    end
end

function statsTableDF2(TV::TimeVars,UP::UrlParams)
    try
        table = UP.btView

        localStats = query("""\
            select timers_t_done from $table where
                page_group ilike '$(UP.pageGroup)' and
                params_u ilike '$(UP.urlRegEx)' and
                user_agent_device_type ilike '$(UP.deviceType)' and
                "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
                params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableDF Exception ",y)
    end
end

function statsDetailsPrint(UP::UrlParams,joinTableSummary::DataFrame,row::Int64)
    try
        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])

        UP.deviceType = "Desktop"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,UP.pageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Mobile"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,UP.pageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Tablet"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,UP.pageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
            dispDMT[3:3,:Unit] = statsDF2[2:2,:unit]
            dispDMT[3:3,:Count] = statsDF2[2:2,:count]
            dispDMT[3:3,:Mean] = statsDF2[2:2,:mean]
            dispDMT[3:3,:Median] = statsDF2[2:2,:median]
            dispDMT[3:3,:Min] = statsDF2[2:2,:min]
            dispDMT[3:3,:Max] = statsDF2[2:2,:max]
        end

        displayTitle(chart_title = "Large Request Stats for: $(topTitle)", chart_info = [tv.timeString], showTimeStamp=false)
        beautifyDF(dispDMT)
    catch y
        println("statsTableDF2 Exception ",y)
    end
end
