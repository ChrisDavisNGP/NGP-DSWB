using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

# Packages
include("../../Lib/Include-Package-v2.1.jl")

# Time values (tv.) structure created in include above, so init time here
#TV = timeVariables(2017,1,2,2,30,2017,1,2,2,45);
TV = timeVariables(2017,3,8,6,0,2017,3,8,11,59);
#TV = weeklyTimeVariables(days=7);

customer = "Nat Geo"
productPageGroup = "News Article" # primary page group
localUrl = "http://news.nationalgeographic.com/news/2014/05/140518-dogs-war-canines-soldiers-troops-military-japanese-prisoner/"
localTable = "$(table)_Find_Large_Page_Url_view"
deviceType = "Mobile"
linesOutput = 25
;

try
    query("""\
        create or replace view $localTable as (
            select * from $table
                where
                    "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                    page_group ilike '$(productPageGroup)' and
                    params_u ilike '$(localUrl)'  and
                    $table.user_agent_device_type ilike '$(deviceType)'

        )
    """)
    cnt = query("""SELECT count(*) FROM $localTable""")
    println("$localTable count is ",cnt[1,1])
catch y
    println("setupLocalTable Exception ",y)
end

try
    query("""
    select
        user_agent_device_type,
        user_agent_family,
    user_agent_os,user_agent_manufacturer,params_ua_vnd,
    count(*)
    from $localTable
    group by
        user_agent_device_type,
        user_agent_family,
    user_agent_os,user_agent_manufacturer,params_ua_vnd
    order by count(*) desc
    limit 30
    """)
catch y
    println("setupLocalTable Exception ",y)
end



try
    query("""
    select
        user_agent_device_type,
        user_agent_family,
    geo_netspeed, params_cpu_cnc,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd,
    count(*)
    from $localTable
    group by
        user_agent_device_type,
        user_agent_family,
    geo_netspeed, params_cpu_cnc,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd
    order by count(*) desc
    limit 25
    """)
catch y
    println("setupLocalTable Exception ",y)
end

try
    query("""
    select
        user_agent_device_type,
        user_agent_family,
        params_scr_xy,
    geo_cc, geo_netspeed, params_cpu_cnc,params_scr_bpp,params_scr_dpx,params_scr_mtp,params_scr_orn,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd,
    count(*)
    from $localTable
    group by
        user_agent_device_type,
        user_agent_family,
        params_scr_xy,
    geo_cc, geo_netspeed, params_cpu_cnc,params_scr_bpp,params_scr_dpx,params_scr_mtp,params_scr_orn,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd
    order by count(*) desc
    limit 25
    """)
catch y
    println("setupLocalTable Exception ",y)
end

try
    query("""SELECT * FROM $localTable limit 3""")
catch y
    println("setupLocalTable Exception ",y)
end

joinTablesTest = DataFrame()

try
    joinTablesTest = query("""\
    select $tableRt.*
    from $localTable join $tableRt
    on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
    where $tableRt.encoded_size > 1
    limit 3
    """);

    beautifyDF(joinTablesTest[1:min(linesOutput,end),:])
catch y
    println("bigTable5 Exception ",y)
end
#display(joinTables)

joinTables = DataFrame()

try
    joinTables = query("""\
    select
        $tableRt.url as urlgroup,
        $localTable.user_agent_device_type,
        $localTable.user_agent_family as useragentfamily,
        $localTable.params_scr_xy,
        $localTable.session_id,
        $localTable."timestamp",
        sum($tableRt.encoded_size) as encoded,
        sum($tableRt.transferred_size) as transferred,
        sum($tableRt.decoded_size) as decoded,
        count(*)
    from $localTable join $tableRt
    on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
    where $tableRt.encoded_size > 1
    group by $tableRt.url,$localTable.user_agent_device_type,$localTable.user_agent_family,
        $localTable.params_scr_xy,$localTable.session_id,$localTable."timestamp"
    order by encoded desc
    """);

    scrubUrlToPrint(SP,joinTables,:urlgroup)
    beautifyDF(joinTables[1:min(linesOutput,end),:])
catch y
    println("bigTable5 Exception ",y)
end
#display(joinTables)

#topSessionId = joinTables[1:1,:session_id]
#topTimeStamp = joinTables[1:1,:timestamp]
#println("tsi=",topSessionId," tts=", topTimeStamp)

joinTableSummary = DataFrame()
joinTableSummary[:useragentfamily] = "delete"
joinTableSummary[:session_id] = ""
joinTableSummary[:timestamp] = 0
joinTableSummary[:encoded] = 0
joinTableSummary[:transferred] = 0
joinTableSummary[:decoded] = 0
joinTableSummary[:count] = 0

sort!(joinTables,cols=[order(:encoded,rev=true)])
for subDf in groupby(joinTables,:useragentfamily)
    #beautifyDF(subDf[1:1,:])
    i = 1
    for row in eachrow(subDf)
        if (i == 1)
            i +=1
            push!(joinTableSummary,[row[:useragentfamily],row[:session_id],row[:timestamp],row[:encoded],row[:transferred],row[:decoded],row[:count]])
        end
    end
end

i = 1
for x in joinTableSummary[:,:useragentfamily]
    if x == "delete"
        deleterows!(joinTableSummary,i)
    end
    i += 1
end
sort!(joinTableSummary,cols=[order(:encoded,rev=true)])
;


beautifyDF(joinTableSummary[1:min(linesOutput,end),[:useragentfamily,:encoded,:transferred,:decoded]])



function detailsPrint(localTable::ASCIIString,tableRt::ASCIIString,joinTableSummary::DataFrame,row::Int64)
    try
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


function statsTableDF2(table::ASCIIString,productPageGroup::ASCIIString,localUrl::ASCIIString,deviceType::ASCIIString,startTimeMs::Int64, endTimeMs::Int64)
    try
        #println(localUrl)

        localStats = query("""\
        select timers_t_done from $table where
        page_group ilike '$(productPageGroup)' and
        params_u ilike '$(localUrl)' and
        user_agent_device_type ilike '$(deviceType)' and
        "timestamp" between $startTimeMs and $endTimeMs and
        params_rt_quit IS NULL
        """);
        return localStats
    catch y
        println("statsTableCreateDF Exception ",y)
    end
end





function statsDetailsPrint2(localTable::ASCIIString,tableRt::ASCIIString,joinTableSummary::DataFrame,row::Int64)
    try
        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])

        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Desktop",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Mobile",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Tablet",TV.startTimeMsUTC,TV.endTimeMsUTC)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
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
        println("statsTableDF2 Exception ",y)
    end
end


i = 0
for row in eachrow(joinTableSummary)
    i += 1
    detailsPrint(localTable,tableRt,joinTableSummary,i)
    statsDetailsPrint2(localTable,tableRt,joinTableSummary,i)
    if (i >= linesOutput)
        break;
    end
end
;
