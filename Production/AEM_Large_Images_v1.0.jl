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
#include("/data/notebook_home/Production/Bodies/Page-Group-Details-Body-v1.0.jl")
include("/data/notebook_home/Production/Lib/Include-Package-v1.0.jl")

# Time values (tv.) structure created in include above, so init time here
#timeVariables(2017,1,2,2,30,2017,1,2,2,45);
#timeVariables(2017,1,9,0,0,2017,1,9,23,59);
weeklyTimeVariables(days=1);

customer = "Nat Geo"
productPageGroup = "%" # primary page group
localUrl = "%"
localTable = "$(table)_AEM_Large_Images_view"
linesOutput = 100
;

try
    query("""\
        create or replace view $localTable as (
            select * from $table
                where
                    "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and
                    page_group in ('News Articles','Travel AEM','Photography AEM','Nat Geo Homepage','Magazine','Magazine Homepage') and
                    params_u ilike '$(localUrl)' and
                    user_agent_device_type = 'Mobile'
        )
    """)
    cnt = query("""SELECT count(*) FROM $localTable""")
    println("$localTable count is ",cnt[1,1])
catch y
    println("setupLocalTable Exception ",y)
end

joinTables = DataFrame()

try
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
    where $tableRt.encoded_size > 1 and
    $tableRt.url not like '%/interactive-assets/%'
    group by $localTable.params_u,$localTable.session_id,$localTable."timestamp"
    order by encoded desc
    """);

    #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
    scrubUrlToPrint(joinTables,limit=150)
    beautifyDF(joinTables[1:min(linesOutput,end),:])
catch y
    println("bigTable5 Exception ",y)
end
#display(joinTables)


#topSessionId = joinTables[1:1,:session_id]
#topTimeStamp = joinTables[1:1,:timestamp]
#println("tsi=",topSessionId," tts=", topTimeStamp)

joinTableSummary = DataFrame()
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
;




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
        $tableRt.encoded_size > 1000000 and
        $tableRt.url not like '%/interactive-assets/%'
        order by $tableRt.start_time
        """);

        displayTitle(chart_title = "Large Requests for: $(topTitle)", chart_info = [tv.timeString], showTimeStamp=false)
        scrubUrlToPrint(joinTablesDetails,limit=250)
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
        println("statsTableDF Exception ",y)
    end
end



function statsDetailsPrint(localTable::ASCIIString,tableRt::ASCIIString,joinTableSummary::DataFrame,row::Int64)
    try
        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])

        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Desktop",tv.startTimeMsUTC,tv.endTimeMsUTC)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Mobile",tv.startTimeMsUTC,tv.endTimeMsUTC)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        statsFullDF2 = statsTableDF2(localTable,productPageGroup,topUrl,"Tablet",tv.startTimeMsUTC,tv.endTimeMsUTC)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2,productPageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
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

i = 0
for row in eachrow(joinTableSummary)
    i += 1
    detailsPrint(localTable,tableRt,joinTableSummary,i)
    statsDetailsPrint(localTable,tableRt,joinTableSummary,i)
    if (i >= linesOutput)
        break;
    end
end
;
