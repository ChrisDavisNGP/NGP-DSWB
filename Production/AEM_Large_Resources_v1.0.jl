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
weeklyTimeVariables(days=7);

customer = "Nat Geo"
productPageGroup = "%" # primary page group
localUrl = "%"
localTable = "$(table)_AEM_Large_Resources_view"
linesOutput = 250
minimumEncoded = 100000
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

#
#    tp = query("""\
#    select
#$localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
#$localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
#$localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd
#    from $localTable join $tableRt
#    on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
#    where $tableRt.encoded_size > 1 and
#    $tableRt.url not like '%/interactive-assets/%'
#    limit 3
#    """);
#display(tp)


#
#    tq = query("""\
#    select
#$tableRt.url,
#initiator_type,height,width,x,y
#    from $localTable join $tableRt
#    on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
#    where $tableRt.encoded_size > 1 and
#    $tableRt.url not like '%/interactive-assets/%'
#    limit 3
#    """);
#display(tq)

function resourceSummary(localTable::ASCIIString,tableRt::ASCIIString,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
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
        $tableRt.url not like '%/interactive-assets/%' and $tableRt.url ilike '$(fileType)'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os
        order by encoded desc, transferred desc, decoded desc
        """);

        displayTitle(chart_title = "Mobile Big Pages Details (Min $(minEncoded) KB), File Type $(fileType)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSizes2(localTable::ASCIIString,tableRt::ASCIIString,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
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

        #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSizes12(localTable::ASCIIString,tableRt::ASCIIString,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
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

        #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

fileType= "%jpg"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;

fileType= "%png"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%png";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%svg"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%svg";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%mp3"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%mp4";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%mp4"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%mp4";linesOut=linesOutput,minEncoded=minimumEncoded)


fileType= "%gif"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%gif";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%wav"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%wav";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%jog"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%jog";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%js"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%js";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%.js?%"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%.js?%";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%css"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%css";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%ttf"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%css";linesOut=linesOutput,minEncoded=minimumEncoded)

fileType= "%woff%"
resourceSummary(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
resourceSizes2(localTable,tableRt,fileType;linesOut=linesOutput,minEncoded=minimumEncoded)
;
#resourceSizes(localTable,tableRt,"%css";linesOut=linesOutput,minEncoded=minimumEncoded)

joinTables = DataFrame()

try
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

    #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
    #scrubUrlToPrint(joinTables,limit=150)
    beautifyDF(joinTables[1:min(linesOutput,end),:])
catch y
    println("bigTable5 Exception ",y)
end
#display(joinTables)

joinTables = DataFrame()

try
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
    where $tableRt.encoded_size > 1 and
    $tableRt.url not like '%/interactive-assets/%'
    group by $tableRt.url,
$localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
$localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
$localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd,
    $tableRt.initiator_type,$tableRt.height,$tableRt.width,$tableRt.x,$tableRt.y
    order by encoded desc
    """);

    #displayTitle(chart_title = "Big Pages Details (Min $(minSize) KB)", chart_info = [tv.timeString], showTimeStamp=false)
    #scrubUrlToPrint(joinTables,limit=150)
    beautifyDF(joinTables[1:min(linesOutput,end),:])
catch y
    println("bigTable5 Exception ",y)
end
#display(joinTables)