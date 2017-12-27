using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

customer = "Nat Geo"
productPageGroup = "Nat Geo Homepage" # primary page group
localTable = "$(table)_$(scriptName)_DOC_view"


firstAndLast = getBeaconsFirstAndLast()
endTime = DateTime(firstAndLast[1,2])
startTime = DateTime(endTime - Day(6))
startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

#graphics covering 24 hours or less can use minutes, otherwise try :hours or :days or :seconds depending on how wide the time range is
datePart = :minute
timeString = "$(padDateTime(startTime)) to $(padDateTime(endTime))"

#st = DateTime(2016,7,25,12,30)
#et = DateTime(2016,7,25,13,30)
#startTime = datetimeToUTC(st, TimeZone("America/New York"))
#endTime = datetimeToUTC(et, TimeZone("America/New York"))
#timeString = "$(padDateTime(startTime)) to $(padDateTime(endTime))"

firstAndLast = getBeaconsFirstAndLast()
println(startTime, " (",startTimeMs,")",   ", ", endTime, " (",endTimeMs,")")

query("""create or replace view $localTable as (select * from $table where page_group = '$(productPageGroup)' and "timestamp" between $startTimeMs and $endTimeMs)""")

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
cnt = query("""SELECT count(*) FROM $localTable""")
#Hide output from final report
println("$localTable count is ",cnt[1,1])

try

    displayTitle(chart_title = "Big Pages Treemap Report (Min 3MB Pages)", chart_info = [timeString], showTimeStamp=false)
    domSize = query("""\
    select count(*),AVG(params_dom_sz) beacons,
    AVG(timers_t_page)/1000 load_time,
    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup
    from $localTable
    where params_dom_sz IS NOT NULL
    and params_dom_sz > 3000000
    group by urlgroup
    order by beacons desc
    limit 25
    """);

    beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views");Symbol("Avg Size");Symbol("Avg Load Time (sec)");Symbol("URL Group")]))

    fieldNames = [:urlgroup]
    domSize[:x1] = "URLs Total Size"
    drawTree(domSize;fieldNames=fieldNames)
catch y
    println("urlTotalSizeTreemap Exception ",y)
end

try
    displayTitle(chart_title = "Total Bytes Used (Size x Views) Treemap Report (Min 2 MB Pages)", chart_info = [timeString], showTimeStamp=false)
    domSize = query("""\
    --select count(*),AVG(params_dom_sz) beacons,
    select count(*),SUM(params_dom_sz) beacons,
    AVG(timers_t_page)/1000 load_time,
    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup
    from $localTable
    where params_dom_sz IS NOT NULL
    and params_dom_sz > 2000000
    group by urlgroup
    order by beacons desc
    limit 25
    """);

    #display(names!(domSize[1:end,[1:4]],[Symbol("Page Views"),Symbol("Total Size"),Symbol("Avg Load Time(sec)"),Symbol("URL Group")]))
    beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views");Symbol("Total Size");Symbol("Avg Load Time (sec)");Symbol("URL Group")]))
    #display(domSize[1:end,:])

    fieldNames = [:urlgroup]
    domSize[:x1] = "URLs Total Size"
    drawTree(domSize;fieldNames=fieldNames)
catch y
    println("urlTotalSizeTreemap Exception ",y)
end

try
    displayTitle(chart_title = "Unique Domains Used", chart_info = [timeString], showTimeStamp=false)

    domSize = query("""\
    select count(*),AVG(params_dom_doms) avgsize,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_doms > 50
    and params_dom_doms IS NOT NULL
    group by urlgroup
    order by avgsize desc
    limit 25
    """);
    beautifyDF(names!(domSize[1:end,[1:3]],[Symbol("Views"),Symbol("Avg Domains"),Symbol("URL Group")]))
catch y
    println("uniqueDomainsUsed Exception ",y)
end

#displayTitle(chart_title = "Friequent Unique Domains Used (50 dom min)", chart_info = [timeString], showTimeStamp=false)

#domSize = query("""\
#select count(*) cnt,SUM(params_dom_doms) avgsize,
#CASE
#when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
#else trim('/' from params_u)
#end urlgroup
#from $localTable
#where params_dom_doms > 50
#and params_dom_doms IS NOT NULL
#group by urlgroup
#order by cnt desc
#limit 15
#""");
#beautifyDF(names!(domSize[1:end,[1:3]],[Symbol("Views"),Symbol("Total Domains"),Symbol("URL Group")]))

try
    displayTitle(chart_title = "Domains Nodes On Page (20k min)", chart_info = [timeString], showTimeStamp=false)

    domSize = query("""\
    select count(*),AVG(params_dom_ln) avgsize,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_ln > 20000
    and params_dom_ln IS NOT NULL
    group by urlgroup
    order by avgsize desc
    limit 25
    """);

    beautifyDF(names!(domSize[1:end,[1:3]],[Symbol("Views"),Symbol("Avg Nodes"),Symbol("URL Group")]))
catch y
    println("domainNodesOnPage Exception ",y)
end

#displayTitle(chart_title = "Domains Resource in RT", chart_info = [timeString], showTimeStamp=false)

#domSize = query("""\
#select count(*) cnt,AVG(params_dom_res) avgsize,page_group,params_u
#from $localTable
#where params_dom_res > 100
#and params_dom_res IS NOT NULL
#group by page_group,params_u
#order by avgsize desc
#limit 25
#""");
#beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Resources"),Symbol("Page Group"),Symbol("URL")]))

#displayTitle(chart_title = "Frequent High Resource in RT", chart_info = [timeString], showTimeStamp=false)

#domSize = query("""\
#select count(*) cnt,AVG(params_dom_res) avgsize,page_group,params_u
#from $localTable
#where params_dom_res > 400
#and params_dom_res IS NOT NULL
#group by page_group,params_u
#order by cnt desc
#limit 25
#""");
#beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Resources"),Symbol("Page Group"),Symbol("URL")]))

try
    displayTitle(chart_title = "Domains Images", chart_info = [timeString], showTimeStamp=false)

    domSize = query("""\
    select count(*) cnt,AVG(params_dom_img) avgsize,AVG(params_dom_img_ext) avgsizeext,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_img > 100
    and params_dom_img IS NOT NULL and params_dom_img_ext IS NOT NULL
    group by urlgroup
    order by avgsize desc
    limit 25
    """);
    beautifyDF(names!(domSize[:,[1:4]],[Symbol("Views"),Symbol("Avg Images"),Symbol("Avg Images External"),Symbol("URL Group")]))
catch y
    println("domainsImages Exception ",y)
end

try
    displayTitle(chart_title = "Frequently Used Images", chart_info = [timeString], showTimeStamp=false)

    domSize = query("""\
    select count(*) cnt,SUM(params_dom_img) avgsize,SUM(params_dom_img_ext) avgsizeext,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_img > 500
    and params_dom_img IS NOT NULL and params_dom_img_ext IS NOT NULL
    group by urlgroup
    order by CNT desc
    limit 25
    """);
    beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Sum Images"),Symbol("Sum Images External"),Symbol("URL Group")]))
catch y
    println("frequentlyUsedImages Exception ",y)
end

try
    displayTitle(chart_title = "Domains Scripts", chart_info = [timeString], showTimeStamp=false)

    #params_dom_img,params_dom_img_ext,
    #params_dom_script,params_dom_script_ext,

    domSize = query("""\
    select count(*) cnt,AVG(params_dom_script) avgsize,AVG(params_dom_script_ext) avgsizeext,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_script > 100
    and params_dom_script IS NOT NULL and params_dom_script_ext IS NOT NULL
    group by urlgroup
    order by avgsize desc
    limit 25
    """);
    beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Scripts"),Symbol("Avg Scripts External"),Symbol("URL Group")]))
catch y
    println("domainScripts Exception ",y)
end

try
    displayTitle(chart_title = "Frequently Used Scripts", chart_info = [timeString], showTimeStamp=false)

    #params_dom_img,params_dom_img_ext,
    #params_dom_script,params_dom_script_ext,

    domSize = query("""\
    select count(*) cnt,SUM(params_dom_script) avgsize,SUM(params_dom_script_ext) avgsizeext,

    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup

    from $localTable
    where params_dom_script > 200
    and params_dom_script IS NOT NULL and params_dom_script_ext IS NOT NULL
    group by urlgroup
    order by cnt desc
    limit 25
    """);
    beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Total Scripts"),Symbol("Total Scripts External"),Symbol("URL Group")]))
catch y
    println("frequentlyUsedScripts Exception ",y)
end

sizeTrend = DataFrame()

try
    #displayTitle(chart_title = "Big Pages Treemap Report (Min 3MB Pages)", chart_info = [timeString], showTimeStamp=false)

    sizeTrend = query("""\
    select
    params_h_t,
    params_dom_sz size,
    CASE
    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
    else trim('/' from params_u)
    end urlgroup
    --,*
    from $localTable
    where params_dom_sz IS NOT NULL
    --and params_u = 'http://news.nationalgeographic.com'
    -- group by urlgroup
    --order by beacons desc
    limit 250
    """);

    #beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Size"),Symbol("Avg Load Time (sec)"),Symbol("URL Group")]))
    sizeof(sizeTrend[1:end,:])
catch y
    println("setupSizeTrend Exception ",y)
end

try
    delete!(sizeTrend,[:urlgroup])
catch y
    println("cleanupSizeTrend Exception ",y)
end

try
    dataNames = ["Dom Byte Size"]
    drawC3Viz(sizeTrend, dataNames=dataNames);
catch y
    println("graphSizeTrend Exception ",y)
end
