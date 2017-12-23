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
include("../../../lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,4,21,10,0,2017,4,21,10,9);
#TV = weeklyTimeVariables(days=7);
TV = yesterdayTimeVariables()

include("RD-By-Page-Body-v1.0.jl")
customer = "Nat Geo"
#productPageGroup = "%"
productPageGroup = "%" # primary page group
localUrl = "%"
resourceUrl = "%ng-black-logo.ngsversion%"

localTable = "$(table)_$(scriptName)_Find_Resource_Details"
linesOutput = 25
minimumEncoded = 0
;

# Resource and it parent is also interesting below this point

try
    query("""\
        create or replace view $localTable as (
            select * from $table
                where
                    "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                    page_group ilike '$(productPageGroup)' and
                    params_u ilike '$(localUrl)'
        )
    """)
    cnt = query("""SELECT count(*) FROM $localTable""")
    println("$localTable count is ",cnt[1,1])
catch y
    println("setupLocalTable Exception ",y)
end


resourceMatched(tableRt;linesOut=3)
;

resourceSummaryAllFields(tableRt;linesOut=3)
;

resourceSummary(tableRt;linesOut=linesOutput)
;

resourceSize(tableRt;linesOut=linesOutput,minEncoded=minimumEncoded)
;

resourceScreen(tableRt;linesOut=linesOutput)
;

resourceSummaryDomainUrl(tableRt;linesOut=linesOutput)
;




function resourceTime1(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        count(*),
        avg(start_time) as "Start Time",
        avg(fetch_start) as "Fetch Start",
        avg(dns_end-dns_start) as "DNS ms",
        avg(tcp_connection_end-tcp_connection_start) as "TCP ms",
        avg(request_start) as "Req Start",
        avg(response_first_byte) as "Req FB",
        avg(response_last_byte) as "Req LB",
        max(response_last_byte) as "Max Req LB",
        url,
        avg(redirect_start) as "Redirect Start",
        avg(redirect_end) as "Redirect End",
        avg(secure_connection_start) as "Secure Conn Start"
        from $tableRt
        where
          url ilike '$resourceUrl' and
          "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
        group by url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(resourceUrl)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end


resourceTime1(tableRt;linesOut=linesOutput)
;

function resourceTime2(tableRt::ASCIIString;linesOut::Int64=25)

    try
        timeTable = query("""\
        select
        (response_last_byte-start_time) as "Time Taken",
        (start_time) as "Start Time",
        (fetch_start) as "Fetch Start",
        (dns_end-dns_start) as "DNS ms",
        (tcp_connection_end-tcp_connection_start) as "TCP ms",
        (request_start) as "Req Start",
        (response_first_byte) as "Req FB",
        (response_last_byte) as "Req LB",
        url,
        (redirect_start) as "Redirect Start",
        (redirect_end) as "Redirect End",
        (secure_connection_start) as "Secure Conn Start"
        from $tableRt
        where
          url ilike '$resourceUrl' and
        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
        (response_last_byte-start_time) > 75 and (response_last_byte-start_time) < 10000
        order by "Time Taken" desc
        """);

        #todo remove negitives

        displayTitle(chart_title = "Raw Resource Url Pattern $(resourceUrl)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(timeTable[1:min(linesOut,end),:])

        timeTable = names!(timeTable[:,:],
        [symbol("taken"),symbol("start"),symbol("fetch"),symbol("dns"),symbol("tcp"),symbol("req_start"),symbol("req_fb"),symbol("req_lb"),symbol("url")
            ,symbol("redirect_start"),symbol("redirect_end"),symbol("secure_conn_start")])

        dv1 = timeTable[:taken]
        statsDF1 = limitedStatsFromDV(dv1)
        showLimitedStats(TV,statsDF1,"Time Taken Stats")

        dv2 = timeTable[:dns]
        statsDF2 = limitedStatsFromDV(dv2)
        showLimitedStats(TV,statsDF2,"DNS Stats")

        dv3 = timeTable[:tcp]
        statsDF3 = limitedStatsFromDV(dv3)
        showLimitedStats(TV,statsDF3,"TCP Stats")

        dv4 = timeTable[:start]
        statsDF4 = limitedStatsFromDV(dv4)
        showLimitedStats(TV,statsDF4,"Start Time On Page Stats")

        dv5 = timeTable[:fetch]
        statsDF5 = limitedStatsFromDV(dv5)
        showLimitedStats(TV,statsDF5,"Fetching Request Stats")

        dv6 = timeTable[:req_start]
        statsDF6 = limitedStatsFromDV(dv6)
        showLimitedStats(TV,statsDF6,"Request Start Stats")

        dv7 = timeTable[:req_fb]
        statsDF7 = limitedStatsFromDV(dv7)
        showLimitedStats(TV,statsDF7,"Request First Byte Stats")

        dv8 = timeTable[:req_lb]
        statsDF8 = limitedStatsFromDV(dv8)
        showLimitedStats(TV,statsDF8,"Request Last Byte Stats")




    catch y
        println("bigTable5 Exception ",y)
    end
end


resourceTime2(tableRt;linesOut=linesOutput)
;

function resourceTime3(tableRt::ASCIIString;linesOut::Int64=25)

    try
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
          url ilike '$resourceUrl' and
        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
        start_time > 10000
        order by start_time desc
        limit 25
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(resourceUrl)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end


resourceTime3(tableRt;linesOut=linesOutput)
;
