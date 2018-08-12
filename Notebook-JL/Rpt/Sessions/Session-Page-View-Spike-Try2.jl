using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

# Packages
include("Find-A-Page-View-Spike-Body-v1.3.jl")
include("../../../Lib/Include-Package.jl")

#TV = timeVariables(2017,6,3,10,59,2017,6,3,11,59)
#TV = weeklyTimeVariables(days=1)
TV = yesterdayTimeVariables()

customer = "Nat Geo"
productPageGroup = "Your Shot" # primary page group
#productPageGroup = "Travel AEM" # primary page group
localTable = "$(table)_$(scriptName)_spike_pview_prod"
localTableRt = "$(tableRt)_spike_pview_prod"

toppagecount = select("""\
            select count(*),sessionid,geo_cc, geo_isp, proxy_address,remote_ip,devicetypename
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid IS NOT NULL
                group by sessionid,geo_cc, geo_isp, proxy_address,remote_ip,devicetypename
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

firstSession = (toppagecount[1:1,:sessionid][1])
println(firstSession)

debugRecords = select("""\
            select *
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
                limit 10
            """);

beautifyDF(debugRecords[1:min(10,end),:])

debugRecords = select("""\
            select timestamp, geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,http_referrer
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
order by timestamp
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = select("""\
            select timestamp, geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,paramsu
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
                order by timestamp
            """);

beautifyDF(debugRecords[1:min(300,end),:])

debugRecords = select("""\
            select timestamp, geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,http_referrer,paramsu
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
                order by timestamp
            """);

beautifyDF(debugRecords[1:min(300,end),:])

#                avg($tableRt.startTime),
#                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.startTime) END) as total,
#                avg($tableRt.redirect_end-$tableRt.redirect_start) as redirect,
#                avg(CASE WHEN ($tableRt.dns_start = 0 and $tableRt.request_start = 0) THEN (0) WHEN ($tableRt.dns_start = 0) THEN ($tableRt.request_start-$tableRt.fetch_start) ELSE ($tableRt.dns_start-$tableRt.fetch_start) END) as blocking,
#                avg($tableRt.dns_end-$tableRt.dns_start) as dns,
#                avg($tableRt.tcp_connection_end-$tableRt.tcp_connection_start) as tcp,
#                avg($tableRt.response_first_byte-$tableRt.request_start) as request,
#                avg(CASE WHEN ($tableRt.response_first_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.response_first_byte) END) as response,
#              avg(0) as gap,
#              avg(0) as critical,
#               CASE WHEN (position('?' in $tableRt.url) > 0) then trim('/' from (substring($tableRt.url for position('?' in substring($tableRt.url from 9)) +7))) else trim('/' from $tableRt.url) end as urlgroup,
#                count(*) as request_count,
#                'Label' as label,
#                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE (($tableRt.response_last_byte-$tableRt.startTime)/1000.0) END) as load,
#                avg($table.domreadytimer) as beacon_time
#localUrl = "%"
#deviceType = "%"
#st = (TV.startTimeMsUTC)
#et = (TV.endTimeMsUTC)
#println(st," , ",et)

toppagecount = select("""\
            select count(*),sessionid
            FROM $table
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid IS NOT NULL
                group by sessionid
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

#toppagecount = select("""\
#            select count(*),$tableRt.sessionid
#            FROM $tableRt join $table on $tableRt.sessionid = $table.sessionid and $tableRt.timestamp = $table.timestamp
#            where
#                $tableRt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $table.sessionid IS NOT NULL
#                group by $tableRt.sessionid
#                order by count(*) desc
#                """);


#toppagecount = select("""\
#            select count(*) as request_count,
#                $tableRT.sessionid
#            FROM $tableRt join $table on $tableRt.sessionid = $table.sessionid and $tableRt.timestamp = $table.timestamp
#                where
#                $tableRt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $table.sessionid IS NOT NULL
#                and $table.pagegroupname ilike '$(productPageGroup)'
#                and $table.paramsu ilike '$(localUrl)'
#                and $table.devicetypename ilike '$(deviceType)'
#                group by sessionid
#                """);

#beautifyDF(toppagecount[1:min(10,end),:])

firstAndLast()

sessionsBeacons()

loadTime()
topUrls()
peakTable()
statsTable()

q = select(""" drop view if exists $localTable;""")
q = select(""" drop view if exists $localTableRt;""")
;
