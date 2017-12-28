using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

# Packages
include("Find-A-Page-View-Spike-Body-v1.3.jl")
include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,6,3,10,59,2017,6,3,11,59)
#TV = weeklyTimeVariables(days=1)
TV = yesterdayTimeVariables()

customer = "Nat Geo"
productPageGroup = "Your Shot" # primary page group
#productPageGroup = "Travel AEM" # primary page group
localTable = "$(table)_$(scriptName)_spike_pview_prod"
localTableRt = "$(tableRt)_spike_pview_prod"

toppagecount = query("""\
            select count(*),session_id,geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id IS NOT NULL
                group by session_id,geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

firstSession = (toppagecount[1:1,:session_id][1])
println(firstSession)

debugRecords = query("""\
            select *
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
                limit 10
            """);

beautifyDF(debugRecords[1:min(10,end),:])

debugRecords = query("""\
            select "timestamp", geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,http_referrer
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
order by "timestamp"
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = query("""\
            select "timestamp", geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,params_u
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
                order by "timestamp"
            """);

beautifyDF(debugRecords[1:min(300,end),:])

debugRecords = query("""\
            select "timestamp", geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,http_referrer,params_u
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
                order by "timestamp"
            """);

beautifyDF(debugRecords[1:min(300,end),:])

#                avg($tableRt.start_time),
#                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.start_time) END) as total,
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
#                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE (($tableRt.response_last_byte-$tableRt.start_time)/1000.0) END) as load,
#                avg($table.timers_domready) as beacon_time
#localUrl = "%"
#deviceType = "%"
#st = (TV.startTimeMsUTC)
#et = (TV.endTimeMsUTC)
#println(st," , ",et)

toppagecount = query("""\
            select count(*),session_id
            FROM $table
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id IS NOT NULL
                group by session_id
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

#toppagecount = query("""\
#            select count(*),$tableRt.session_id
#            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
#            where
#                $tableRt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $table.session_id IS NOT NULL
#                group by $tableRt.session_id
#                order by count(*) desc
#                """);


#toppagecount = query("""\
#            select count(*) as request_count,
#                $tableRT.session_id
#            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
#                where
#                $tableRt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $table.session_id IS NOT NULL
#                and $table.page_group ilike '$(productPageGroup)'
#                and $table.params_u ilike '$(localUrl)'
#                and $table.user_agent_device_type ilike '$(deviceType)'
#                group by session_id
#                """);

#beautifyDF(toppagecount[1:min(10,end),:])

firstAndLast()

sessionsBeacons()

loadTime()
topUrls()
peakTable()
statsTable()

q = query(""" drop view if exists $localTable;""")
q = query(""" drop view if exists $localTableRt;""")
;
