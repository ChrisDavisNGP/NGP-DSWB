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
#include("Find-A-Page-View-Spike-Body-v1.3.jl")
include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,5,30,7,59,2017,5,30,14,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

bt = UP.beaconTable
rt = UP.resourceTable

toppagecount = query("""\
            select
            count(*),session_id,geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id IS NOT NULL
                group by session_id,geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

firstSession = (toppagecount[1:1,:session_id][1])

debugRecords = query("""\
            select
            *
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
                limit 10
            """);

beautifyDF(debugRecords[1:min(10,end),:])

debugRecords = query("""\
            select
count(*), geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,http_referrer
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
group by geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,http_referrer
                order by count(*) desc
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = query("""\
            select
count(*), geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,params_u
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
group by geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,params_u
                order by count(*) desc
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = query("""\
            select
"timestamp", geo_cc, geo_isp, proxy_address,remote_ip,user_agent_device_type,http_referrer,params_u
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id = '$(firstSession)'
order by "timestamp"
            """);

beautifyDF(debugRecords[1:min(100,end),:])

#                avg($rt.start_time),
#                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.start_time) END) as total,
#                avg($rt.redirect_end-$rt.redirect_start) as redirect,
#                avg(CASE WHEN ($rt.dns_start = 0 and $rt.request_start = 0) THEN (0) WHEN ($rt.dns_start = 0) THEN ($rt.request_start-$rt.fetch_start) ELSE ($rt.dns_start-$rt.fetch_start) END) as blocking,
#                avg($rt.dns_end-$rt.dns_start) as dns,
#                avg($rt.tcp_connection_end-$rt.tcp_connection_start) as tcp,
#                avg($rt.response_first_byte-$rt.request_start) as request,
#                avg(CASE WHEN ($rt.response_first_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.response_first_byte) END) as response,
#              avg(0) as gap,
#              avg(0) as critical,
#               CASE WHEN (position('?' in $rt.url) > 0) then trim('/' from (substring($rt.url for position('?' in substring($rt.url from 9)) +7))) else trim('/' from $rt.url) end as urlgroup,
#                count(*) as request_count,
#                'Label' as label,
#                avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE (($rt.response_last_byte-$rt.start_time)/1000.0) END) as load,
#                avg($bt.timers_domready) as beacon_time
#localUrl = "%"
#deviceType = "%"
#st = (TV.startTimeMsUTC)
#et = (TV.endTimeMsUTC)
#println(st," , ",et)

toppagecount = query("""\
            select
            count(*),session_id
            FROM $bt
            where
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and session_id IS NOT NULL
                group by session_id
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

#toppagecount = query("""\
#            select
#            count(*),$rt.session_id
#            FROM $rt join $bt on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
#            where
#                $rt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $bt.session_id IS NOT NULL
#                group by $rt.session_id
#                order by count(*) desc
#                """);


#toppagecount = query("""\
#            select
#            count(*) as request_count,
#                $btRT.session_id
#            FROM $rt join $bt on $rt.session_id = $bt.session_id and $rt."timestamp" = $bt."timestamp"
#                where
#                $rt."timestamp" between $TV.startTimeMsUTC and $TV.endTimeMsUTC
#                and $bt.session_id IS NOT NULL
#                and $bt.page_group ilike '$(UP.pageGroup)'
#                and $bt.params_u ilike '$(localUrl)'
#                and $bt.user_agent_device_type ilike '$(deviceType)'
#                group by session_id
#                """);

#beautifyDF(toppagecount[1:min(10,end),:])

#firstAndLast()
#sessionsBeacons()
#loadTime()
#topUrls()
#peakTable()
#statsTable()
