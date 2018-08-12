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
#include("Find-A-Page-View-Spike-Body-v1.3.jl")
include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,5,30,7,59,2017,5,30,14,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

bt = UP.beaconTable
rt = UP.resourceTable

toppagecount = select("""\
            select count(*),sessionid,geo_cc, geo_isp, proxy_address,remote_ip,devicetypename
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid IS NOT NULL
                group by sessionid,geo_cc, geo_isp, proxy_address,remote_ip,devicetypename
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

firstSession = (toppagecount[1:1,:sessionid][1])

debugRecords = select("""\
            select *
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
                limit 10
            """);

beautifyDF(debugRecords[1:min(10,end),:])

debugRecords = select("""\
            select count(*), geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,http_referrer
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
group by geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,http_referrer
                order by count(*) desc
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = select("""\
            select count(*), geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,paramsu
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
group by geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,paramsu
                order by count(*) desc
            """);

beautifyDF(debugRecords[1:min(100,end),:])

debugRecords = select("""\
            select timestamp, geo_cc, geo_isp, proxy_address,remote_ip,devicetypename,http_referrer,paramsu
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid = '$(firstSession)'
order by timestamp
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
#                avg($bt.domreadytimer) as beacon_time
#localUrl = "%"
#deviceType = "%"
#st = (TV.startTimeMsUTC)
#et = (TV.endTimeMsUTC)
#println(st," , ",et)

toppagecount = select("""\
            select count(*),sessionid
            FROM $bt
            where
                timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
                and sessionid IS NOT NULL
                group by sessionid
                order by count(*) desc
                """);

beautifyDF(toppagecount[1:min(10,end),:])

#toppagecount = select("""\
#            select count(*),$rt.sessionid
#            FROM $rt join $bt on $rt.sessionid = $bt.sessionid and $rt.timestamp = $bt.timestamp
#            where
#                $rt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
#                and $bt.sessionid IS NOT NULL
#                group by $rt.sessionid
#                order by count(*) desc
#                """);


#toppagecount = select("""\
#            select count(*) as request_count,
#                $btRT.sessionid
#            FROM $rt join $bt on $rt.sessionid = $bt.sessionid and $rt.timestamp = $bt.timestamp
#                where
#                $rt.timestamp between $TV.startTimeMsUTC and $TV.endTimeMsUTC
#                and $bt.sessionid IS NOT NULL
#                and $bt.pagegroupname ilike '$(UP.pageGroup)'
#                and $bt.paramsu ilike '$(localUrl)'
#                and $bt.devicetypename ilike '$(deviceType)'
#                group by sessionid
#                """);

#beautifyDF(toppagecount[1:min(10,end),:])

#firstAndLast()
#sessionsBeacons()
#loadTime()
#topUrls()
#peakTable()
#statsTable()
