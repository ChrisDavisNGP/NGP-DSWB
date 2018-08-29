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

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UP.pageGroup = "Nat Geo Homepage"
UP.urlRegEx = "%www.nationalgeographic.com/"
UP.urlFull = "https://www.nationalgeographic.com/"
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

defaultBeaconCreateView(TV,UP,SP)

btv = UP.btView

displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)

t1DF = select("""\

select count(*),paramsu
FROM $btv
where
beacontypename = 'page view'
group by paramsu
order by count(*) desc
limit 5

""")
beautifyDF(t1DF)

displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)

t2DF = select("""\

select count(*),sessionid,paramsu
FROM $btv
where
beacontypename = 'page view' and
paramsu ilike '$(UP.urlRegEx)'
group by paramsu,sessionid
order by count(*) desc
""")
beautifyDF(t2DF)

sessionid = "ad2fd687-691f-4764-a9bb-2182db03634e-oho76h"

displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)

t3DF = select("""\

select count(*),sessionid,paramsu,timestamp
FROM $btv
where
beacontypename = 'page view' and
sessionid = '$sessionid'
group by paramsu,sessionid,timestamp
order by timestamp asc
""")
beautifyDF(t3DF)


ts = "('1482106711154','1482106711161','1482107709775')";

rtv = UP.rtView

select("""drop view if exists $rtv""")



#select("""create or replace view $rtv as (select * from $tableRt where timestamp between $startTimeMs and $endTimeMs and (url ilike '$(localUrlRt)' or paramsu ilike '$(localUrlRt)'))""")
select("""create or replace view $rtv as (select * from $tableRt where timestamp between $startTimeMs and $endTimeMs and sessionid = '$sessionid') limit 10000""")

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacontypename = 'page view'
cnt = select("""SELECT count(*) FROM $rtv""")
#Hide output from final report
println("$rtv count is ",cnt[1,1])

#DF Select cnt btv where sessionid, timestamp
sBeacon = select("""\
select count(*)
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
""")
display(sBeacon)

#DF Select cnt rtv where sessionid, timestamp
sRt = select("""\
select count(*)
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
""")

#methods(beautifyDF)
#methodswith(DataArray)
#describe(tlfields)

#tlfields[:, Bool[ismatch(r"^session_", x) for x in tlfields[:column]]]
#tlfields[:, :Bool[ismatch(r"^session_", x) for x in tlfields[1:1,:]]]
#beautifyDF(tlfields[1:1,:sessionid])

#println(tlfields[1:1,:sessionid])
#println(tlfields[1:1,:session_isunload])
#println(tlfields[1:1,:session_latest])
#println(tlfields[1:1,:session_obopages])
#println(tlfields[1:1,:session_pages])
#println(tlfields[1:1,:session_start])
#println(tlfields[1:1,:session_totalloadtime])

displayTitle(chart_title = "Top Level Fields (1) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select domain,timestamp,key,http_method,http_referrer,http_version,site_version,url
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Top Level Fields (2) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select beacontypename,pagegroupname,remote_ip,proxy_address,
errors,warnings,spdy,ssl,ipv6,
mobile_connection_type,compression_types,ab_test
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Session from Beacon", showTimeStamp=false)
sessionFields = select("""\
select sessionid,session_start,session_latest,session_obopages,session_pages,session_totalloadtime,session_isunload
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "User Agent Fields from Beacon", showTimeStamp=false)
sessionFields = select("""\
select useragentname,useragentversion,user_agent_minor,user_agent_mobile,user_agent_model,operatingsystemname,operatingsystemversion,
user_agent_manufacturer,devicetypename,user_agent_isp,params_ua_plt,params_ua_vnd
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "User Agent Fields Raw from Beacon", showTimeStamp=false)
sessionFields = select("""\
select user_agent_raw
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Geo from Beacon", showTimeStamp=false)
sessionFields = select("""\
select countrycode,geo_city,geo_lat,geo_lon,geo_netspeed,geo_org,geo_postalcode,regioncode,geo_isp
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Bandwidth from Beacon", showTimeStamp=false)
sessionFields = select("""\
select bandwidth_kbps,bandwidth_error_pc,bandwidth_block
from $btv
where
sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
--and bandwidth_kbps IS NOT NULL
""")

displayTitle(chart_title = "Timers (T) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select timers_t_resp,timers_t_page,pageloadtime,timers_t_domloaded,timers_t_configfb,timers_t_configjs,
timers_t_load,timers_t_prerender,timers_t_postrender
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""");
display(sessionFields)
println("times_t_resp = Beacon Back End")
println("times_t_page = Beacon Front End")
println("times_t_done = Beacon Page Load")

displayTitle(chart_title = "Timers (boomr) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select timers_boomr_ld,timers_boomr_fb,timers_boomr_lat,timers_boomerang,timers_fb_to_boomr,timers_navst_to_boomr,timers_boomr_to_end
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Timers from Beacon", showTimeStamp=false)
sessionFields = select("""\
select timers_before_dns,timers_dns,timers_tcp,timers_ssl,timers_domload,domreadytimer,timers_renderstart,timers_loaded,timers_missing
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""");
display(sessionFields)
println("timers_dns = DNS")
println("timers_tcp = TCP")
println("timers_ssl = SSL")
println("timers_domload = DOM Loading")
println("domreadytimer = DOM Complete")
println("timers_renderstart = First Paint")
println("timers_loaded = nt_load_end - nt_nav_st")

displayTitle(chart_title = "Timers from Beacon", showTimeStamp=false)
sessionFields = select("""\
select timers_custom0,timers_custom1,timers_custom2,timers_custom3,timers_custom4,timers_custom5,timers_custom6,timers_custom7,timers_custom8,timers_custom9
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""");
display(sessionFields)
println("timers_custom3 = Moat Ad")
println("timers_custom4 = DoubleClick")
println("timers_custom5 = JS")
println("timers_custom6 = CSS")
println("timers_custom7 = JPG")

displayTitle(chart_title = "Custom from Beacon", showTimeStamp=false)
sessionFields = select("""\
select custom_metrics_0,custom_metrics_1,custom_metrics_2,custom_metrics_3,custom_metrics_4,custom_metrics_5,custom_metrics_6,custom_metrics_7,custom_metrics_8,custom_metrics_9
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "CloudFlare Headers from Beacon", showTimeStamp=false)
sessionFields = select("""\
select headers_cf_visitor,headers_cf_ray,headers_cf_connecting_ip,headers_x_forwarded_for,headers_x_forwarded_proto
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "HTTP Headers from Beacon", showTimeStamp=false)
sessionFields = select("""\
select headers_connection,headers_host,headers_accept_encoding,headers_accept_language,headers_accept,headers_content_length,headers_various
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Navigation Timing (1) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_nt_nav_type,params_nt_red_cnt,params_nt_spdy,params_nt_cinf
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Navigation Timing (2) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_nt_nav_st,params_nt_red_st,params_nt_red_end,(params_nt_red_end-params_nt_red_st) red_delta,
params_nt_fet_st,params_nt_dns_st,params_nt_dns_end,(params_nt_dns_end-params_nt_dns_st) dns_delta,
params_nt_con_st,params_nt_ssl_st,params_nt_con_end,(params_nt_con_end-params_nt_con_st) con_delta
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Navigation Timing (3) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_nt_req_st,params_nt_res_st,params_nt_unload_st,params_nt_unload_end,
params_nt_first_paint,params_nt_domloading,
params_nt_res_end
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Navigation Timing (4) from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_nt_domint,params_nt_domcontloaded_st,params_nt_domcontloaded_e,params_nt_domcomp,
params_nt_load_st,params_nt_load_end
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Boomerang Debug Info from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_rt_bmr_conen,params_rt_bmr_const,params_rt_bmr_domen,params_rt_bmr_domst,params_rt_bmr_fetst,params_rt_bmr_reqst,params_rt_bmr_resen,params_rt_bmr_resst,params_rt_bmr_secst
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Config.js Debug Info from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_rt_cnf_conen,params_rt_cnf_const,params_rt_cnf_domen,params_rt_cnf_domst,params_rt_cnf_fetst,params_rt_cnf_reqst,params_rt_cnf_resen,params_rt_cnf_resst,params_rt_cnf_secst
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Debug Info from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_rt_abld,params_rt_blstart,params_rt_bstart,params_rt_cstart,params_rt_end,params_rt_ntvu,params_rt_obo,
paramsrtquit,params_rt_sh,params_rt_si,params_rt_sl,params_rt_srst,params_rt_start,params_rt_tstart,params_rt_tt,params_rt_ss,
params_cmet_mpulseid,params_errors,params_h_t,params_if,params_v,params_h_cr
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "URL from Beacon", showTimeStamp=false)
sessionFields = select("""\
select paramsu,params_pgu,params_r,params_r2,params_nu
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Page Structure from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_dom_doms,params_dom_img,params_dom_img_ext,
params_dom_script,params_dom_script_ext,
params_dom_ln,params_dom_res,params_dom_sz,params_dom_res_slowest
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""");
display(sessionFields)
println("params_dom_doms\t\t=\tUnique Domain Count")
println("params_dom_img\t\t=\tDOM Images On Page")
println("params_dom_img_ext\t=\tDOM Images Ext")
println("params_dom_script\t=\tDOM Srcipts On Page")
println("params_dom_script_ext\t=\tDOM Scripts Ext")
println("params_dom_ln\t\t=\tDOM Nodes On Page")
println("params_dom_res\t\t=\tResources in RT")
println("params_dom_sz\t\t=\tDOM Size")

displayTitle(chart_title = "Screen and Device Details from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_scr_bpp,params_scr_dpx,params_scr_mtp,params_scr_orn,params_scr_xy,params_mem_total,params_mem_used,params_bat_lvl,params_cpu_cnc
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Visibility State from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_vis_lh,params_vis_lv,params_vis_st
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "XHR Details from Beacon", showTimeStamp=false)
sessionFields = select("""\
select http_errno,params_http_method,params_http_hdr,params_http_initiator,params_xhr_sync,params_rt_subres
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Bandwidth & Latency Details from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_bw_time,params_lat,params_lat_err
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Mobile Connection Details from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_mob_ct,params_mob_bw,params_mob_mt
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Params Custom from Beacon", showTimeStamp=false)
sessionFields = select("""\
select params_custom0_st,params_custom1_st,params_custom2_st,params_custom3_st,params_custom4_st,params_custom5_st,params_custom6_st,params_custom7_st,params_custom8_st,params_custom9_st
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Google Analytics from Beacon", showTimeStamp=false)
sessionFields = select("""\
select ga_clientid,ga_utm_source,ga_utm_medium,ga_utm_term,ga_utm_content,ga_utm_campaign
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "AA & IA from Beacon", showTimeStamp=false)
sessionFields = select("""\
select aa_aid,aa_mid,aa_campaign,ia_coreid,ia_mmc_vendor,ia_mmc_category,ia_mmc_placement,ia_mmc_item
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Customer Dimensions from Beacon", showTimeStamp=false)
sessionFields = select("""\
select cdim
from $btv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp
""")

displayTitle(chart_title = "Matching Records from RT Data", showTimeStamp=false)
sessionFields = select("""\
select count(*),timestamp, sessionid
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
group by sessionid,timestamp
order by timestamp
""")

displayTitle(chart_title = "Description Fields from RT Data", showTimeStamp=false)
sessionFields = select("""\
select sessionid,session_start,timestamp,paramsu
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp,start_time
limit 1000
""")

display(sessionFields[1:3,:])
#display(sessionFields[31:end,:])
#data is all the same except timestamp may vary depending on in clause above

displayTitle(chart_title = "URL Fields from RT Data", showTimeStamp=false)

#todo for loop on all in the "in" list
#figure out how to do more than 30

sessionFields = select("""\
select initiator_type, substring(url for position('/' in substring(url from 9)) +7) urlgroup,url
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp,start_time
limit 1000
""")

display(sessionFields[1:30,:])
display(sessionFields[31:end,:])

displayTitle(chart_title = "Timing from RT Data", showTimeStamp=false)
sessionFields = select("""\
select start_time,
redirect_start,redirect_end,
fetch_start,
dns_start,dns_end,
tcp_connection_start,secure_connection_start,
tcp_connection_end,
request_start, response_first_byte,response_last_byte,
worker_start
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by timestamp,start_time
limit 1000
""")
display(sessionFields[1:30,:])
display(sessionFields[31:end,:])

displayTitle(chart_title = "Additional Details from RT Data", showTimeStamp=false)
sessionFields = select("""\
select encoded_size,transferred_size,decoded_size,height,width,x,y
from $rtv
where sessionid = '$(sessionid)' and timestamp in $(ts)
order by start_time
limit 1000
""")

try
    select("""drop view if exists $btv""")
    select("""drop view if exists $rtv""")

catch y
    println("clean up Exception ",y)
end
