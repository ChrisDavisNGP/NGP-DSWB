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
include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,1,2,2,30,2017,1,2,2,45);

UP = UrlParamsInit(scriptName)
UP.timeUpperMs = 6000000 # Extra long times for big files
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

defaultBeaconCreateView(TV,UP,SP)

try
    t1DF = query("""
    select
        user_agent_device_type,
        user_agent_family,
    user_agent_os,user_agent_manufacturer,params_ua_vnd,
    count(*)
    from $(UP.btView)
    group by
        user_agent_device_type,
        user_agent_family,
    user_agent_os,user_agent_manufacturer,params_ua_vnd
    order by count(*) desc
    limit 30
    """)
    beautifyDF(t1DF)
catch y
    println("setupLocalTable Exception ",y)
end



try
    t2DF = query("""
    select
        user_agent_device_type,
        user_agent_family,
    geo_netspeed, params_cpu_cnc,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd,
    count(*)
    from $(UP.btView)
    group by
        user_agent_device_type,
        user_agent_family,
    geo_netspeed, params_cpu_cnc,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd
    order by count(*) desc
    limit 25
    """)
    beautifyDF(t2DF)
catch y
    println("setupLocalTable Exception ",y)
end

try
    t3DF = query("""
    select
        user_agent_device_type,
        user_agent_family,
        params_scr_xy,
    geo_cc, geo_netspeed, params_cpu_cnc,params_scr_bpp,params_scr_dpx,params_scr_mtp,params_scr_orn,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd,
    count(*)
    from $(UP.btView)
    group by
        user_agent_device_type,
        user_agent_family,
        params_scr_xy,
    geo_cc, geo_netspeed, params_cpu_cnc,params_scr_bpp,params_scr_dpx,params_scr_mtp,params_scr_orn,user_agent_os,user_agent_osversion,user_agent_manufacturer,params_ua_plt,params_ua_vnd
    order by count(*) desc
    limit 25
    """)
    beautifyDF(t3DF)
catch y
    println("setupLocalTable Exception ",y)
end

try
    t4DF = query("""SELECT * FROM $(UP.btView) limit 3""")
    beautifyDF(t4DF)
catch y
    println("setupLocalTable Exception ",y)
end

joinTablesTest = DataFrame()

try
    joinTablesTest = query("""\
    select $tableRt.*
    from $(UP.btView) join $tableRt
    on $(UP.btView).session_id = $tableRt.session_id and $(UP.btView)."timestamp" = $tableRt."timestamp"
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
        $(UP.btView).user_agent_device_type,
        $(UP.btView).user_agent_family as useragentfamily,
        $(UP.btView).params_scr_xy,
        $(UP.btView).session_id,
        $(UP.btView)."timestamp",
        sum($tableRt.encoded_size) as encoded,
        sum($tableRt.transferred_size) as transferred,
        sum($tableRt.decoded_size) as decoded,
        count(*)
    from $(UP.btView) join $tableRt
    on $(UP.btView).session_id = $tableRt.session_id and $(UP.btView)."timestamp" = $tableRt."timestamp"
    where $tableRt.encoded_size > 1
    group by $tableRt.url,$(UP.btView).user_agent_device_type,$(UP.btView).user_agent_family,
        $(UP.btView).params_scr_xy,$(UP.btView).session_id,$(UP.btView)."timestamp"
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

i = 0
for row in eachrow(joinTableSummary)
    i += 1
    detailsPrint(UP.btView,tableRt,joinTableSummary,i)
    statsDetailsPrint2(UP.btView,joinTableSummary,i)
    if (i >= linesOutput)
        break;
    end
end
;
