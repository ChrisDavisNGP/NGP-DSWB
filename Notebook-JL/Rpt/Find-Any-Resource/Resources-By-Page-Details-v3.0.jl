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

include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

# Resource and it parent is also interesting below this point

try
    query("""\
        create or replace view $(UP.btView) as (
            select * from $(UP.beaconTable)
                where
                    "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                    page_group ilike '$(UP.pageGroup)' and
                    params_u ilike '$(UP.urlRegEx)'
        )
    """)
    cnt = query("""SELECT count(*) FROM $(UP.btView)""")
    println("$(UP.btView) count is ",cnt[1,1])
catch y
    println("setupLocalTable Exception ",y)
end

linesOutput = 3
resourceMatched(TV,UP,SP;linesOut=linesOutput)

resourceSummaryAllFields(TV,UP,SP;linesOut=linesOutput)

linesOutput = SP.showLines
resourceSummary(TV,UP,SP;linesOut=linesOutput)

minimumEncoded = 0
resourceSize(TV,UP,SP;linesOut=linesOutput,minEncoded=minimumEncoded)

resourceScreen(TV,UP,SP;linesOut=linesOutput)

resourceSummaryDomainUrl(TV,UP,SP;linesOut=linesOutput)

resourceTime1(TV,UP,SP;linesOut=linesOutput)

resourceTime2(TV,UP,SP;linesOut=linesOutput)

resourceTime3(TV,UP,SP;linesOut=linesOutput)
