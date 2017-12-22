using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser
using JSON

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

TV = yesterdayTimeVariables(hours=1)
UP = UrlParamsInit(scriptName)
SP = ShowParamsInit()
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.reportLevel = 10  # Set to 10 to see all non-debug info including long queries
ShowParamsValidate(SP)

if (SP.reportLevel > 9)
    bc = getBeaconCount();
    all = getBeaconsFirstAndLast();
    #bcType = getBeaconCountByType();
    beautifyDF(names!(bc[:,:],[symbol("Beacon Count")]))
    beautifyDF(all)
    #beautifyDF(names!(bcType[:,:],[symbol("Beacon Type"),symbol("Beacon Count")]))
end

t1DF = query("""\
select * from beacons_4744 where page_group = 'News Article' and beacon_type = 'page view' limit 3
""")
standardChartTitle(TV,UP,SP,"News Article Page View Dump")
beautifyDF(t1DF)

t2DF = query("""select * from beacons_4744 where beacon_type = 'error' limit 3""")
if (size(t2DF,1) > 0)
    standardChartTitle(TV,UP,SP,"Error Beacon Dump")
    beautifyDF(t2DF)
end

rtcnt = query("""select count(*) from beacons_4744_rt""");
maxRt = query("""select max("timestamp") from beacons_4744_rt""");
minRt = query("""select min("timestamp") from beacons_4744_rt""");

minStr = msToDateTime(minRt[1,:min]);
maxStr = msToDateTime(maxRt[1,:max]);

printDf = DataFrame();
printDf[:minStr] = minStr;
printDf[:maxStr] = maxStr;
;

standardChartTitle(TV,UP,SP,"Resource Information")
beautifyDF(names!(rtcnt[:,:],[symbol("Resource Timing Count")]))
beautifyDF(names!(printDf[:,:],[symbol("First RT"),symbol("Last RT")]))

;
