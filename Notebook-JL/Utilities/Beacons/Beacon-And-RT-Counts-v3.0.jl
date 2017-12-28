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

TV = pickTime()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.reportLevel = 10  # Set to 10 to see all non-debug info including long queries
ShowParamsValidate(SP)

bt = UP.beaconTable
rt = UP.resourceTable

if (SP.reportLevel > 9)
    bc = getBeaconCount();
    allBeacons = getBeaconsFirstAndLast();
    #bcType = getBeaconCountByType();
    beautifyDF(names!(bc[:,:],[Symbol("Beacon Count")]))
    beautifyDF(allBeacons)
    #beautifyDF(names!(bcType[:,:],[Symbol("Beacon Type"),Symbol("Beacon Count")]))
end

UP.pageGroup = "News Article"
UP.limitRows = 10
t1DF = defaultLimitedBeaconsToDF(TV,UP,SP)
standardChartTitle(TV,UP,SP,"$(UP.pageGroup) Page View Dump")
beautifyDF(t1DF)

t2DF = errorBeaconsToDF(TV,UP,SP)
if (size(t2DF,1) > 0)
    standardChartTitle(TV,UP,SP,"Error Beacon Dump")
    beautifyDF(t2DF)
end

rtcnt = query("""select count(*) from $rt""");
maxRt = query("""select max("timestamp") from $rt""");
minRt = query("""select min("timestamp") from $rt""");

minStr = msToDateTime(minRt[1,:min]);
maxStr = msToDateTime(maxRt[1,:max]);

printDf = DataFrame();
printDf[:minStr] = minStr;
printDf[:maxStr] = maxStr;

standardChartTitle(TV,UP,SP,"Resource Information")
beautifyDF(names!(rtcnt[:,:],[Symbol("Resource Timing Count")]))
beautifyDF(names!(printDf[:,:],[Symbol("First RT"),Symbol("Last RT")]))
;
