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
include("/data/notebook_home/Production/Lib/Include-Package-v1.0.jl")
include("/data/notebook_home/Production/Bodies/AEM-Large-Images-Body-v1.0.jl")

# Time values (tv.) structure created in include above, so init time here
#timeVariables(2017,1,2,2,30,2017,1,2,2,45);
timeVariables(2017,6,7,10,0,2017,6,7,10,9);
#weeklyTimeVariables(days=1);

UP = UrlParamsInit()
UP.beaconTable = table   #table
UP.resourceTable = tableRt
UP.btView = "$(table)_AEM_Large_Images_view" #localtable
UP.pageGroup = "%"   #productPageGroup
UP.urlRegEx = "%"   #localUrl
UP.deviceType = "mobile"

SG = SoastaGraphsInit()
customer = "Nat Geo" 
SG.customer = customer

linesOutput = 3
;

defaultLocalTableALI(tv,UP)
;

joinTables = DataFrame()
joinTables = gatherSizeDataALI(UP,linesOutput)
;

joinTableSummary = DataFrame()
joinTableSummary = tableSummaryALI(joinTables)
;

i = 0
for row in eachrow(joinTableSummary)
    i += 1
    detailsPrint(UP,joinTableSummary,i)
    statsDetailsPrint(UP,joinTableSummary,i)
    if (i >= linesOutput)
        break;
    end
end
;

q = query(""" drop view if exists $(UP.btView);""")
;