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

# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

short_results = getLatestResults(hours=0, minutes=5, table_name=table)
size(short_results)

groups, group_summary = groupResults(short_results, dims=2, showProgress=true)
beautifyDF(group_summary)

gbg = getBestGrouping(short_results, group_summary)
beautifyDF(gbg)

soasta_results = getLatestResults(table_name=table, hours=4);
size(soasta_results)

groups, group_summary = groupResults(soasta_results, dims=2, showProgress=true);
beautifyDF(group_summary)

gbg = getBestGrouping(soasta_results, group_summary)
beautifyDF(gbg)
;
