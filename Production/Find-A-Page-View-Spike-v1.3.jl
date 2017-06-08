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

# Packages
include("/data/notebook_home/Production/Bodies/Find-A-Page-View-Spike-Body-v1.3.jl")
include("/data/notebook_home/Production/Lib/Include-Package-v1.0.jl")

# Time values (tv.) structure created in include above, so init time here
timeVariables(2017,6,3,10,59,2017,6,3,12,59)
#weeklyTimeVariables(days=1)
#yesterdayTimeVariables()
;

customer = "Nat Geo"
productPageGroup = "Your Shot" # primary page group
#productPageGroup = "Travel AEM" # primary page group
localTable = "$(table)_spike_pview_prod"
localTableRt = "$(tableRt)_spike_pview_prod"
;

firstAndLast()

sessionsBeacons()

loadTime()
topUrls()
peakTable()
statsTable()

q = query(""" drop view if exists $localTable;""")
q = query(""" drop view if exists $localTableRt;""")
;
