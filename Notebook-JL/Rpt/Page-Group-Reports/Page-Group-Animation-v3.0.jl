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

customer = "Nat Geo"
productPageGroup = "News Article" # primary page group
localTable = "$(table)_$(scriptName)_productPage_view_prod"
localTableRt = "$(tableRt)_productPage_view_prod"

#TV = timeVariables(2016,12,21,19,0,2016,12,21,23,59);
TV = yesterdayTimeVariables()

# Create view to query only product page_group
query("""create or replace view $localTable as (select * from $table where page_group = '$(productPageGroup)' and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC))""")

setTable(localTable)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
# t1DF = query("""SELECT count(*) FROM $localTable""")

retailer_results = getLatestResults(hours=1, minutes=30, table_name="$(localTable)")
size(retailer_results)

# drop some of the fields to make the output easier to read

#delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model,:referrer])
delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model])

doit(retailer_results, showDimensionViz=true, showProgress=true);

q = query(""" drop view if exists $localTable;""")
q = query(""" drop view if exists $localTableRt;""")
;
