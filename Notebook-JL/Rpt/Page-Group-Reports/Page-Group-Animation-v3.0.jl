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

TV = pickTime()
#TV = timeVariables(2016,12,21,19,0,2016,12,21,23,59);

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "News Article"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 600000.0
UP.urlRegEx = "%channel.nationalgeographic.com/genius%"   #localUrl
UP.urlFull = "http://channel.nationalgeographic.com/genius/"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
SP.showLines = 25
SP.reportLevel = 2
ShowParamsValidate(SP)

openingTitle(TV,UP,SP)

bt = UP.beaconTable
btv = UP.btView

# Create view to query only product page_group
#query("""create or replace view $btv as (select * from $bt where page_group = '$(UP.pageGroup)' and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC))""")
defaultBeaconCreateView(TV,UP,SP)

setTable(btv)

# Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
# where beacon_type = 'page view'
# t1DF = query("""SELECT count(*) FROM $btv""")

retailer_results = getLatestResults(hours=1, minutes=30, table_name="$(btv)")
size(retailer_results)

# drop some of the fields to make the output easier to read

#delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model,:referrer])
delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model])

doit(retailer_results, showDimensionViz=true, showProgress=true);

q = query(""" drop view if exists $btv;""")
;
