using ODBC
using DataFrames
using DSWB
using Formatting
using Distributions

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,6,8,2,0,2017,6,8,12,59)
#TV = weeklyTimeVariables(days=1)
TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "Mobile"
UP.pageGroup = "News Article"
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 600000.0
UP.urlRegEx = "%"
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 10   # Tests use even numbers with > tests, make this an odd number or zero
SP.showLines = 25
ShowParamsValidate(SP)

defaultBeaconCreateView(TV,UP,SP)
setTable(UP.btView)

rawStatsSROS(TV,UP)

AllStatsDF = createAllStatsDF(TV,UP)

drawC3VizConverter(UP,AllStatsDF;graphType=1)

drawC3VizConverter(UP,AllStatsDF;graphType=2)

drawC3VizConverter(UP,AllStatsDF;graphType=3)

drawC3VizConverter(UP,AllStatsDF;graphType=4)

drawC3VizConverter(UP,AllStatsDF;graphType=5)

drawC3VizConverter(UP,AllStatsDF;graphType=6)

q = query(""" drop view if exists $(UP.btView);""")
q = query(""" drop view if exists $(UP.rtView);""")
;
