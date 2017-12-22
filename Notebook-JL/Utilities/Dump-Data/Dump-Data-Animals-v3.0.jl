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

# Packages
include("../../../Lib/Include-Package-v2.1.jl")
#include("SQL-Data-Mining-For-Group-Body-v1.0.jl")

#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UP.pageGroup = "%"   #productPageGroup
UP.urlRegEx = "%"   #localUrl
UP.deviceType = "Mobile"

SP = ShowParamsInit()
SP.debugLevel = 10;
SP.debug = true;
SP.showLines = 25

UP.pageGroup = "Animals AEM"
displayGroup(TV,UP,SP)
;
