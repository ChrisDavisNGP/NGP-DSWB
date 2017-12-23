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

#TV = timeVariables(2017,6,17,10,0,2017,6,17,10,59);
TV = weeklyTimeVariables(days=1)

UP = UrlParamsInit(scriptName)
UP.pageGroup = "Channel"   #productPageGroup
UP.urlRegEx = "%"   #localUrl
UP.deviceType = "%"
UP.sizeMin = 50000

SP = ShowParamsInit()
SP.debugLevel = 5
SP.showLines = 10

defaultBeaconCreateView(TV,UP,SP)

fileType = "%jpg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%png"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%jpeg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%gif"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%imviewer"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%svg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%jpeg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

q = query(""" drop view if exists $(UP.btView);""")
;
