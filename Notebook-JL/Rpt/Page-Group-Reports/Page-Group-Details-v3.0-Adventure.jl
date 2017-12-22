using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
#setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)
#TV = weeklyTimeVariables(days=7)
TV = yesterdayTimeVariables()


UP = UrlParamsInit(scriptName)
UP.agentOs = "%"
UP.deviceType = "Mobile"
UP.limitRows = 10
#UP.limitRows = 250
UP.pageGroup = "Adventure AEM"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.devView=false
SP.criticalPathOnly=true
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)

type LocalVars
    mobileView::ASCIIString
    desktopView::ASCIIString
end

LV = LocalVars("$(table)_$(scriptName)_Mobile_view_prod","$(table)_$(scriptName)_Desktop_view_prod")

pageGroupDetailsWorkflow(TV,UP,SP,mobileView,desktopView)
;
