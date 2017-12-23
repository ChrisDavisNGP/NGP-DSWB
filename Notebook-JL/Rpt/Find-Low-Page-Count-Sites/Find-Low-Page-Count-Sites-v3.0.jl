## Tables and Data Source setup

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
;

include("../../Lib/Include-Package-v2.1.jl")
;

TV = weeklyTimeVariables(days=1)
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)
#TV = yesterdayTimeVariables()
;

UP = UrlParamsInit("Find_Low_Page_Count_Sites")
UP.agentOs = "%"
UP.deviceType = "%"
UP.limitRows = 250
UP.pageGroup = "%"   #productPageGroup
UP.samplesMin = 10
UP.sizeMin = 10000
UP.timeLowerMs = 2000.0
UP.timeUpperMs = 60000.0
UP.urlRegEx = "%"   #localUrl
UP.urlFull = "%"
UP.usePageLoad=false
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.criticalPathOnly=true
SP.devView=false
SP.debugLevel = 0   # Tests use even numbers with > tests, make this an odd number or zero
ShowParamsValidate(SP)
;

displayTitle(chart_title = "Channel", showTimeStamp=false)
displayTopUrlsByCount(TV,UP,SP,"Channel Microsite";rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Channel Legacy";   rowLimit=10000,beaconsLimit=0,paginate=true);

displayTitle(chart_title = "Small Groups", showTimeStamp=false)
displayTopUrlsByCount(TV,UP,SP,"Education";         rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Errors";            rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Expedition Granted";rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Kids MyShot";       rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Index PHP";         rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"News EC2";          rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Radio";             rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Sponsored";         rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Tickets";           rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Travel Subs";       rowLimit=10000,beaconsLimit=0,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Video AMP";         rowLimit=10000,beaconsLimit=0,paginate=true);

displayTitle(chart_title = "Medium Groups", showTimeStamp=false)
displayTopUrlsByCount(TV,UP,SP,"Help";              rowLimit=10000,beaconsLimit=50,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Travel WP";         rowLimit=10000,beaconsLimit=20,paginate=true);

displayTitle(chart_title = "Unclassified", showTimeStamp=false)
displayTopUrlsByCount(TV,UP,SP,"Nat Geo Site";      rowLimit=10000,beaconsLimit=10,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"Unknown";           rowLimit=10000,beaconsLimit=1,paginate=true);
displayTopUrlsByCount(TV,UP,SP,"(No Page Group)";   rowLimit=10000,beaconsLimit=1,paginate=true);

#No Segment Data displayTitle(chart_title = "Cheeta", showTimeStamp=false)
#displayTopUrlsByCount(TV,UP,SP,"Cheeta CMS";        rowLimit=10000,beaconsLimit=5,paginate=true);
#displayTopUrlsByCount(TV,UP,SP,"Cheeta NGM";        rowLimit=10000,beaconsLimit=100,paginate=true);

;
