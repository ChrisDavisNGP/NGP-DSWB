using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"
sessions = "beacons_4744_sessions";

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
#setTable(tableRt, tableType = RESOURCE_TABLE)
#setTable(sessions; tableType = SESSIONS_TABLE);

include("../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

### This chart shows the median load times of the slowest page groups

chartTopPageGroupsByMedianLoadTime(TV.startTimeUTC, TV.endTimeUTC)
slowestPageGroups = getTopPageGroupsByMedianLoadTime(TV.startTimeUTC, TV.endTimeUTC)

###The following shows the slowest URLs from within each of the 5 slowest page groups.

slowest5PageGroups = slowestPageGroups[1:min(5, size(slowestPageGroups,1)), 1];
for i = 1:5
    pageGroup = slowest5PageGroups[i];
    df = getTopURLsByLoadTime(TV.startTimeUTC, TV.endTimeUTC; pageGroup = slowest5PageGroups[i], minPercentage=0.01);
    display("text/html", """
    <h2 style="color:#ccc">Slowest URLs in Page Group: $pageGroup</h2>
    """)
    display(df);
end
