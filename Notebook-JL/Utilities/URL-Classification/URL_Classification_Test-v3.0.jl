using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")
include("../../../Lib/URL-Classification-Package-v2.0.jl")

#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)
TV = weeklyTimeVariables(days=2)
#TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

toppageurl = query("""\
    select
        'None' as urlpagegroup,
        CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup
    FROM $(UP.resourceTable)
    where
        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
    group by urlgroup,urlpagegroup
    limit $(UP.limitRows)
 """)

 display(size(toppageurl))

 #save for debug
toppageurlbackup = deepcopy(toppageurl);

#include("../../../Lib/URL-Classification-Package-v2.0.jl")

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary(SP.debug);

#include("../../../Lib/URL-Classification-Package-v2.0.jl")

# Debug
toppageurl = deepcopy(toppageurlbackup)

scrubUrlToPrint(SP,toppageurl,:urlgroup);
classifyUrl(SP,toppageurl);
#
