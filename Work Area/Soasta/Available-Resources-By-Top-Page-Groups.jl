
using RT;
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
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)

UP = UrlParamsInit(scriptName)
UP.urlRegEx = "%www.nationalgeographic.com%";
UP.pageGroup = "Nat Geo Homepage";
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

#startTime = DateTime(2016, 2, 15);
#endTime = DateTime(2016, 2, 15, 23, 59, 59);
#startTimeMs = datetimeToMs(startTime);
#endTimeMs = datetimeToMs(endTime);

conversionMetric = :custom_metrics_0;
defaultCutOff = 98;

#dropTables("officedepot_beacons_rt_first_party_summary", "officedepot_beacons_rt_summary",
#    "officedepot_beacons_rt_summary_first_pg", "officedepot_beacons_rt_summary_pg", "officedepot_beacons_rt_summary_third_pg",
#    "officedepot_beacons_rt_third_party_summary")

#dropTables( "temp_full_sum",
#"temp_full_sum_first",
#"temp_full_sum_third" )

#todo find showpercentage
#showPercentage(UP.urlRegEx, TV.startTime, TV.endTime; pageGroup = UP.pageGroup)
#showPercentage(UP.urlRegEx, startTime, endTime; pageGroup = UP.pageGroup, browser = "Mobile Safari")

valuesDict = Dict{String, Array}();
valuesDict[string(PG)] = [UP.pageGroup];

@time chartResourceServersStatSummary(startTime, endTime; minResourceHits = 10_000, minLoadTime = 0, maxLoadTime = DEFAULT_1_MIN, byList = valuesDict)

valuesDict2 = Dict{String, Array}();
valuesDict2[string(PG)] = [UP.pageGroup];

@time chartResourceServerStatSummary(startTime, endTime; minResourceHits = 10_000, minLoadTime = 0, maxLoadTime = DEFAULT_1_MIN, byList = valuesDict2)

@time chartSiteSummaryByBeaconCriteria(startTime, endTime; pageGroup = UP.pageGroup, pgPercentCutOff = defaultCutOff)

topResourcesPerPage[topResourcesPerPage[:, :row] .<= 10, 1:10]

getP
