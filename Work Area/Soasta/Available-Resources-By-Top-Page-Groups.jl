
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
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

#startTime = DateTime(2016, 2, 15);
#endTime = DateTime(2016, 2, 15, 23, 59, 59);
#startTimeMs = datetimeToMs(startTime);
#endTimeMs = datetimeToMs(endTime);

url = "%www.nationalgeographic.com%";
conversionMetric = :custom_metrics_0;
pageGroup = "Nat Geo Homepage";
os = "iOS";
defaultCutOff = 98;

#dropTables("officedepot_beacons_rt_first_party_summary", "officedepot_beacons_rt_summary",
#    "officedepot_beacons_rt_summary_first_pg", "officedepot_beacons_rt_summary_pg", "officedepot_beacons_rt_summary_third_pg",
#    "officedepot_beacons_rt_third_party_summary")

#dropTables( "temp_full_sum",
#"temp_full_sum_first",
#"temp_full_sum_third" )

#todo find showpercentage
showPercentage(url, TV.startTime, TV.endTime; pageGroup = pageGroup)

showPercentage(url, startTime, endTime; pageGroup = pageGroup, browser = "Mobile Safari")

values = Dict{String, Array}();
values[string(PG)] = [pageGroup];

@time chartResourceServersStatSummary(startTime, endTime; minResourceHits = 10_000, minLoadTime = 0, maxLoadTime = DEFAULT_1_MIN, byList = values)

values = Dict{String, Array}();
values[string(PG)] = [pageGroup];

@time chartResourceServerStatSummary(startTime, endTime; minResourceHits = 10_000, minLoadTime = 0, maxLoadTime = DEFAULT_1_MIN, byList = values)

@time chartSiteSummaryByBeaconCriteria(startTime, endTime; pageGroup = pageGroup, pgPercentCutOff = defaultCutOff)

topResourcesPerPage[topResourcesPerPage[:, :row] .<= 10, 1:10]

getP
