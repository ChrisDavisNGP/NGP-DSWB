using RT;

dsn = "vfdsn";
bTable = "dillards_beacons";
rtTable = "$(bTable)_rt";

connect(Redshift, dsn);
setRedshiftEndpoint(dsn);

setTable(bTable);
setTable(rtTable; tableType=RESOURCE_TABLE);

startTime = DateTime(2016, 2, 15);
endTime = DateTime(2016, 2, 15, 23, 59, 59);
startTimeMs = datetimeToMs(startTime);
endTimeMs = datetimeToMs(endTime);

url = "http%://www.dillards.com%";
conversionMetric = :custom_metrics_0;
pageGroup = "Category";
os = "iOS";
defaultCutOff = 98;

dropTables("officedepot_beacons_rt_first_party_summary", "officedepot_beacons_rt_summary",
    "officedepot_beacons_rt_summary_first_pg", "officedepot_beacons_rt_summary_pg", "officedepot_beacons_rt_summary_third_pg",
    "officedepot_beacons_rt_third_party_summary")

dropTables( "temp_full_sum",
"temp_full_sum_first",
"temp_full_sum_third" )

showPercentage(url, startTime, endTime; pageGroup = pageGroup)

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
