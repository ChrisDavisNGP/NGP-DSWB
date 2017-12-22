using DSWB
setRedshiftEndpoint("dswb-natgeo")
setTable("beacons_4744");

bc = getBeaconCount();
all = getBeaconsFirstAndLast();
bcType = getBeaconCountByType();

methods(getBeaconCount)

beautifyDF(names!(bc[:,:],[symbol("Beacon Count")]))
beautifyDF(all)
beautifyDF(names!(bcType[:,:],[symbol("Beacon Type"),symbol("Beacon Count")]))

query("""\
select * from beacons_4744 where page_group = 'News Article' limit 3
""")

query("""\
select * from beacons_4744 where page_group = 'News Article' and beacon_type = 'page view' limit 3
""")

query("""\
select * from beacons_4744 where beacon_type = 'error' limit 3
""")

setTable("beacons_4744_rt", tableType = "RESOURCE_TABLE");

rtcnt = query("""select count(*) from beacons_4744_rt""");

maxRt = query("""\
select max("timestamp") from beacons_4744_rt
""");

minRt = query("""\
select min("timestamp") from beacons_4744_rt
""");

minStr = msToDateTime(minRt[1,:min]);
maxStr = msToDateTime(maxRt[1,:max]);

printDf = DataFrame();
printDf[:minStr] = minStr;
printDf[:maxStr] = maxStr;
;

beautifyDF(names!(rtcnt[:,:],[symbol("Resource Timing Count")]))
beautifyDF(names!(printDf[:,:],[symbol("First RT"),symbol("Last RT")]))

;
