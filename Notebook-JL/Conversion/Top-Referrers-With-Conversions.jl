# Top External Referrers

using DSWB
setRedshiftEndpoint("dswb-natgeo")
setTable("beacons_4744")
setConversionGroup("Video")

startTime = DateTime(2016,6,1)
endTime   = DateTime(2016,7,15)

# External Referrers Grouped by Domain Name

chartExternalReferrerSummary(startTime, endTime, groupByDomain=true)
