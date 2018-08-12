# Top External Referrers

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
setTable("RUM_PRD_BEACON_FACT_DSWB_34501")
setConversionGroup("Video")

startTime = DateTime(2016,6,1)
endTime   = DateTime(2016,7,15)

# External Referrers Grouped by Domain Name

chartExternalReferrerSummary(startTime, endTime, groupByDomain=true)
