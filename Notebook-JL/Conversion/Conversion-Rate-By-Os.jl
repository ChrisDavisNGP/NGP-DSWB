# Conversion Rate by OS

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
setTable("RUM_PRD_BEACON_FACT_DSWB_34501")
setConversionGroup("Video")

startTime = DateTime(2016,7,8)
endTime = DateTime(2016,7,9)

chartConversionsByOS(startTime, endTime)
