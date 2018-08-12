# Conversions by Average Load Times

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
beaconTable = setTable("RUM_PRD_BEACON_FACT_DSWB_34501")
conversionPageURL = "%fls.doubleclick.net/activity%"
setConversionPage(conversionPageURL)


getBeaconCount()

getBeaconsFirstAndLast()

startTime = DateTime(2016,8,8)
endTime = DateTime(2016,8,9)

getBeaconCount(startTime,endTime)

chartConversionsVsLoadTimes(startTime,endTime)

chartConversionsVsLoadTimes(startTime,endTime, timer=:timers_t_page, conversionPage=conversionPageURL)

chartConversionsVsLoadTimes(startTime,endTime, url="%news%")

chartConversionsVsLoadTimes(startTime,endTime, url="%Men.jsp%")
