# Conversions by Average Load Times

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
beaconTable = setTable("RUM_PRD_BEACON_FACT_DSWB_34501")
conversionPageURL = "%fls.doubleclick.net/activity%"
setConversionPage(conversionPageURL)


getBeaconCount()

getBeaconsFirstAndLast()

start_time = DateTime(2016,8,8)
endTime = DateTime(2016,8,9)

getBeaconCount(start_time,endTime)

chartConversionsVsLoadTimes(start_time,endTime)

chartConversionsVsLoadTimes(start_time,endTime, timer=:timers_t_page, conversionPage=conversionPageURL)

chartConversionsVsLoadTimes(start_time,endTime, url="%news%")

chartConversionsVsLoadTimes(start_time,endTime, url="%Men.jsp%")
