# Conversions by Average Load Times

using DSWB
setRedshiftEndpoint("dswb-natgeo")
beaconTable = setTable("beacons_4744")
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
