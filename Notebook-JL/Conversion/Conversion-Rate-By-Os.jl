# Conversion Rate by OS

using DSWB
setRedshiftEndpoint("dswb-natgeo")
setTable("beacons_4744")
setConversionGroup("Video")

startTime = DateTime(2016,7,8)
endTime = DateTime(2016,7,9)

chartConversionsByOS(startTime, endTime)
