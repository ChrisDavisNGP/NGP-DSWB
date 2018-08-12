# Test Plan Creation: Path Analysis

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
setTable("beacons_4744")

startTime = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

# Find the peak day
getPeak(startTime, endTime, :day)

# Find the peak hour
getPeak(startTime, endTime, :hour)

# Find the peak minute
getPeak(startTime, endTime, :minute)

# Find the peak second
getPeak(startTime, endTime, :second)

# Find the peak day for order confirmations
getPeak(startTime, endTime, :day, pageGroup="News Article")

# Find the peak day for order confirmations
getPeak(startTime, endTime, :hour, pageGroup="News Article")

# Find the page group distribution for the peak traffic hour
startTime = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

getGroupPercentages(startTime, endTime)

# Calculate the average session length for the peak day
startTime = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

getAverageSessionDuration(startTime, endTime)

# Calculate the top 10 paths that resulted in conversions
@time top10 = getAllPaths(startTime, endTime; n=10, conversionGroup = "Video");

# Generate a sunburst chart for the top 10 paths that resulted in conversions
drawSunburst(top10[1]; totalPaths=top10[3])

# Get the top 10 paths and generate a sunburst chart for the month of August 2014
#startTime = DateTime(2016,9,28,21,0,0)
#endTime   = DateTime(2016,9,28,22,0,0)

result10 = getAllPaths(startTime, endTime; n=10, f=getAbandonPaths);
drawSunburst(result10[1]; totalPaths=result10[3])

# Get the top 10 paths that most represent user behavior and generate a sunburst chart for the month of August 2014
#startTime = DateTime(2016,6,28,21,0,0)
#endTime   = DateTime(2016,6,28,22,0,0)

represent10 = getRepresentativeTestPlanPaths(startTime, endTime; n=10);
drawSunburst(represent10[1]; totalPaths=represent10[3])
