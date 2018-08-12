# Test Plan Creation: Path Analysis

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
setTable("RUM_PRD_BEACON_FACT_DSWB_34501")

start_time = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

# Find the peak day
getPeak(start_time, endTime, :day)

# Find the peak hour
getPeak(start_time, endTime, :hour)

# Find the peak minute
getPeak(start_time, endTime, :minute)

# Find the peak second
getPeak(start_time, endTime, :second)

# Find the peak day for order confirmations
getPeak(start_time, endTime, :day, pageGroup="News Article")

# Find the peak day for order confirmations
getPeak(start_time, endTime, :hour, pageGroup="News Article")

# Find the page group distribution for the peak traffic hour
start_time = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

getGroupPercentages(start_time, endTime)

# Calculate the average session length for the peak day
start_time = DateTime(2016,9,3,0,0,0)
endTime   = DateTime(2016,9,3,23,0,0)

getAverageSessionDuration(start_time, endTime)

# Calculate the top 10 paths that resulted in conversions
@time top10 = getAllPaths(start_time, endTime; n=10, conversionGroup = "Video");

# Generate a sunburst chart for the top 10 paths that resulted in conversions
drawSunburst(top10[1]; totalPaths=top10[3])

# Get the top 10 paths and generate a sunburst chart for the month of August 2014
#start_time = DateTime(2016,9,28,21,0,0)
#endTime   = DateTime(2016,9,28,22,0,0)

result10 = getAllPaths(start_time, endTime; n=10, f=getAbandonPaths);
drawSunburst(result10[1]; totalPaths=result10[3])

# Get the top 10 paths that most represent user behavior and generate a sunburst chart for the month of August 2014
#start_time = DateTime(2016,6,28,21,0,0)
#endTime   = DateTime(2016,6,28,22,0,0)

represent10 = getRepresentativeTestPlanPaths(start_time, endTime; n=10);
drawSunburst(represent10[1]; totalPaths=represent10[3])
