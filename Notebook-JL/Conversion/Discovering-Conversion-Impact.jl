# Discovering the Conversion Impact

using DSWB
db = setSnowflakeEndpoint("tenant_232301")
setTable("RUM_PRD_BEACON_FACT_DSWB_34501")
setConversionGroup("Video Player Base")

startTime = DateTime(2016,7,10)
endTime = DateTime(2016,7,13)

#Ranking Page Groups by Negative Impact to Conversions

The conversion impact score is a relative score and is provided as a means to rank page groups by their propensity to negatively impact conversions due to high load times. For each page group it is calculated using the the proportion of overall requests that are associated with that group, along with the Spearman Ranked Correlation between its load times and number of conversions. The conversion impact score will always be a number between -1 and 1, though scores much greater than zero should be very rare. The more negative the score, the more detrimental to conversions that high load times for that page group are, relative to the other page groups.

groupsWithHighConversionImpact = getTopGroupsByConversionImpact(startTime, endTime)

convImpactTop20 = groupsWithHighConversionImpact[1:20, [symbol("Page Group"), symbol("Full Page Load Time"), symbol("Relative Conversion Impact Score")]]
drawImpact(convImpactTop20)

#Ranking Page Groups by Negative Impact to User Activity

The activity impact score is a relative score and is provided as a means to rank page groups by their propensity to negatively impact user activity due to high load times. For each page group it is calculated using the the proportion of overall requests that are associated with that group, along with the Spearman Ranked Correlation between its load times and number of pages requested per session. The activity impact score will always be a number between -1 and 1, though scores much greater than zero should be very rare. The more negative the score, the more detrimental to user activity that high load times for that page group are, relative to the other page groups.

groupsWithHighActivityImpact = getTopGroupsByActivityImpact(startTime, endTime)

activityImpactTop20 = groupsWithHighActivityImpact[1:20, [symbol("Page Group"), symbol("Full Page Load Time"), symbol("Relative Activity Impact Score")]]
drawImpact(activityImpactTop20)
