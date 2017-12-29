# Page Group Performances vs. Conversions

using DSWB
setRedshiftEndpoint("dswb-natgeo")
setTable("beacons_4744")
setConversionGroup("Video")

startTime = DateTime(2016,7,10);
endTime = DateTime(2016,7,13);

groupsWithHighConversionImpact = getTopGroupsByConversionImpact(startTime, endTime)

### This graph illustrates the relationship between a page group's relative conversion impact score, and its median load time.

convImpactTop20 = groupsWithHighConversionImpact[1:min(20, size(groupsWithHighConversionImpact, 1)), [symbol("Page Group"), symbol("Full Page Load Time"), symbol("Relative Conversion Impact Score")]];
drawImpact(convImpactTop20);

###The following charts show the relationship between page load times and conversion rates for the 5 groups with the highest conversion impact.

top5Pages = convImpactTop20[1:5, symbol("Page Group")];
for i = 1:5 chartConversionsVsLoadTimes(startTime, endTime; pageGroup = top5Pages[i]); end
