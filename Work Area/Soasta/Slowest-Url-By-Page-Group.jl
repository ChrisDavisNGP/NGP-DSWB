#Slowest URLs by Page Group

---

The following graphs show information concerning the slowest URLs.

using DSWB
setRedshiftEndpoint("dswb-natgeo")
setTable("beacons_4744")

startTime = DateTime(2016,7,1)
endTime = DateTime(2016,7,13)

### This chart shows the median load times of the slowest page groups

chartTopPageGroupsByMedianLoadTime(startTime, endTime)
slowestPageGroups = getTopPageGroupsByMedianLoadTime(startTime, endTime)

###The following shows the slowest URLs from within each of the 5 slowest page groups.

slowest5PageGroups = slowestPageGroups[1:min(5, size(slowestPageGroups,1)), 1];
for i = 1:5
    pageGroup = slowest5PageGroups[i];
    df = getTopURLsByLoadTime(startTime, endTime; pageGroup = slowest5PageGroups[i], minPercentage=0.01);
    display("text/html", """
    <h2 style="color:#ccc">Slowest URLs in Page Group: $pageGroup</h2>
    """)
    display(df);
end
