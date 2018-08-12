#INITIALIZE
using DSWB, Formatting
using DSWBIJuliaComm
include("./init.jl")

#CONFIGURE
if (Base.length(timezone)==0) timezone="America/Los_Angeles" end #Set timezone to West Coast if not already set. Other options: "America/Chicago", "America/New_York"
if (startTime=="") startTime=getBeaconsFirstAndLast()[:1] ; print("startTime set to $(startTime)\t") else startTime = datetimeToUTC(startTime, TimeZone(timezone)) end #Set start time to first beacon if not set.
if (endTime=="") endTime=getBeaconsFirstAndLast()[:2] ; print("endTime set to $(endTime)\t") else endTime = datetimeToUTC(endTime, TimeZone(timezone)); end #Set end time to last beacon if not set.
fieldNames = [ :devicetypename, :pagegroupname, :user_agent_family ]

startTimeMs=datetimeToMs(datetimeToUTC(startTime,TimeZone(timezone)))
endTimeMs=datetimeToMs(datetimeToUTC(endTime,TimeZone(timezone)))
treemapView = "$(tableName)_shortdates"
select("""create or replace view $treemapView as (
select *
from $tableName
where timestamp between $(startTimeMs) and $(endTimeMs)
)""")
t = getTreemapData(treemapView, fieldNames, forceRefresh=true)
overallMedian = getOverallMedianLoadTime(t)
select("""drop view $(treemapView)""")

displayTitle(chart_title = "Drillable Treemap",chart_info=["$(customer) data from $(startTime) to $endTime"],showTimeStamp = false)
drawTreev2(t, "$(customer)", overallMedian)
;
