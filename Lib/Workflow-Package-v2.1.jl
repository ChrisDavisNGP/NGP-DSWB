function pageGroupDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

    pageGroupDetailsTables(UP.btView,mobileView,desktopView,UP.beaconTable,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
    setTable(UP.btView);

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = 0.0
    statsDF = statsPGD()
    medianThreshold = statsDF[1:1,:median][1]
    
end
