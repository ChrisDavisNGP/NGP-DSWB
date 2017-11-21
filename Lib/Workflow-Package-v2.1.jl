function pageGroupDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

    pageGroupDetailsTables(TV,UP,mobileView,desktopView)
    setTable(UP.btView);

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = 0.0
    statsDF = statsPGD(TV,UP)
    medianThreshold = statsDF[1:1,:median][1]

    peakPGD()

    concurrentSessionsPGD()

    loadTimesPGD()

    topUrlPGD()

    thresholdChartPGD(medianThreshold)

    sessionLoadPGD()

    loadTimesParamsUPGD()

    medianTimesPGD()

    customRefPGD()

    stdRefPGD()

    medLoadHttpPGD()

    treemapsPGD()

    dpQuartilesPGD()

    sunburst()

    # General Context for All groupSamplesTableDF

    setTable(table)

    pgTreemap()

    bouncesPGD()

    pgQuartPGD()

    activityImpactPGD()

    #todo clean up views
end
