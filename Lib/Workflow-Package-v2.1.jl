function pageGroupDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

    pageGroupDetailsTables(TV,UP,mobileView,desktopView)
    setTable(UP.btView);

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = 0.0
    statsDF = statsPGD(TV,UP)
    medianThreshold = statsDF[1:1,:median][1]

    peakPGD(TV,UP,SP)

    concurrentSessionsPGD(TV,UP,SP,mobileView,desktopView)

    loadTimesPGD(TV,UP,SP,mobileView,desktopView)

    topUrlPGD(TV,UP)

    thresholdChartPGD(medianThreshold)

    sessionLoadPGD(TV,UP)

    loadTimesParamsUPGD(TV,UP)

    medianTimesPGD(TV)

    customRefPGD(TV,UP)

    stdRefPGD(TV,UP)

    medLoadHttpPGD(TV)

    treemapsPGD(TV,UP)

    dpQuartilesPGD(TV)

    sunburst(TV)

    # General Context for All groupSamplesTableDF

    setTable(UP.beaconTable)

    pgTreemap(TV,UP)

    bouncesPGD(TV)

    pgQuartPGD(TV,UP)

    activityImpactPGD(TV,UP)

    #todo clean up views
end

function individualStreamlineWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  urlListDF = returnMatchingUrlTableV2(TV,UP)
  if (SP.debugLevel > 4)
      beautifyDF(urlListDF[1:min(10,end),:])
  end

  newListDF = urlListDF[Bool[x > UP.samplesMin for x in urlListDF[:cnt]],:]
  topUrlList = newListDF[:urlgroup]
  topUrls = cleanupTopUrlTable(topUrlList)
  if (SP.debugLevel > 8)
      println(topUrls)
  end

  finalUrlTableOutput(TV,UP,SP,topUrls)

end
