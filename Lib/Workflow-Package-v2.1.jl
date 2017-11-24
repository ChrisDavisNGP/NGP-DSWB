function dailyWorkFlow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  try
    showPeakTable(TV,UP,SP;showStartTime30=true,showStartTime90=false,tableRange="Daily ")
  catch y
    println("showPeakTable Exception")
  end

  try
      chartConcurrentSessionsAndBeaconsOverTime(TV.startTime, TV.endTime, TV.datePart)
  catch y
      println("chartConcurrentSessionsAndBeaconsOverTime Exception ",y)
  end

  try
      chartLoadTimes(TV.startTime, TV.endTime, TV.datePart)
  catch y
      println("chartLoadTimes Exception ",y)
  end

  topUrlTableByTime(TV,UP,SP)   # use UP.pageGroup = "%" for no group

  try
      browserFamilyTreemap(TV,UP)
  catch y
      println("browserFamilyTreemap Exception ",y)
  end

  try
      countryTreemap(TV,UP)
  catch y
      println("countryTreemap Exception ",y)
  end

  try
    deviceTypeTreemap(TV,UP)
  catch y
    println("deviceTypeTreemap Exception ",y)
  end

  pageGroupTreemap(TV,UP,SP)

  pageGroupQuartiles(TV,UP,SP);

  try
    chartLoadTimes(TV.startTime, TV.endTime, :hour)
  catch y
    println("chartLoadTimes 2 Exception ",y)
  end

  try
    chartActivityImpactByPageGroup(TV.startTime, TV.endTime;n=10);
  catch y
    println("chartActivityImpactByPageGroup Exception ",y)
  end

  try
      perfsessionLength = getAggregateSessionLengthAndDurationByLoadTime(TV.startTime, TV.endTime);

      c3 = drawC3Viz(perfsessionLength; columnNames=[:load_time,:total,:avg_length], axisLabels=["Session Load Times","Completed Sessions", "Average Session Length"],dataNames=["Completed Sessions",
          "Average Session Length", "Average Session Duration"], mPulseWidget=false, chart_title="Session Load for All Pages", y2Data=["data2"], vizTypes=["area","line"]);
  catch y
      println("getAggregateSessionLengthAndDurationByLoadTime Exception ",y)
  end

end

function pageGroupDetailsWorkFlow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

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

    pgTreemap(TV,UP,SP)

    bouncesPGD(TV)

    pgQuartPGD(TV,UP,SP)

    activityImpactPGD(TV,UP)

    #todo clean up views
end

function individualStreamlineWorkFlow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

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
