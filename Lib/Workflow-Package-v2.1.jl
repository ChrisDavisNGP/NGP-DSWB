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

function dumpDataFieldsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultTableGNGSSDM(TV,UP)

    test1GNGSSDM(UP,LV)

    testUserAgentGNGSSDM(UP,LV)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function pageGroupDetailsWorkFlow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

    pageGroupDetailsTables(TV,UP,mobileView,desktopView)
    setTable(UP.btView);

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = 0.0
    statsDF = statsPGD(TV,UP)
    medianThreshold = statsDF[1:1,:median][1]

    showPeakTable(TV,UP,SP;showStartTime30=false,showStartTime90=false,tableRange="Sample Set ")

    concurrentSessionsPGD(TV,UP,SP,mobileView,desktopView)

    loadTimesPGD(TV,UP,SP,mobileView,desktopView)

    topUrlTable(UP.btView,UP.pageGroup,TV.timeString; limit=UP.limitRows)

    try
        chartPercentageOfBeaconsBelowThresholdStackedBar(TV.startTimeUTC, TV.endTimeUTC, TV.datePart; threshold = medianThreshold)
    catch y
        println("chartPercentageOfBeaconsBelowThresholdStackedBar exception ",y)
    end

    try
        perfsessionLength = getAggregateSessionLengthAndDurationByLoadTime(TV.startTimeUTC, TV.endTimeUTC);

        c3 = drawC3Viz(perfsessionLength; columnNames=[:load_time,:total,:avg_length], axisLabels=["Session Load Times","Completed Sessions", "Average Session Length"],
             dataNames=["Completed Sessions","Average Session Length", "Average Session Duration"], mPulseWidget=false,
             chart_title="Session Stats for $(UP.pageGroup) Page Group", y2Data=["data2"], vizTypes=["area","line"]);
    catch y
        println("sessionLoadPGD Exception ",y)
    end

    loadTimesParamsUPGD(TV,UP)

    try
        chartMedianLoadTimesByDimension(TV.startTimeUTC, TV.endTimeUTC,dimension=:geo_cc,minPercentage=0.6)
        chartMedianLoadTimesByDimension(TV.startTimeUTC, TV.endTimeUTC; dimension=:user_agent_device_type, n=15, orderBy="frontend", minPercentage=0.001)
        printDF = getMedianLoadTimesByDimension(TV.startTimeUTC, TV.endTimeUTC; dimension=:user_agent_device_type, n=15, orderBy="frontend", minPercentage=0.001)
        beautifyDF(printDF)
    catch y
        println("medianTimesPGD Exception ",y)
    end


    customRefPGD(TV,UP)

    stdRefPGD(TV,UP)

    try
        chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:http_referrer,minPercentage=0.5)
        chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:params_r,minPercentage=0.5)
        chartTopN(TV.startTimeUTC, TV.endTimeUTC; variable=:landingPages)
    catch y
        println("cell chartSlowestUrls Exception ",y)
    end

    treemapsPGD(TV,UP)

    datePartQuartiles(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)

    try
        result10 = getAllPaths(TV.startTimeUTC, TV.endTimeUTC; n=60, f=getAbandonPaths);
        drawSunburst(result10[1]; totalPaths=result10[3])
    catch y
        println("sunburst Exception ",y)
    end

    # General Context for All groupSamplesTableDF

    setTable(UP.beaconTable)

    pageGroupTreemap(TV,UP,SP)

    chartLoadTimeMediansAndBounceRatesByPageGroup(TV.startTimeUTC,TV.endTimeUTC)

    pageGroupQuartiles(TV,UP,SP)

    chartActivityImpactByPageGroup(TV.startTimeUTC, TV.endTimeUTC;n=UP.limitRows)

    ;
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
