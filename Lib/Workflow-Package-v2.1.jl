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
      browserFamilyTreemap(TV,UP,SP)
  catch y
      println("browserFamilyTreemap Exception ",y)
  end

  try
      countryTreemap(TV,UP,SP)
  catch y
      println("countryTreemap Exception ",y)
  end

  try
    deviceTypeTreemap(TV,UP,SP)
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

    defaultBeaconView(TV,UP,SP)

    test1GNGSSDM(UP,SP)

    testUserAgentGNGSSDM(UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function dumpDataFieldsV2Workflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconView(TV,UP,SP)

    test1GNGSSDM(UP,SP)

    test2GNGSSDM(UP,SP)

    test3GNGSSDM(UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function findAPageViewSpikeWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    firstAndLastBeaconReport(TV,UP)

    try
        setTable(UP.btView)
        chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("sessionsBeacons Exception ",y)
    end

    try
        setTable(UP.btView)
        chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("loadTime Exception ",y)
    end

    setTable(UP.btView)
    topUrlTable(UP.btView,UP.pageGroup,TV.timeString;limit=15)

    setTable(UP.btView)
    showPeakTable(TV,UP,SP)

    statsTableFAPVSB(TV,UP)

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

    treemapsPGD(TV,UP,SP)

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

function aemLargeImagesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconView(TV,UP,SP);

    joinTables = DataFrame()
    joinTables = gatherSizeData(TV,UP,SP)
    ;

    joinTableSummary = DataFrame()
    joinTableSummary = createJoinTableSummary(SP,joinTableSummary,joinTables)
    ;

    i = 0
    for row in eachrow(joinTableSummary)
        i += 1
        joinTablesDetailsPrint(TV,UP,SP,joinTableSummary,i)
        statsDetailsPrint(TV,UP,SP,joinTableSummary,i)
        if (i >= SP.showLines)
            break;
        end
    end
    ;

    q = query(""" drop view if exists $(UP.btView);""")
    ;

end

function urlDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  #Turn sections on / off to debug
  wfShowSessions = true
  wfShowMedLoadTimes = true
  wfShowTopPages = true
  wfShowTopUrlPages = true
  wfShowChartTopPage = false # broken - need ticket
  wfShowMedLoadUrl = true
  wfShowChartCacheHitRatio = false # broken - need ticket
  wfShowChartTopPageResources = true
  wfShowChartResResponse = false # broken - need ticket
  wfShowChartResUrlResponse = false # broken like the one above
  wfShowPercentBelow = false # broken - need ticket
  wfShowBounceByUrl = true
  wfShowResResponseTime = false # broken - need ticket
  wfShowAggSessionLength = true
  wfShowMedLoadByDevice = true
  wfShowMedLoadByGeo = true
  wfShowCustomReferrers = true
  wfShowReferrers = true
  wfShowMedLoadByReferrers = true
  wfShowTreemaps = true
  wfShowSunburst = true

  wfClearViews = true

  defaultBeaconView(TV,UP,SP)

  defaultResourceView(TV,UP)

  if (wfShowSessions)
    #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup)", chart_info = [timeString])
    chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowMedLoadTimes)
    chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart, pageUrl=UP.urlRegEx)
  end

  if (wfShowTopPages)
    topPageViewsUDB(TV,UP,SP)
  end

  if (wfShowTopUrlPages)
    topUrlPageViewsUDB(TV,UP,SP)
  end

  # Currently broken - need ticket to Soasta
  if (wfShowChartTopPage)
    #fail thresholdValues = [1000, 10000, 100000]
    #fail chartRes = chartResponseTimesVsTargets(startTime, endTime, datePart, thresholdValues)
    setTable(UP.rtView, tableType = "RESOURCE_TABLE")
    try
        #chartRes = chartTopPageResourcesSummary(TV.startTimeUTC, TV.endTimeUTC; beaconTable=UP.btView, resourceTable=UP.rtView,n=10,minPercentage=0.05)
        chartRes = chartTopPageResourcesSummary(TV.startTimeUTC, TV.endTimeUTC)
        display(chartRes[1:20,:])
    catch y
        println("chartTop LocalTable Exception ",y)
    end

    setTable(UP.btView)
  end

  if (wfShowMedLoadUrl)
    chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:url,minPercentage=0.1)
    chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:params_u,minPercentage=0.1)
  end

  # Known bad - need ticket
  if (wfShowChartCacheHitRatio)
    setTable(UP.rtView, tableType = "RESOURCE_TABLE")
    chartRes = chartCacheHitRatioByUrl(TV.startTimeUTC, TV.endTimeUTC, minPercentage=0.1)
    setTable(UP.btView)
    display(chartRes)
  end

  # Need to test
  if (wfShowChartTopPageResources)
    #setTable(localTableRt, tableType = "RESOURCE_TABLE")
    #chartTopPageResourcesSummary(startTime, endTime)
    #chartTopPageResourcesSummary(startTime, endTime, datepart = datePart)
    #setTable(localTable)
    #display(chartRes)
  end

  # known bad - need ticket
  if (wfShowChartResResponse)
    setTable(UP.rtView, tableType = "RESOURCE_TABLE")
    chartRes = chartResourceResponseTimeDistribution(TV.startTimeUTC, TV.endTimeUTC,url=UP.urlRegEx)
    setTable(UP.btView)
    display(chartRes)
  end

  if (wfShowChartResUrlResponse)
    setTable(UP.rtView, tableType = "RESOURCE_TABLE")
    #chartRes = chartResourceResponseTimeDistribution(startTime, endTime,url="http://phenomena.nationalgeographic.com/files/2016/05/BH91DH.jpg")
    chartRes = chartResourceResponseTimeDistribution(TV.startTimeUTC, TV.endTimeUTC)
    setTable(UP.btView)
    display(chartRes)
  end

  # known bad - need ticket
  if (wfShowPercentBelow)
    println(TV.startTimeUTC," and ",TV.endTimeUTC)
    #chartPercentageOfBeaconsBelowThresholdStackedBar(TV.startTimeUTC, TV.endTimeUTC, :hour; pageGroup=UP.pageGroup, threshold=6000, url=UP.urlRegEx)
    chartPercentageOfBeaconsBelowThresholdStackedBar(TV.startTimeUTC, TV.endTimeUTC, :hour)
  end

  if (wfShowBounceByUrl)
    chartBouncesVsLoadTimes(TV.startTimeUTC, TV.endTimeUTC, url=UP.urlFull)
    #chartBouncesVsLoadTimes(startTime, endTime)
  end

  # known bad - need ticket
  if (wfShowResResponseTime)
    setTable(UP.rtView, tableType = "RESOURCE_TABLE")
    responseDist = getResourceResponseTimeDistribution(TV.startTimeUTC,TV.endTimeUTC, n=15, url=UP.urlFull)
    setTable(UP.btView)
    display(responseDist)
  end

  if (wfShowAggSessionLength)
    myFilter = SQLFilter[like("params_u",UP.urlRegEx)]

    perfsessionLength = getAggregateSessionLengthAndDurationByLoadTime(TV.startTimeUTC, TV.endTimeUTC; filters=myFilter)
    c3 = drawC3Viz(perfsessionLength; columnNames=[:load_time,:total,:avg_length], axisLabels=["Page Load Times","Completed Sessions", "Average Session Length"],dataNames=["Completed Sessions",
        "Average Session Length", "Average Session Duration"], mPulseWidget=false, chart_title="Top URL Page Load for $(UP.pageGroup) Page Group", y2Data=["data2"], vizTypes=["area","line"])
  end

  if (wfShowMedLoadByDevice)
    chartMedianLoadTimesByDimension(TV.startTimeUTC, TV.endTimeUTC; dimension=:user_agent_device_type, n=15, orderBy="frontend", minPercentage=0.001)
  end

  if (wfShowMedLoadByGeo)
    chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:geo_cc,minPercentage=2.5,n=10)
  end

  if (wfShowCustomReferrers)
    localTable = UP.btView
    productPageGroup = UP.pageGroup
    customRefPGD(TV,UP)
  end

  if (wfShowReferrers)
    stdRefPGD(TV,UP)
  end

  if (wfShowMedLoadByReferrers)
    chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:http_referrer,minPercentage=0.5)
    t1 = getMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:http_referrer,minPercentage=0.5)
    display(t1)

    chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:params_r,minPercentage=0.5)
    t2 = getMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:params_r,minPercentage=0.5)
    display(t2)
  end

  if (wfShowTreemaps)
    treemapsPGD(TV,UP,SP)
  end

  if (wfShowSunburst)
    result10 = getAllPaths(TV.startTimeUTC, TV.endTimeUTC; n=30, f=getAbandonPaths,useurls=true);
    drawSunburst(result10[1]; totalPaths=result10[3])
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end

end
