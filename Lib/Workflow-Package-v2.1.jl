function dailyWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

#  if isdefined(:gWfShowPeakTable)
#        wfShowPeakTable = gWfShowPeakTable
#  else
      wfShowPeakTable = true
#  end
  wfShowSessionBeacons = true
  wfShowChartLoad = true
  wfShowTopUrls = true
  wfShowBrowserTreemap = true
  wfShowCountryTreemap = true
  wfShowDeviceTypeTreemap = true
  wfShowPageGroupTreemp = true
  wfShowGroupQuartiles = true
  wfShowActvitityImpact = true
  wfShowAggSession = true

  wfClearViews = true

# todo SQLFILTER Everywhere and use the view tables where possible

  openingTitle(TV,UP,SP)

  defaultBeaconCreateView(TV,UP,SP)

  setTable(UP.btView)

  try
    if (wfShowPeakTable)
        showPeakTable(TV,UP,SP;showStartTime30=true,tableRange="Daily ")
    end
  catch y
    println("showPeakTable Exception")
  end

  try
    if (wfShowSessionBeacons)
          chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart; table=UP.btView)
    end
  catch y
      println("chartConcurrentSessionsAndBeaconsOverTime Exception ",y)
  end

  try
      if (wfShowChartLoad)
          chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart;table=UP.btView)
      end
  catch y
      println("chartLoadTimes Exception ",y)
  end

  if (wfShowTopUrls)
      topUrlTableByTime(TV,UP,SP)   # use UP.pageGroup = "%" for no group
  end

  setTable(UP.btView)

  try
      if (wfShowBrowserTreemap)
          browserFamilyTreemap(TV,UP,SP)
      end
  catch y
      println("browserFamilyTreemap Exception ",y)
  end

  try
      if (wfShowCountryTreemap)
          countryTreemap(TV,UP,SP)
      end
  catch y
      println("countryTreemap Exception ",y)
  end

  try
      if (wfShowDeviceTypeTreemap) && UP.deviceType == "%"
          deviceTypeTreemap(TV,UP,SP)
      end
  catch y
    println("deviceTypeTreemap Exception ",y)
  end

  if (wfShowPageGroupTreemp) && UP.pageGroup == "%"
      pageGroupTreemap(TV,UP,SP)
  end

  if (wfShowGroupQuartiles) && UP.pageGroup == "%"
      pageGroupQuartiles(TV,UP,SP);
  end

  try
      if (wfShowActvitityImpact) && UP.pageGroup == "%"
          chartActivityImpactByPageGroup(TV.startTime, TV.endTime;n=10,table=UP.btView);
      end
  catch y
    println("chartActivityImpactByPageGroup Exception ",y)
  end

  try
      if (wfShowAggSession)
          perfsessionLength = getAggregateSessionLengthAndDurationByLoadTime(TV.startTimeUTC, TV.endTimeUTC; table=UP.btView);

          c3 = drawC3Viz(perfsessionLength; columnNames=[:load_time,:total,:avg_length], axisLabels=["Session Load Times","Completed Sessions", "Average Session Length"],dataNames=["Completed Sessions",
              "Average Session Length", "Average Session Duration"], mPulseWidget=false, chart_title="Session Load for All Pages", y2Data=["data2"], vizTypes=["area","line"]);
      end
  catch y
      println("getAggregateSessionLengthAndDurationByLoadTime Exception ",y)
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end


end

function dumpDataFieldsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconCreateView(TV,UP,SP)

    test1GNGSSDM(UP,SP)

    testUserAgentGNGSSDM(UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function dumpDataFieldsV2Workflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    defaultBeaconCreateView(TV,UP,SP)

    test1GNGSSDM(UP,SP)

    test2GNGSSDM(UP,SP)

    test3GNGSSDM(UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function findAPageViewSpikeWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    defaultBeaconCreateView(TV,UP,SP)

    try
        chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart; table=UP.btView)
    catch y
        println("chartconcurrentsessionsBeacons Exception ",y)
    end

    try
        chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart; table=UP.btView)
    catch y
        println("chartloadTime Exception ",y)
    end

    setTable(UP.btView)
    topUrlTable(TV,UP,SP)

    showPeakTable(TV,UP,SP)

    beaconViewStats(TV,UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function pageGroupDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)

    openingTitle(TV,UP,SP)

    pageGroupDetailsCreateView(TV,UP,SP,mobileView,desktopView)
    setTable(UP.btView);

    statsDF = beaconViewStats(TV,UP,SP)
    if !isdefined(:statDF)
        println("No data")
    end

    medianThreshold = statsDF[1:1,:median][1]

    showPeakTable(TV,UP,SP;showStartTime30=false,tableRange="Sample Set ")

    concurrentSessionsPGD(TV,UP,SP,mobileView,desktopView)

    loadTimesPGD(TV,UP,SP,mobileView,desktopView)

    topUrlTable(TV,UP,SP)

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

    standardReferrals(TV,UP,SP)

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

    # General Context for All

    setTable(UP.beaconTable)

    pageGroupTreemap(TV,UP,SP)

    chartLoadTimeMediansAndBounceRatesByPageGroup(TV.startTimeUTC,TV.endTimeUTC)

    pageGroupQuartiles(TV,UP,SP)

    chartActivityImpactByPageGroup(TV.startTimeUTC, TV.endTimeUTC;n=UP.limitRows)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    q = query(""" drop view if exists $(mobileView);""")
    q = query(""" drop view if exists $(desktopView);""")
    ;
end

function individualStreamlineWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  openingTitle(TV,UP,SP)

  if UP.useJson
      urlListDF = newPagesList(UP)
      finalListToUseDV = urlListDF[:urlgroup]
  else
      urlListDF = returnMatchingUrlTableV2(TV,UP)
  end

  if (SP.debugLevel > 4)
      beautifyDF(urlListDF[1:min(10,end),:])
  end

  if !UP.useJson
      # Clean up the list before using
      newListDF = urlListDF[Bool[x > UP.samplesMin for x in urlListDF[:cnt]],:]
      topUrlListDV = newListDF[:urlgroup]
      finalListToUseDV = cleanupTopUrlTable(topUrlListDV)

      if (SP.debugLevel > 4)
          println("Started with ",size(urlListDF,1), " Trimmed down to ",size(newListDF,1), " due to $(UP.samplesMin) limit")
          println("Final DV size is ",size(topUrlsDV,1))
          if (SP.debugLevel > 8)
              println(finalListToUseDV)
          end
      end

  end

  finalUrlTableOutput(TV,UP,SP,finalListToUseDV)

end

function aemLargeImagesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconCreateView(TV,UP,SP);

    joinTables = DataFrame()
    joinTables = gatherSizeData(UP,SP)
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

  defaultBeaconCreateView(TV,UP,SP)
  defaultResourceView(TV,UP)

  setTable(UP.btView)

  if (wfShowSessions)
    #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup)", chart_info = [TV.timeString])
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
    myFilter = SQLFilter[ilike("params_u",UP.urlRegEx)]

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
      standardReferrals(TV,UP,SP)
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

function findATimeSpikeWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  #Turn sections on / off to debug
  wfShowLongTimes = true
  wfShowBelowThreshold = false # bad output - need ticket
  wfShowLoadTimes = true
  wfShowDurationByDate = true
  wfShowTopUrls = true
  wfShowSessionsAndBeacons = true
  wfShowLongTimes = true

  wfClearViews = true

  openingTitle(TV,UP,SP)

  defaultBeaconCreateView(TV,UP,SP)

  statsDF = DataFrame()
  localStats2 = DataFrame()

  statsDF = beaconViewStats(TV,UP,SP)
  localStats2 = localStatsFATS(TV,UP,statsDF)

  if (wfShowLongTimes)
    longTimesFATS(TV,UP,localStats2)
  end

  setTable(UP.btView)

  if (wfShowBelowThreshold)
    chartPercentageOfBeaconsBelowThresholdStackedBar(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowLoadTimes)
    #displayTitle(chart_title = "Median Load Times for $(productPageGroup)", chart_info = [TV.timeString])
    chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowDurationByDate)
    chartSessionDurationQuantilesByDatepart(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowTopUrls)
    chartTopURLsByLoadTime(TV.startTimeUTC, TV.endTimeUTC)
  end

  if (wfShowSessionsAndBeacons)
    #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup)", chart_info = [TV.timeString])
    chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowLongTimes)
    graphLongTimesFATS(localStats2)
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end

end

function aemLargeResourcesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,minimumEncoded::Int64)

    openingTitle(TV,UP,SP)

    #Turn sections on / off to debug
    wfShowBigPagesByFileType = true
    wfShowLeftOvers = true
    wfShowLeftOversDetails = true

    defaultBeaconCreateView(TV,UP,SP)

    if (wfShowBigPagesByFileType)
        bigPageSizeDetails(TV,UP,SP,"%jpg";minEncoded=minimumEncoded)
        bigPageSizeDetails(TV,UP,SP,"%png";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%svg";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%mp3";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%mp4";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%gif";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%wav";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%jog";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%js";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%js?%";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%css";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%ttf";minEncoded=minimumEncoded);
        bigPageSizeDetails(TV,UP,SP,"%woff";minEncoded=minimumEncoded);
    end

    if (wfShowLeftOvers)
        try
            lookForLeftOversALR(UP,linesOutput)
        catch y
            println("lookForLeftOversALR Exception ",y)
        end
    end

    if (wfShowLeftOversDetails)
        try
            lookForLeftOversDetailsALR(UP,linesOutput)
        catch y
            println("lookForLeftOversALR Exception ",y)
        end
    end

    if (wfClearViews)
        q = query(""" drop view if exists $(UP.btView);""")
        q = query(""" drop view if exists $(UP.rtView);""")
    end

end

function findAnyResourceWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  #Turn sections on / off to debug
  wfShowResourcesByParamsU = true
  wfShowResourcesByUrl = true
  wfShowResourcesByUrls = true
  wfShowResourcesStats = true
  wfShowResourcesAllFields = false

  wfClearViews = true

  openingTitle(TV,UP,SP)

  if (wfShowResourcesByParamsU)
      displayMatchingResourcesByParentUrl(TV,UP,SP)
  end

  if (wfShowResourcesByUrl)
      displayMatchingResourcesByUrl(TV,UP,SP)
  end

  defaultBeaconCreateView(TV,UP,SP)

  if (wfShowResourcesByUrls)
      displayMatchingResourcesByUrls(TV,UP,SP)
  end

  if (wfShowResourcesStats)
      displayMatchingResourcesStats(TV,UP,SP)
  end

  if (wfShowResourcesAllFields)
      displayMatchingResourcesAllFields(TV,UP,SP)
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end

end

function showRequestsForLargePagesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  #Turn sections on / off to debug
  wfShowBigPage2 = true
  wfShowBigPage3 = true
  wfShowBigPage4 = true
  wfShowBigPage5 = true
  wfShowBigPage6 = true

  wfClearViews = true

  defaultBeaconCreateView(TV,UP,SP)

  minSizeBytes = bigPages1SRFLP(TV,UP,SP)

  if (wfShowBigPage2)
      bigPages2SRFLP(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage3)
      bigPages3SRFLP(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage4)
      bigPages4SRFLP(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage5)
      bigPages5SRFLP(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage6)
      bigPages6SRFLP(TV,UP,SP,minSizeBytes)
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end

end
