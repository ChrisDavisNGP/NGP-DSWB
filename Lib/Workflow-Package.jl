function dailyWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if isdefined(:gRunArray) && !gRunArray[1]
        wfShowPeakTable = false
    else
        wfShowPeakTable = true
    end

    if isdefined(:gRunArray)  && !gRunArray[2]
        wfShowSessionBeacons = false
    else
        wfShowSessionBeacons = true
    end

    if isdefined(:gRunArray)  && !gRunArray[3]
      wfShowChartLoad = false
    else
      wfShowChartLoad = true
    end

    if isdefined(:gRunArray)  && !gRunArray[4]
      wfShowTopUrls = false
    else
      wfShowTopUrls = true
    end

    if isdefined(:gRunArray)  && !gRunArray[5]
      wfShowBrowserTreemap = false
    else
      wfShowBrowserTreemap = true
    end

    if isdefined(:gRunArray)  && !gRunArray[6]
      wfShowCountryTreemap = false
    else
      wfShowCountryTreemap = true
    end

    if isdefined(:gRunArray)  && !gRunArray[7]
      wfShowDeviceTypeTreemap = false
    else
      wfShowDeviceTypeTreemap = true
    end

    if isdefined(:gRunArray)  && !gRunArray[8]
      wfShowPageGroupTreemp = false
    else
      wfShowPageGroupTreemp = true
    end

    if isdefined(:gRunArray)  && !gRunArray[9]
      wfShowGroupQuartiles = false
    else
      wfShowGroupQuartiles = true
    end

    if isdefined(:gRunArray)  && !gRunArray[10]
      wfShowActvitityImpact = false
    else
      wfShowActvitityImpact = true
    end

    if isdefined(:gRunArray)  && !gRunArray[11]
      wfShowAggSession = false
    else
      wfShowAggSession = true
    end

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

    urlCountPrintTable(UP,SP)

    agentCountPrintTable(UP,SP)

    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
    ;

end

function studyRangeOfStatsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if isdefined(:gRunArray) && !gRunArray[1]
        wfPageGroupGraph = false
    else
        wfPageGroupGraph = true
    end

    if isdefined(:gRunArray) && !gRunArray[2]
        wfStatsGraphMedian = false
    else
        wfStatsGraphMedian = true
    end

    if isdefined(:gRunArray) && !gRunArray[3]
        wfStatsGraphQ = false
    else
        wfStatsGraphQ = true
    end

    if isdefined(:gRunArray) && !gRunArray[4]
        wfStatsGraphKurt = false
    else
        wfStatsGraphKurt = true
    end

    if isdefined(:gRunArray) && !gRunArray[5]
        wfStatsGraphSkew = false
    else
        wfStatsGraphSkew = true
    end

    if isdefined(:gRunArray) && !gRunArray[6]
        wfStatsGraphEntropy = false
    else
        wfStatsGraphEntropy = true
    end

    if isdefined(:gRunArray) && !gRunArray[7]
        wfStatsGraphModes = false
    else
        wfStatsGraphModes = true
    end

    wfClearViews = true

    if isdefined(:explain) && explain
        explainStudyRangeOfStats()
        return
    end

    openingTitle(TV,UP,SP)

    defaultBeaconCreateView(TV,UP,SP)
    setTable(UP.btView)

    if wfPageGroupGraph
        rawTimeDF = fetchGraph7Stats(UP)
        #beautifyDF(rawTimeDF[1:3,:])
        drawC3VizConverter(UP,rawTimeDF;graphType=7)
    end

    if (wfStatsGraphQ ||
        wfStatsGraphKurt ||
        wfStatsGraphSkew ||
        wfStatsGraphModes ||
        wfStatsGraphMedian ||
        wfStatsGraphEntropy
       )
        AllStatsDF = createAllStatsDF(TV,UP,SP)
    end


    if wfStatsGraphMedian
        drawC3VizConverter(UP,AllStatsDF;graphType=1)
    end

    if wfStatsGraphQ
        drawC3VizConverter(UP,AllStatsDF;graphType=2)
    end

    if wfStatsGraphKurt
        drawC3VizConverter(UP,AllStatsDF;graphType=3)
    end

    if wfStatsGraphSkew
        drawC3VizConverter(UP,AllStatsDF;graphType=4)
    end

    if wfStatsGraphEntropy
        drawC3VizConverter(UP,AllStatsDF;graphType=5)
    end

    if wfStatsGraphModes
        drawC3VizConverter(UP,AllStatsDF;graphType=6)
    end

    if wfClearViews
        q = query(""" drop view if exists $(UP.btView);""")
        q = query(""" drop view if exists $(UP.rtView);""")
    end

end

function dumpDataFieldsV2Workflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    defaultBeaconCreateView(TV,UP,SP)

    urlCountPrintTable(UP,SP)

    urlParamsUCountPrintTable(UP,SP)

    paramsUCountPrintTable(UP,SP)

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

    datePartQuartiles(TV,UP)

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
      urlListDF = newPagesList(UP,SP)
      listToUseDV = urlListDF[:urlgroup] * "%"
      finalListToUseDV = cleanupTopUrlTable(listToUseDV)
  else
      urlListDF = returnMatchingUrlTableV2(TV,UP)
  end

  if (SP.debugLevel > 8)
      beautifyDF(urlListDF[1:min(10,end),:])
  end

  if !UP.useJson
      # Clean up the list before using
      newListDF = urlListDF[Bool[x > UP.samplesMin for x in urlListDF[:cnt]],:]
      topUrlListDV = newListDF[:urlgroup]
      finalListToUseDV = cleanupTopUrlTable(topUrlListDV)

      if (SP.debugLevel > 4)
          println("Started with ",size(urlListDF,1), " Trimmed down to ",size(newListDF,1), " due to $(UP.samplesMin) limit")
          println("Final DV size is ",size(finalListToUseDV,1))
      end

  end

  if (SP.debugLevel > 4)
      for item in finalListToUseDV
          println(item)
      end
  end

  finalUrlTableOutput(TV,UP,SP,finalListToUseDV)

end

function aemLargeImagesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconCreateView(TV,UP,SP);

    joinTables = DataFrame()
    joinTables = gatherSizeDataToDF(UP,SP)
    ;

    joinTableSummary = DataFrame()
    joinTableSummary = createJoinTableSummary(SP,joinTableSummary,joinTables)
    ;

    i = 0
    for row in eachrow(joinTableSummary)
        i += 1
        joinTablesDetailsPrintTable(TV,UP,SP,joinTableSummary,i)
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
    #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(UP.pageGroup)", chart_info = [TV.timeString])
    chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowMedLoadTimes)
    chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart, pageUrl=UP.urlRegEx)
  end

  if (wfShowTopPages)
    countUrlgroupPrintTable(TV,UP,SP)
  end

  if (wfShowTopUrlPages)
    countParamUBtViewPrintTable(TV,UP,SP)
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
    #displayTitle(chart_title = "Median Load Times for $(UP.pageGroup)", chart_info = [TV.timeString])
    chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
  end

  if (wfShowDurationByDate)
      datePartQuartiles(TV,UP)
  end

  if (wfShowTopUrls)
    chartTopURLsByLoadTime(TV.startTimeUTC, TV.endTimeUTC)
  end

  if (wfShowSessionsAndBeacons)
    #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(UP.pageGroup)", chart_info = [TV.timeString])
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
    wfClearViews = true

    defaultBeaconCreateView(TV,UP,SP)

    if (wfShowBigPagesByFileType)
        bigPagesSizePrintTable(TV,UP,SP,"%jpg";minEncoded=minimumEncoded)
        bigPagesSizePrintTable(TV,UP,SP,"%png";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%svg";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%mp3";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%mp4";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%gif";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%wav";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%jog";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%js";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%js?%";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%css";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%ttf";minEncoded=minimumEncoded);
        bigPagesSizePrintTable(TV,UP,SP,"%woff";minEncoded=minimumEncoded);
    end

    if (wfShowLeftOvers)
        try
            lookForLeftOversPrintTable(UP,SP)
        catch y
            println("lookForLeftOversPrintTable Exception ",y)
        end
    end

    if (wfShowLeftOversDetails)
        try
            lookForLeftOversDetailsPrintTable(UP,SP)
        catch y
            println("lookForLeftOversPrintTable Exception ",y)
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

  if (wfShowResourcesByUrl)
      displayMatchingResourcesByUrlRtPrintTable(TV,UP,SP)
  end

  if (wfShowResourcesByParamsU)
      displayMatchingResourcesByParentUrlPrintTable(TV,UP,SP)
  end

  defaultBeaconCreateView(TV,UP,SP)

  if (wfShowResourcesByUrls)
      displayMatchingResourcesByUrlBtvRtPrintTables(TV,UP,SP)
  end

  if (wfShowResourcesStats)
      displayMatchingResourcesStatsPrintTable(TV,UP,SP)
  end

  if (wfShowResourcesAllFields)
      displayMatchingResourcesAllFieldsPrintTable(TV,UP,SP)
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
      bigPages2PrintTable(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage3)
      bigPages3PrintTable(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage4)
      bigPages4PrintTable(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage5)
      bigPages5PrintTable(TV,UP,SP,minSizeBytes)
  end

  if (wfShowBigPage6)
      bigPages6PrintTable(TV,UP,SP,minSizeBytes)
  end

  if (wfClearViews)
    q = query(""" drop view if exists $(UP.btView);""")
    q = query(""" drop view if exists $(UP.rtView);""")
  end

end

function determineBeaconsGroupingWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    short_results = getLatestResults(hours=0, minutes=5, table_name=UP.beaconTable)
    size(short_results)

    groups, group_summary = groupResults(short_results, dims=2, showProgress=true)
    beautifyDF(group_summary)

    gbg = getBestGrouping(short_results, group_summary)
    beautifyDF(gbg)

    soasta_results = getLatestResults(table_name=UP.beaconTable, hours=4);
    size(soasta_results)

    groups, group_summary = groupResults(soasta_results, dims=2, showProgress=true);
    beautifyDF(group_summary)

    gbg = getBestGrouping(soasta_results, group_summary)
    beautifyDF(gbg)
end

function beaconAndRtCountsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    bt = UP.beaconTable
    rt = UP.resourceTable

    if (SP.reportLevel > 9)
        bc = getBeaconCount();
        allBeacons = getBeaconsFirstAndLast();
        #bcType = getBeaconCountByType();
        beautifyDF(names!(bc[:,:],[Symbol("Beacon Count")]))
        beautifyDF(allBeacons)
        #beautifyDF(names!(bcType[:,:],[Symbol("Beacon Type"),Symbol("Beacon Count")]))
    end

    UP.pageGroup = "News Article"
    UP.limitRows = 10
    t1DF = defaultLimitedBeaconsToDF(TV,UP,SP)
    standardChartTitle(TV,UP,SP,"$(UP.pageGroup) Page View Dump")
    beautifyDF(t1DF)

    t2DF = errorBeaconsToDF(TV,UP,SP)
    if (size(t2DF,1) > 0)
        standardChartTitle(TV,UP,SP,"Error Beacon Dump")
        beautifyDF(t2DF)
    end

    rtcnt = query("""select count(*) from $rt""");
    maxRt = query("""select max("timestamp") from $rt""");
    minRt = query("""select min("timestamp") from $rt""");

    minStr = msToDateTime(minRt[1,:min]);
    maxStr = msToDateTime(maxRt[1,:max]);

    printDf = DataFrame();
    printDf[:minStr] = minStr;
    printDf[:maxStr] = maxStr;

    standardChartTitle(TV,UP,SP,"Resource Information")
    beautifyDF(names!(rtcnt[:,:],[Symbol("Resource Timing Count")]))
    beautifyDF(names!(printDf[:,:],[Symbol("First RT"),Symbol("Last RT")]))
    ;

end

function weeklyCTOReportWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    try
        chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("chartConcurrentSessionsAndBeaconsOverTime Exception ",y)
    end

    try
        chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
    catch y
        println("chartLoadTimes Exception ",y)
    end

    showPeakTable(TV,UP,SP;showStartTime30=true)

    topUrlTableByTime(TV,UP,SP)

    pageGroupQuartiles(TV,UP,SP);

    chartActivityImpactByPageGroup(TV.startTimeUTC, TV.endTimeUTC;n=10);

    try
        pageGroupTreemap(TV,UP,SP)
    catch y
        println("pageGroupTreemap Exception ",y)
    end

    try
        deviceTypeTreemap(TV,UP,SP)
    catch y
        println("deviceTypeTreemap Exception ",y)
    end

    try
        browserFamilyTreemap(TV,UP,SP)
    catch y
        println("browserFamilyTreemap Exception ",y)
    end

    try
        countryTreemap(TV,UP,SP)
    catch y
        println("chartConcurSessions Exception ",y)
    end
end

function pageGroupAnimationWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    openingTitle(TV,UP,SP)

    bt = UP.beaconTable
    btv = UP.btView

    # Create view to query only product page_group
    defaultBeaconCreateView(TV,UP,SP)

    setTable(btv)

    # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
    # where beacon_type = 'page view'
    # t1DF = query("""SELECT count(*) FROM $btv""")

    retailer_results = getLatestResults(hours=10, minutes=0, table_name="$(btv)")
    size(retailer_results)

    # drop some of the fields to make the output easier to read

    #delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model,:referrer])
    delete!(retailer_results,[:geo_rg,:geo_city,:geo_org,:user_agent_major,:user_agent_osversion,:user_agent_os,:user_agent_model])

    doit(retailer_results, showDimensionViz=true, showProgress=true);

    q = query(""" drop view if exists $btv;""")
    ;

end

function largeResourcesForImageMgrWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconCreateView(TV,UP,SP)

    largeResourceFileTypePrint(TV,UP,SP,"%jpg")
    largeResourceFileTypePrint(TV,UP,SP,"%png")
    largeResourceFileTypePrint(TV,UP,SP,"%jpeg")
    largeResourceFileTypePrint(TV,UP,SP,"%gif")
    largeResourceFileTypePrint(TV,UP,SP,"%imviewer")
    largeResourceFileTypePrint(TV,UP,SP,"%svg")
    largeResourceFileTypePrint(TV,UP,SP,"%jpeg")

    q = query(""" drop view if exists $(UP.btView);""")
    ;

end

function resourcesDetailsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    saveSpShowLines = SP.showLines
    SP.showLines = 3
    resourceMatched(TV,UP,SP)
    resourceSummaryAllFields(TV,UP,SP)

    SP.showLines = saveSpShowLines
    resourceSummary(TV,UP,SP)

    minimumEncoded = 0
    resourceSize(TV,UP,SP;minEncoded=minimumEncoded)

    resourceScreenPrintTable(TV,UP,SP)

    resourceSummaryDomainUrl(TV,UP,SP)

    resourceTime1(TV,UP,SP)

    resourceTime2(TV,UP,SP)

    resourceTime3(TV,UP,SP)

end

function findAnyAggResourcesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    UP.resRegEx = "%www.nationalgeographic.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%news.nationalgeographic.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%adservice.google%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%googlesyndication.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%yahoo.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%innovid.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%moatads.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%fls.doubleclick%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%unrulymedia.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%googleapis.com%"   # Google Doubleclick related
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%2mdn.net%"   # Google Doubleclick related
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%doubleclick.net%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%monetate_off%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%monetate.net%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%MonetateTests%"
    findAnyResourceWorkflow(TV,UP,SP)
    ;

end

function findAdsResourcesWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    UP.resRegEx = "%v1.9.3%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%v1.9.5%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%cdn1.spotible.com%"
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%fng-ads.fox.com/fw_ads%" # Oct 19 freewheel ads
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%player.foxdcg.com/ngp-freewheel%" # Oct 19 freewheel ads
    findAnyResourceWorkflow(TV,UP,SP)

    UP.resRegEx = "%pr-bh.ybp.yahoo.com%"
    findAnyResourceWorkflow(TV,UP,SP)
    ;

end

function statsAndTreemapsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    localTableDF = statsAndTreemapsData(TV,UP,SP)

    if nrow(localTableDF) == 0
        displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
        return
    end

    if (SP.debugLevel > 8)
        println("Individual part 1 done with ", nrow(localTableDF), " records")
    end

    #statsOldDF = statsAndTreemapsStats(TV,UP,SP,localTableDF)
    #beautifyDF(statsOldDF)

    statsDF = timeBeaconStats(TV,UP,SP,localTableDF;showAdditional=true,showShort=false,useQuartile=true)

    topPageUrlDF = statsAndTreemapsFinalData(TV,UP,SP,statsDF)

    statsAndTreemapsOutput(TV,UP,SP,topPageUrlDF)

end

function urlAutoIndividualWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    localTableDF = statsAndTreemapsData(TV,UP,SP)

    if nrow(localTableDF) == 0
        displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
        return
    end

    if (SP.debugLevel > 8)
        println("Individual part 1 done with ", nrow(localTableDF), " records")
    end

    # Stats on the data
    statsDF = timeBeaconStats(TV,UP,SP,localTableDF;showAdditional=true,usePercent)
    UP.timeLowerMs = convert(Int64,statsDF[1:1,:rangeLower][1])
    UP.timeUpperMs = convert(Int64,statsDF[1:1,:rangeUpper][1])

    if (SP.debugLevel > 6)
        println("Individual selecting from $(UP.timeLowerMs) to $(UP.timeUpperMs)")
    end

    showAvailableSessionsStreamline(TV,UP,SP,localTableDF)

end
