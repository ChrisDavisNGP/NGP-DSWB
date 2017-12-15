function criticalPathAggregationMain(TV::TimeVars,UP::UrlParams,SP::ShowParams)
  try

      localTableDF = DataFrame()
#      localTableRtDF = DataFrame()
      statsDF = DataFrame()

      localTableDF = defaultBeaconsToDF(TV,UP,SP)
      recordsFound = nrow(localTableDF)

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
          return
      end

      # Stats on the data
      statsDF = beaconStats(TV,UP,SP,localTableDF;showAdditional=true)
      UP.timeLowerMs = round(statsDF[1:1,:median][1] * 0.90)
      UP.timeUpperMs = round(statsDF[1:1,:median][1] * 1.10)

      criticalPathStreamline(TV,UP,SP,localTableDF)

  catch y
      println("criticalPathAggregationMain Exception ",y)
  end
end


# From Individual-Streamline-Body

function individualStreamlineMain(TV::TimeVars,UP::UrlParams,SP::ShowParams)
  try

      localTableDF = DataFrame()
#      localTableRtDF = DataFrame()
      statsDF = DataFrame()

      localTableDF = defaultBeaconsToDF(TV,UP,SP)
      recordsFound = nrow(localTableDF)

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
          return
      end

      if (SP.debugLevel > 8)
          println("Individual part 1 done with ",recordsFound, " records")
      end

      # Stats on the data
      statsDF = beaconStats(TV,UP,SP,localTableDF;showAdditional=true)
      UP.timeLowerMs = round(statsDF[1:1,:median][1] * 0.90)
      UP.timeUpperMs = round(statsDF[1:1,:median][1] * 1.10)

      if (SP.debugLevel > 2)
          println("Individual selecting from $(UP.timeLowerMs) to $(UP.timeUpperMs)")
      end

#      localTableRtDF = getResourcesForBeaconCreateDF(TV,UP)
#      recordsFound = nrow(localTableRtDF)
#
#      if (SP.debugLevel > 4)
#          println("part 2 done with ",recordsFound, " resource table records")
#      end
#
#      if recordsFound == 0
#          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) has no resource matches during this time",showTimeStamp=false)
#          return
#      end
#
#      if (SP.debugLevel > 8)
#          println("part 3 done")
#      end

      showAvailableSessionsStreamline(TV,UP,SP,localTableDF)


  catch y
      println("IndividualStreamlineMain Exception ",y)
  end
end

# From Individual-Streamline-Body

function individualStreamlineTableV2(TV::TimeVars,UP::UrlParams,SP::ShowParams;repeat::Int64=1)
  try

      # Get Started

      localTableDF = DataFrame()
      localTableRtDF = DataFrame()
      statsDF = DataFrame()

      localTableDF = estimateFullBeaconsV2(TV,UP,SP)
      recordsFound = nrow(localTableDF)

      if (SP.debugLevel > 0)
          println("part 1 done with ",recordsFound, " records")
          if recordsFound == 0
              displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
          end
      end

      if recordsFound == 0
          row = DataFrame(url=UP.urlFull,beacon_time=0,request_count=0,encoded_size=0,samples=0)
          return row
      end

      # Stats on the data
      row = beaconStatsRow(TV,UP,SP,localTableDF)

      # record the latest record and save to print outside the final loop
      return row

  catch y
      println("individualStreamlineTableV2 Exception ",y)
  end
end

# From Individual-Streamline-Body
function criticalPathStreamline(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)
  try
      io = 0
      sessionIdString = ASCIIString("")

      criticalPathDF = DataFrame(urlgroup=ASCIIString[],time=Int64[])

      for subdf in groupby(localTableDF,[:session_id,:timestamp])
          # Quick out
          if (io == SP.showLines)
              break
          end
          if(SP.debugLevel > 4)
              println("Finding page $io Timer=",subdf[1,:timers_t_done]," rl=",UP.timeLowerMs," ru=",UP.timeUpperMs)
          end
          if (UP.usePageLoad)
              timeVar = subdf[1,:timers_t_done]
          else
              timeVar = subdf[1,:timers_domready]
          end
          if (timeVar >= UP.timeLowerMs && timeVar <= UP.timeUpperMs)
              io += 1
              if io <= SP.showLines
                  sessionId = subdf[1,:session_id]
                  sessionIdString = ASCIIString(sessionId)
                  timeStampVar = subdf[1,:timestamp]
                  timeVarSec = timeVar / 1000.0
                  if (SP.debugLevel > 8)
                      labelString = "$(timeVarSec) Seconds"
                      println("Page $(io) of $(SP.showLines): $(labelString)")
                      if (SP.debugLevel > 6)
                          println("executeSingleSession(TV,UP,SP,",timeVar,",\"",sessionId,"\",",timeStampVar,") #    Time=",timeVar)
                      end
                  end
                  topPageUrl = individualPageData(TV,UP,SP,sessionIdString,timeStampVar)
                  suitable  = individualCriticalPath(TV,UP,SP,topPageUrl,criticalPathDF,timeVar,sessionIdString,timeStampVar)
                  if (!suitable)
                      SP.showLines += 1
                  end
              else
                  break
              end
          end
      end

      lineCount = io
      if (SP.debugLevel > 4)
          println("size of criticalPathDF is ",size(criticalPathDF,1)," Using $lineCount pages")
      end

      finalCriticalPathDF = finalCriticalPath(TV,UP,SP,criticalPathDF)

      summaryCriticalPathDF = deepcopy(finalCriticalPathDF)

      if (SP.debugLevel > 4)
          standardChartTitle(TV,UP,SP,"Debug4: Final Critical Path DF")
          beautifyDF(finalCriticalPathDF)
      end

      # Display critical path Agg but remove outlyiers
      criticalPathFinalTreemap(TV,UP,SP,
        finalCriticalPathDF[Bool[isless(UP.sizeMin,x) for x in finalCriticalPathDF[:counter]], :]
      )

      summaryUrlGroupDF = summaryReduce(TV,UP,SP,summaryCriticalPathDF,lineCount)

      if (SP.debugLevel > 4)
          beautifyDF(summaryUrlGroupDF)
      end

      saveShow = SP.devView
      SP.devView = false
      criticalPathFinalTreemap(TV,UP,SP,summaryUrlGroupDF)
      SP.devView = saveShow

  catch y
      println("criticalPathStreamline Exception ",y)
  end
end


function showAvailableSessionsStreamline(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)
  try
#      full = join(localTableDF,localTableRtDF, on = [:session_id,:timestamp])
      io = 0
      sessionIdString = ASCIIString("")

#      for subdf in groupby(full,[:session_id,:timestamp])
      for subdf in groupby(localTableDF,[:session_id,:timestamp])
          #look for quick out
          if (io == SP.showLines)
              return
          end
          s = size(subdf,1)
          if(SP.debugLevel > 2)
              println("Current Page Size=",s," Target Timer=",subdf[1,:timers_t_done]," rl=",UP.timeLowerMs," ru=",UP.timeUpperMs)
          end
          if (UP.usePageLoad)
              timeVar = subdf[1,:timers_t_done]
          else
              timeVar = subdf[1,:timers_domready]
          end
          if (timeVar >= UP.timeLowerMs && timeVar <= UP.timeUpperMs)
              io += 1
              #println("Testing $(io) against $(SP.showLines)")
              if io <= SP.showLines
                  sessionId = subdf[1,:session_id]
                  #println("Session_id $(sessionId)")
                  sessionIdString = ASCIIString(sessionId)
                  timeStampVar = subdf[1,:timestamp]
                  timeVarSec = timeVar / 1000.0
                  # We may be missing requests such that the timers_t_done is a little bigger than the treemap
                  if (SP.debugLevel > 2)
                      labelString = "$(timeVarSec) Seconds"
                      println("Page $(io) of $(SP.showLines): $(labelString)")
                      if (SP.debugLevel > 6)
                          println("executeSingleSession(TV,UP,SP,",timeVar,",\"",sessionId,"\",",timeStampVar,") #    Time=",timeVar)
                      end
                  end
                  topPageUrl = individualPageData(TV,UP,SP,sessionIdString,timeStampVar)
                  suitable  = individualPageReport(TV,UP,SP,topPageUrl,timeVar,sessionIdString,timeStampVar)
                  if (!suitable)
                      if (SP.debugLevel > 2)
                          println("Not suitable: $(UP.urlRegEx),$(sessionIdString),$(timeStampVar),$(timeVar)")
                      end
                      SP.showLines += 1
                  end
              else
                  return
              end
          end
      end
  catch y
      println("showAvailSessions Exception ",y)
  end
end

# From Individual-Streamline-Body

function executeSingleSession(TV::TimeVars,UP::UrlParams,SP::ShowParams,timerDone::Int64,studySession::ASCIIString,studyTime::Int64)
  try

    sessionPageUrl = individualPageData(TV,UP,SP,studySession,studyTime)
    individualPageReport(TV,UP,SP,sessionPageUrl,timerDone,studySession,studyTime)

  catch y
      println("showAvailSessions Exception ",y)
  end

end

# From Individual-Streamline-Body

function individualPageData(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)
  try

      toppageurl = DataFrame()

      if studyTime > 0
          if SP.debugLevel > 8
              println("Calling sessionUrlTableCreateDF")
          end
          toppageurl = sessionUrlTableCreateDF(TV,UP,SP,studySession,studyTime)
          elseif (studySession != "None")
              if SP.debugLevel > 8
                  println("Calling allSessionUrlTableCreateDF")
              end
              toppageurl = allSessionUrlTableCreateDF(TV,UP,SP,studySession)
          else
              if SP.debugLevel > 8
                  println("Calling allPageUrlTableCreateDF")
              end
              toppageurl = allPageUrlTableCreateDF(TV,UP)
      end

      return toppageurl

  catch y
      println("individualPageData Exception ",y)
  end
end

function individualCriticalPath(TV::TimeVars,UP::UrlParams,SP::ShowParams,
  toppageurl::DataFrame,criticalPathDF::DataFrame,timerDone::Int64,studySession::ASCIIString,studyTime::Int64)
  try

      if size(toppageurl,1) == 0
          if (SP.debugLevel > 4)
              println("Rejecting current page, no data")
          end
          return false
      end

      toppageurl = names!(toppageurl[:,:],
      [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
          symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
          symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

      if (!suitableTest(UP,SP,toppageurl))
          if (SP.debugLevel > 4)
              println("suitable Test failed")
          end
          return false
      end

      removeNegitiveTime(toppageurl,:Total)
      removeNegitiveTime(toppageurl,:Redirect)
      removeNegitiveTime(toppageurl,:Blocking)
      removeNegitiveTime(toppageurl,:DNS)
      removeNegitiveTime(toppageurl,:TCP)
      removeNegitiveTime(toppageurl,:Request)
      removeNegitiveTime(toppageurl,:Response)

      if (SP.debugLevel > 8)
        println("Classify Data");
      end
      classifyUrl(SP,toppageurl);

      if (SP.debugLevel > 8)
        println("Add Gap and Critical Path")
      end

      toppageurl = gapAndCriticalPathV2(toppageurl,timerDone);
      if (!suitableTest(UP,SP,toppageurl))
          if (SP.debugLevel > 4)
              println("Not Suitable after gap calcuation")
          end
          return false
      end

      if (SP.debugLevel > 6)
          standardChartTitle(TV,UP,SP,"Debug6: After Gap")
          beautifyDF(toppageurl)
      end

      reduceCriticalPath(TV,UP,SP,toppageurl,criticalPathDF)

      # Save the fields


      return true

  catch y
      println("individualCriticalPath Exception ",y)
  end
end


function individualPageReport(TV::TimeVars,UP::UrlParams,SP::ShowParams,
  toppageurl::DataFrame,timerDone::Int64,studySession::ASCIIString,studyTime::Int64)
  try
      UrlParamsValidate(UP)

      if size(toppageurl,1) == 0
          if (SP.debugLevel > 4)
              println("Rejecting current page, no data")
          end
          return false
      end

      if (SP.debugLevel > 8)
        println("Clean Up Data table ",size(toppageurl,1))
      end

      toppageurl = names!(toppageurl[:,:],
      [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
          symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
          symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

      if (!suitableTest(UP,SP,toppageurl))
          return false
      end


      toppageurlbackup = deepcopy(toppageurl);
      toppageurl = deepcopy(toppageurlbackup)

      if (SP.debugLevel > 8)
          beautifyDF(toppageurl)
      end

      removeNegitiveTime(toppageurl,:Total)
      removeNegitiveTime(toppageurl,:Redirect)
      removeNegitiveTime(toppageurl,:Blocking)
      removeNegitiveTime(toppageurl,:DNS)
      removeNegitiveTime(toppageurl,:TCP)
      removeNegitiveTime(toppageurl,:Request)
      removeNegitiveTime(toppageurl,:Response)

      if (SP.debugLevel > 4)
        println("Classify Data");
      end
      classifyUrl(SP,toppageurl);

      if (SP.debugLevel > 4)
        println("Scrub Data");
      end
      scrubUrlToPrint(SP,toppageurl,:urlgroup);

      if (SP.debugLevel > 4)
        println("Add Gap and Critical Path")
      end

      toppageurl = gapAndCriticalPathV2(toppageurl,timerDone);
      if (!suitableTest(UP,SP,toppageurl))
          return false
      end

      if (SP.devView)
          waterFallFinder(TV,UP,SP,studySession,studyTime)
      end

      if (SP.debugLevel > 4)
          beautifyDF(toppageurl)
      end

      labelField = UP.urlFull
      criticalPathTreemapV2(TV,UP,SP,labelField,toppageurl)

      if (SP.devView)
          gapTreemapV2(TV,UP,SP,toppageurl;showTreemap=false)
      end

      if (!SP.criticalPathOnly)
          #itemCountTreemap(TV,UP,SP,toppageurl)      All entries are 1
          endToEndTreemap(TV,UP,SP,toppageurl)
          blockingTreemap(TV,UP,SP,toppageurl)
          requestTreemap(TV,UP,SP,toppageurl)
          responseTreemap(TV,UP,SP,toppageurl)
          dnsTreemap(TV,UP,SP,toppageurl)
          tcpTreemap(TV,UP,SP,toppageurl)
          redirectTreemap(TV,UP,SP,toppageurl)
      end

      return true

  catch y
      println("individualPageReport Exception ",y)
  end
end


# From Individual-Streamline-Body

function gapAndCriticalPathV2(toppageurl::DataFrame,timerDone::Int64)
  try
      # Start OF Gap & Critical Path Calculation

      toppageurl = names!(toppageurl[:,:],
      [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
          symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
          symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

      sort!(toppageurl,cols=[order(:Start),order(:Total,rev=true)]);

      #clear times beyond timerDone, set timerDone high if you wish to see all
      toppageurl2 = deepcopy(toppageurl)

      i = 0
      lastRow = 0
      for url in toppageurl[1:end,:urlgroup]
          i += 1
          newStartTime = toppageurl[i,:Start]
          newTotalTime = toppageurl[i,:Total]
          newEndTime = newStartTime + newTotalTime
          if (newStartTime > timerDone)
              if lastRow == 0
                  lastRow = i
              end
              #println("Clearing $(lastRow) for $(url) newStartTime=$(newStartTime), newEndTime=$(newEndTime), target=$(timerDone)")
              deleterows!(toppageurl2,lastRow)
              continue
          end

          #look for requests which cross the end of the timerDone
          if (newEndTime > timerDone && lastRow == 0)
              adjTime = timerDone-newStartTime
              #println("Adjusting $(lastRow) for $(url) newStartTime=$(newStartTime), oldEndTime=$(newEndTime), newEndTime=$(adjTime), target=$(timerDone)")
              toppageurl2[i,:Total] = adjTime
          end

      end

      #println("")
      #println(" Result ")
      #println("")

      #i = 0
      #for url in toppageurl2[1:end,:urlgroup]
      #    i += 1
      #    newStartTime = toppageurl2[i,:Start]
      #    newTotalTime = toppageurl2[i,:Total]
      #    println("XXX ",url," newStartTime=$(newStartTime), newTotalTime=$(newTotalTime), target=$(timerDone)")
      #end

      toppageurl = deepcopy(toppageurl2)

      toppageurl[:Gap] = 0
      toppageurl[:Critical] = 0

      #todo check the size to make sure at least 3 rows of data

      prevStartTime = toppageurl[1,:Start]
      prevTotalTime = toppageurl[1,:Total]
      i = 0
      toppageurl[:Critical] = toppageurl[1,:Total]

      for url in toppageurl[1:end,:urlgroup]
          i += 1
          toppageurl[i,:Gap] = 0
          toppageurl[i,:Critical] = 0

          newStartTime = toppageurl[i,:Start]
          newTotalTime = toppageurl[i,:Total]
          #println("Url ",url," newStartTime=$(newStartTime), newTotalTime=$(newTotalTime), prevStartTime=$(prevStartTime)")

          #Sorted by start time ascending and largest total time decending
          #Anyone with same time has the previous is nested inside the current one and has no time

          if (newStartTime == prevStartTime && i > 1)
              #println("Matched line $i start time $newStartTime")
              toppageurl[i,:Gap] = 0
              toppageurl[i,:Critical] = 0
              continue
          end

          # did we have a gap?
          gapTime = newStartTime - prevStartTime - prevTotalTime

          # Negitive gap means we start inside someone else
          if (gapTime < 0)
              #nested request or overlapping but no gap already done toppageurl[i,:gap] = 0

              prevMarker = prevStartTime + prevTotalTime
              newMarker = newStartTime + newTotalTime

              #println("\tprevMarker=$prevMarker, newMarker=$newMarker")
              if (i == 1)
                prevTotalTime = prevMarker
                toppageurl[i,:Critical] = prevMarker
                prevStartTime = 0
                continue
              end

              if (prevMarker >= newMarker)
                  # Still nested inside a larger request already donetoppageurl[i,:critical] = 0
                  continue
              else
                  # Figure how much of new time is beyond end of old time
                  # println("nst=",newStartTime,",ntt=",newTotalTime,",nm=",newMarker,",pst=",prevStartTime,",ptt=",prevTotalTime,",pm=",prevMarker)
                  # When done we will pick up at the end of this newer overlapped request
                  prevTotalTime = newMarker - prevMarker
                  #println("ptt=",prevTotalTime)

                  # it is critical path but only the part which did not overlap with the previous request
                  toppageurl[i,:Critical] = newMarker - prevMarker
                  prevStartTime = prevMarker
              end

          else
              #println("gap time ",gapTime,",",newStartTime,",",newTotalTime,",",prevStartTime,",",prevTotalTime)
              toppageurl[i,:Gap] = gapTime
              # All of its time is critical path since this is start of a new range
              toppageurl[i,:Critical] = newTotalTime
              prevTotalTime = newTotalTime
              prevStartTime = newStartTime
          end
          # move on
          runningTime = sum(toppageurl[:,:Gap]) + sum(toppageurl[:,:Critical])
          #println("rt", runningTime, " at ",prevStartTime)

      end

      # Do not fix last record.  It is the "Not Blocking" Row.  Zero it out
      #i += 1
      #toppageurl[i,:Gap] = 0
      #toppageurl[i,:Critical] = 0

      return toppageurl

   catch y
      println("gapAndCritcalPathV2 Exception ",y)
  end
end

# From Individual-Streamline-Body

function suitableTest(UP::UrlParams,SP::ShowParams,toppageurl::DataFrame)
  try
      i = 0
      lastRow = 0
      newTotalTime = 0
      for url in toppageurl[1:end,:urlgroup]
          i += 1
          calc = toppageurl[i,:Start] + toppageurl[i,:Total]
          if calc > newTotalTime
              newTotalTime = calc
          end
      end
      #println("newTotalTime = $newTotalTime")
      if (newTotalTime < UP.timeLowerMs || newTotalTime > UP.timeUpperMs)
          if (SP.debugLevel > 2)
              println("Dropping page due to total time of $(newTotalTime)")
          end
          return false
      end

      return true

   catch y
      println("suitableTest Exception ",y)
  end
end

# From Individual-Streamline-Body

function newPagesList()
  try

  jList = JSON.parse(theList)

  dataArray = get(jList,"data","none")
  urlListDF = DataFrame()
  urlListDF[:urlgroup] = [""]

  if (dataArray != "none")

      for dataDict in dataArray
          attribDict = get(dataDict,"attributes","none")
          urlValue = get(attribDict,"uri","none")
          #typeof(urlValue)
          #println(urlValue)

          push!(urlListDF,[urlValue])
      end
  end
  deleterows!(urlListDF,1)
  return urlListDF

  catch y
      println("newPagesList Exception",y)
  end
end

function statsAndTreemaps(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try

        UrlParamsValidate(UP)

        # Is there data?
        localTableDF = defaultBeaconsToDF(TV,UP,SP)
        if (SP.debugLevel > 0)
          println("$(UP.beaconTable) count is ",size(localTableDF))
          println("")
        end

        # Stats on the data
        statsDF = DataFrame()
        dv = localTableDF[:timers_t_done]
        statsDF = basicStatsFromDV(dv)

        standardChartTitle(TV,UP,SP,"Beacon Data Stats")
        beautifyDF(statsDF[:,:])

        rangeLower = statsDF[1:1,:q25][1]
        rangeUpper = statsDF[1:1,:q75][1]

        studyTime = 0
        studySession = "None"

        toppageurl = DataFrame()
        if studyTime > 0
            toppageurl = sessionUrlTableCreateDF(TV,UP,SP,studySession,studyTime)
            elseif (studySession != "None")
              toppageurl = allSessionUrlTableCreateDF(TV,UP,SP,studySession)
            else
                toppageurl = allPageUrlTableCreateDF(TV,UP)
        end

        if (SP.debugLevel > 0)
          println("topPageUrl rows and column counts are ",size(toppageurl))
          println("")
        end

        toppageurl = names!(toppageurl[:,:],
        [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
            symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
            symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);


        # Debug
        toppageurlbackup = deepcopy(toppageurl);

        # Debug
        toppageurl = deepcopy(toppageurlbackup)

        removeNegitiveTime(toppageurl,:Total)
        removeNegitiveTime(toppageurl,:Redirect)
        removeNegitiveTime(toppageurl,:Blocking)
        removeNegitiveTime(toppageurl,:DNS)
        removeNegitiveTime(toppageurl,:TCP)
        removeNegitiveTime(toppageurl,:Request)
        removeNegitiveTime(toppageurl,:Response)

        summaryStatsDF = DataFrame()
        dv = toppageurl[:Total]
        summaryStatsDF = basicStatsFromDV(dv)

        standardChartTitle(TV,UP,SP,"RT Data Stats")
        beautifyDF(summaryStatsDF[:,:])

        classifyUrl(SP,toppageurl);
        scrubUrlToPrint(SP,toppageurl,:urlgroup);

        summaryPageGroup = summarizePageGroups(toppageurl)
        standardChartTitle(TV,UP,SP,"Top 10 Largest Classifications")
        beautifyDF(summaryPageGroup[1:min(end,10),:])

        # This is the non-Url specific report so get the summary table and overwrite toppageurl
        toppageurl = deepcopy(summaryPageGroup);

        endToEndTreemap(TV,UP,SP,toppageurl)
        itemCountTreemap(TV,UP,SP,toppageurl)

        if (SP.reportLevel > 1)
            blockingTreemap(TV,UP,SP,toppageurl)
            requestTreemap(TV,UP,SP,toppageurl)
            responseTreemap(TV,UP,SP,toppageurl)
            dnsTreemap(TV,UP,SP,toppageurl)
            tcpTreemap(TV,UP,SP,toppageurl)
            redirectTreemap(TV,UP,SP,toppageurl)
        end
    catch y
        println("statsAndTreemaps Exception ",y)
    end
end

# From 3rd Party Body TypeALl

function summarizePageGroups(toppageurl::DataFrame)
    try
        summaryPageGroup = DataFrame()
        summaryPageGroup[:urlpagegroup] = "Grand Total"
        summaryPageGroup[:Start] = 0
        summaryPageGroup[:Total] = 0
        summaryPageGroup[:Redirect] = 0
        summaryPageGroup[:Blocking] = 0
        summaryPageGroup[:DNS] = 0
        summaryPageGroup[:TCP] = 0
        summaryPageGroup[:Request] = 0
        summaryPageGroup[:Response] = 0
        summaryPageGroup[:Gap] = 0
        summaryPageGroup[:Critical] = 0
        summaryPageGroup[:urlgroup] = ""
        summaryPageGroup[:request_count] = 0
        summaryPageGroup[:label] = ""
        summaryPageGroup[:load_time] = 0.0
        summaryPageGroup[:beacon_time] = 0.0

        for subDf in groupby(toppageurl,:urlpagegroup)
            #println(subDf[1:1,:urlpagegroup]," ",size(subDf,1))
            Total = 0
            Redirect = 0
            Blocking = 0
            DNS = 0
            TCP = 0
            Request = 0
            Response = 0
            Gap = 0
            Critical = 0
            request_count = 0
            load_time = 0.0
            beacon_time = 0.0

            for row in eachrow(subDf)
                #println(row)
                Total += row[:Total]
                Redirect += row[:Redirect]
                Blocking += row[:Blocking]
                DNS += row[:DNS]
                TCP += row[:TCP]
                Request += row[:Request]
                Response += row[:Response]
                Gap += row[:Gap]
                Critical += row[:Critical]
                request_count += row[:request_count]
                load_time += row[:load_time]
                beacon_time += row[:beacon_time]
            end
            #convert to seconds
            load_time = (Total / request_count) / 1000
            push!(summaryPageGroup,[subDf[1:1,:urlpagegroup];0;Total;Redirect;Blocking;DNS;TCP;Request;Response;Gap;Critical;subDf[1:1,:urlpagegroup];request_count;"Label";load_time;beacon_time])
        end

        sort!(summaryPageGroup,cols=[order(:Total,rev=true)])
        return summaryPageGroup
    catch y
        println("summarizePageGroup Exception ",y)
    end
end

# Large Pages

function createJoinTableSummary(SP::ShowParams,joinTableSummary::DataFrame,joinTables::DataFrame)

    joinTableSummary[:urlgroup] = "delete"
    joinTableSummary[:session_id] = ""
    joinTableSummary[:timestamp] = 0
    joinTableSummary[:encoded] = 0
    joinTableSummary[:transferred] = 0
    joinTableSummary[:decoded] = 0
    joinTableSummary[:count] = 0

    sort!(joinTables,cols=[order(:encoded,rev=true)])
    for subDf in groupby(joinTables,:urlgroup)
        #beautifyDF(subDf[1:1,:])
        i = 1
        for row in eachrow(subDf)
            if (i == 1)
                i +=1
                push!(joinTableSummary,[row[:urlgroup],row[:session_id],row[:timestamp],row[:encoded],row[:transferred],row[:decoded],row[:count]])
            end
        end
    end

    i = 1
    for x in joinTableSummary[:,:urlgroup]
        if x == "delete"
            deleterows!(joinTableSummary,i)
        end
        i += 1
    end
    sort!(joinTableSummary,cols=[order(:encoded,rev=true)])

    beautifyDF(joinTableSummary[1:min(SP.showLines,end),[:urlgroup,:encoded,:transferred,:decoded]])

    return joinTableSummary
end

function reduceCriticalPath(TV::TimeVars,UP::UrlParams,SP::ShowParams,pageDF::DataFrame,criticalPathDF::DataFrame)

    if (SP.debugLevel > 8)
        println("Starting reduceCriticalPath")
    end

    try

        for subDF in groupby(pageDF,[:urlpagegroup])
            currentGroup = subDF[1:1,:urlpagegroup]
            currentCriticalPath = sum(subDF[:,:Critical])
            #println("$currentGroup cp=$currentCriticalPath")
            if (currentCriticalPath > 0)
                push!(criticalPathDF,[currentGroup;currentCriticalPath])
            end
        end

        return
    catch y
        println("reduceCriticalPath Exception ",y)
    end
end

function summaryReduce(TV::TimeVars,UP::UrlParams,SP::ShowParams,summaryDF::DataFrame,lineCount::Int64)

    if (SP.debugLevel > 8)
        println("Starting summaryReduce")
    end

    try
        summaryUrlGroupDF = DataFrame(urlgroup=ASCIIString[],average=Float64[],
            maximum=Int64[],counter=Int64[],label=ASCIIString[])

        classifyUrlGroup(SP,summaryDF)
        #beautifyDF(summaryDF)

        for subDF in groupby(summaryDF,[:urlgroup])
            currentGroup = subDF[1:1,:urlgroup]
            currentTotal = sum(subDF[:,:average])/lineCount
            currentMax = maximum(subDF[:,:maximum])
            currentCount = size(subDF[:,:urlgroup],1)
            #println("$currentGroup cp=$currentCriticalPath")
            if (currentTotal > 0)
                push!(summaryUrlGroupDF,[currentGroup;currentTotal;currentMax;currentCount;"label"])
            end
        end

        return summaryUrlGroupDF

    catch y
        println("summaryReduce Exception ",y)
    end
end

function finalCriticalPath(TV::TimeVars,UP::UrlParams,SP::ShowParams,criticalPathDF::DataFrame)

    if (SP.debugLevel > 8)
        println("Starting finalCriticalPathDF")
    end

    try
        finalCriticalPathDF = DataFrame(urlgroup=ASCIIString[],average=Float64[],maximum=Int64[],counter=Int64[],label=ASCIIString[])

        for subDF in groupby(criticalPathDF,[:urlgroup])
            currentGroup = subDF[1:1,:urlgroup]
            currentMean = mean(subDF[:,:time])
            currentMax = maximum(subDF[:,:time])
            currentCount = size(subDF[:,:urlgroup],1)
            push!(finalCriticalPathDF,[currentGroup;currentMean;currentMax;currentCount;"label"])
        end

        return finalCriticalPathDF
    catch y
        println("finalCriticalPath Exception ",y)
    end
end

function printWellKnownUrlGroup(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if (SP.debugLevel > 8)
        println("Starting printWellKnownUrlGroup")
    end

    try
        printDF = DataFrame(urlgroup=ASCIIString[],finalgroup=ASCIIString[])

        for key in keys(WellKnownUrlGroup)
            push!(printDF,[key;get(WellKnownUrlGroup,key,"Not Found")])
        end


        for subDF in groupby(printDF,[:finalgroup])
            title = (subDF[1:1,:finalgroup][1])
            println("\n\n\nGroup: $title\n")
            #println(subDF[:,:urlGroup])
            for value in subDF[:,:urlgroup]
                println("\t\t$value")
            end
        end

    catch y
        println("printWellKnownUrlGroup Exception ",y)
    end
end

function printCsvWellKnownUrlGroup(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if (SP.debugLevel > 8)
        println("Starting printWellKnownUrlGroup")
    end

    try
        printDF = DataFrame(urlgroup=ASCIIString[],finalgroup=ASCIIString[])

        for key in keys(WellKnownUrlGroup)
            push!(printDF,[key;get(WellKnownUrlGroup,key,"Not Found")])
        end


        for subDF in groupby(printDF,[:finalgroup])
            title = (subDF[1:1,:finalgroup][1])
            #println("\n\n\nGroup: $title\n")
            #println(subDF[:,:urlGroup])
            for value in subDF[:,:urlgroup]
                println("$title,$value")
            end
        end

    catch y
        println("printCsvWellKnownUrlGroup Exception ",y)
    end
end
