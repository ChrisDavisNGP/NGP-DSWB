function criticalPathAggregationMain(TV::TimeVars,UP::UrlParams,SP::ShowParams)
  try

      localTableDF = DataFrame()
      localTableRtDF = DataFrame()
      statsDF = DataFrame()

      localTableDF = defaultBeaconsToDF(TV,UP,SP)
      recordsFound = nrow(localTableDF)

      if (SP.debugLevel > 4)
          println("part 1 done with ",recordsFound, " records")
      end

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
          #println("$(UP.urlFull) for $(deviceType) was not found during $(TV.timeString)")
          return
      end

      # Stats on the data
      statsDF = beaconStats(TV,UP,SP,localTableDF;showAdditional=true)
      rangeLowerMs = statsDF[1:1,:median][1] * 0.95
      rangeUpperMs = statsDF[1:1,:median][1] * 1.05

      localTableRtDF = getResourcesForBeacon(TV,UP)
      recordsFound = nrow(localTableRtDF)

      if (SP.debugLevel > 4)
          println("part 2 done with ",recordsFound, " records")
      end

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) has no resource matches during this time",showTimeStamp=false)
          #println("$(UP.urlFull) for $(deviceType) was not found during $(TV.timeString)")
          return
      end

      criticalPathStreamline(TV,UP,SP,localTableDF,localTableRtDF)


  catch y
      println("critialPathAggregationMain Exception ",y)
  end
end


# From Individual-Streamline-Body

function individualStreamlineMain(TV::TimeVars,UP::UrlParams,SP::ShowParams)
  try

      localTableDF = DataFrame()
      localTableRtDF = DataFrame()
      statsDF = DataFrame()

      localTableDF = defaultBeaconsToDF(TV,UP,SP)
      recordsFound = nrow(localTableDF)

      if (SP.debugLevel > 4)
          println("part 1 done with ",recordsFound, " records")
      end

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
          #println("$(UP.urlFull) for $(deviceType) was not found during $(TV.timeString)")
          return
      end

      # Stats on the data
      statsDF = beaconStats(TV,UP,SP,localTableDF;showAdditional=true)
      UP.timeLowerMs = round(statsDF[1:1,:median][1] * 0.90)
      UP.timeUpperMs = round(statsDF[1:1,:median][1] * 1.10)

      if (SP.debugLevel > 2)
          println("part 2 done: selecting from $(UP.timeLowerMs) to $(UP.timeUpperMs)")
      end

      localTableRtDF = getResourcesForBeacon(TV,UP)
      recordsFound = nrow(localTableRtDF)

      if (SP.debugLevel > 4)
          println("part 2 done with ",recordsFound, " records")
      end

      if recordsFound == 0
          displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) has no resource matches during this time",showTimeStamp=false)
          #println("$(UP.urlFull) for $(deviceType) was not found during $(TV.timeString)")
          return
      end

      if (SP.debugLevel > 4)
          println("part 3 done")
      end

      showAvailableSessionsStreamline(TV,UP,SP,localTableDF,localTableRtDF)
      #println("part 4 done")


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

function estimateFullBeaconsV2(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  try
      table = UP.beaconTable
      rt = UP.resourceTable

      if (UP.usePageLoad)
          localTableDF = query("""\
          select
              'None' as urlpagegroup,
              avg($rt.start_time),
              avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.start_time) END) as total,
              avg($rt.redirect_end-$rt.redirect_start) as redirect,
              avg(CASE WHEN ($rt.dns_start = 0 and $rt.request_start = 0) THEN (0) WHEN ($rt.dns_start = 0) THEN ($rt.request_start-$rt.fetch_start) ELSE ($rt.dns_start-$rt.fetch_start) END) as blocking,
              avg($rt.dns_end-$rt.dns_start) as dns,
              avg($rt.tcp_connection_end-$rt.tcp_connection_start) as tcp,
              avg($rt.response_first_byte-$rt.request_start) as request,
              avg(CASE WHEN ($rt.response_first_byte = 0) THEN (0) ELSE ($rt.response_last_byte-$rt.response_first_byte) END) as response,
              avg(0) as gap,
              avg(0) as critical,
              CASE WHEN (position('?' in $rt.url) > 0) then trim('/' from (substring($rt.url for position('?' in substring($rt.url from 9)) +7))) else trim('/' from $rt.url) end as urlgroup,
              count(*) as request_count,
              'Label' as label,
              avg(CASE WHEN ($rt.response_last_byte = 0) THEN (0) ELSE (($rt.response_last_byte-$rt.start_time)/1000.0) END) as load,
              avg($table.timers_t_done) as beacon_time
          FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
              where
              $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
              and $table.session_id IS NOT NULL
              and $table.page_group ilike '$(UP.pageGroup)'
              and $table.params_u ilike '$(UP.urlRegEx)'
              and $table.user_agent_device_type ilike '$(UP.deviceType)'
              and $table.user_agent_os ilike '$(UP.agentOs)'
              and $table.timers_t_done >= $(UP.timeLowerMs) and $table.timers_t_done <= $(UP.timeUpperMs)
              and $table.params_rt_quit IS NULL
              group by urlgroup,urlpagegroup,label
              """);
      else

          if (SP.debugLevel > 8)
              debugTableDF = query("""\
              select
                  *
              FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
                  where
                  $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
                  and $table.session_id IS NOT NULL
                  and $table.page_group ilike '$(UP.pageGroup)'
                  and $table.params_u ilike '$(UP.urlRegEx)'
                  and $table.user_agent_device_type ilike '$(UP.deviceType)'
                  and $table.user_agent_os ilike '$(UP.agentOs)'
                  and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
                  and $table.params_rt_quit IS NULL
                  limit 3
                  """);

              beautifyDF(debugTableDF[1:min(30,end),:])
              println("pg=",UP.pageGroup," url=",UP.urlRegEx," dev=",UP.deviceType," dr lower=",UP.timeLowerMs," dr upper=",UP.timeUpperMs);

          end

          localTableDF = query("""\
          select
          CASE WHEN (position('?' in $table.params_u) > 0) then trim('/' from (substring($table.params_u for position('?' in substring($table.params_u from 9)) +7))) else trim('/' from $table.params_u) end as urlgroup,
              count(*) as request_count,
              avg($table.timers_domready) as beacon_time,
              sum($rt.encoded_size) as encoded_size,
              $table.errors as errors, $table.session_id,$table."timestamp"

          FROM $rt join $table on $rt.session_id = $table.session_id and $rt."timestamp" = $table."timestamp"
              where
              $rt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
              and $table.session_id IS NOT NULL
              and $table.page_group ilike '$(UP.pageGroup)'
              and $table.params_u ilike '$(UP.urlRegEx)'
              and $table.user_agent_device_type ilike '$(UP.deviceType)'
              and $table.user_agent_os ilike '$(UP.agentOs)'
              and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
              and $table.params_rt_quit IS NULL
              and $table.errors IS NULL
          group by urlgroup,$table.session_id,$table."timestamp",errors
              """);


          if (nrow(localTableDF) == 0)
              return localTableDF
          end

          # Clean Up Bad Samples
          # Currently request < 10

          iRow = 0
          reqVector = localTableDF[:request_count]

          for reqCount in reqVector
              iRow = iRow + 1
              if (reqCount < 10)
                  if (SP.debugLevel > 8)
                      beautifyDF(localTableDF[iRow:iRow,:])
                  end
                 deleterows!(localTableDF,iRow)
              end
          end

          if (SP.debugLevel > 6)
              beautifyDF(localTableDF[1:min(30,end),:])
          end
      end

      return localTableDF
  catch y
      println("urlDetailTables Exception ",y)
  end
end

# From Individual-Streamline-Body

function finalUrlTableOutput(TV::TimeVars,UP::UrlParams,SP::ShowParams,topUrls::DataArray)
  try

  finalTable = DataFrame()
  finalTable[:url] = [""]
  finalTable[:beacon_time] = [0]
  finalTable[:request_count] = [0]
  finalTable[:encoded_size] = [0]
  finalTable[:samples] = [0]

  for testUrl in topUrls
      #UP.urlRegEx = string("%",ASCIIString(testUrl),"%")
      #UP.urlFull = string("/",ASCIIString(testUrl),"/")
      UP.urlRegEx = string("%",ASCIIString(testUrl))
      UP.urlFull = testUrl
      if (UP.deviceType == "Mobile")
          row = individualStreamlineTableV2(TV,UP,SP,repeat=1)

          if (UP.orderBy == "size")
              if (row[:encoded_size][1] < UP.sizeMin)
                   if (SP.debugLevel > 4)
                       println("Case 1: Dropping row", row[:encoded_size][1], " < ", UP.sizeMin);
                   end
                  continue
              end
              if (row[:samples][1] < UP.samplesMin)
                   if (SP.debugLevel > 4)
                      println("Case 2: Dropping row", row[:samples][1], " < ", UP.samplesMin);
                   end
                  continue
              end
          else
              if (row[:beacon_time][1] < UP.timeLowerMs)
                   if (SP.debugLevel > 4)
                      println("Case 3: Dropping row", row[:beacon_time][1], " < ", UP.timeLowerMs);
                   end
                  continue
              end
              if (row[:samples][1] < UP.samplesMin)
                   if (SP.debugLevel > 4)
                      println("Case 4: Dropping row", row[:samples][1], " < ", UP.samplesMin);
                   end
                  continue
              end
          end

          push!(finalTable,[row[:url];row[:beacon_time];row[:request_count];row[:encoded_size];row[:samples]])
      end

      if (UP.deviceType == "Desktop")
          row = individualStreamlineTableV2(TV,UP,SP,repeat=1)

          if (UP.orderBy == "size")
              if (row[:encoded_size][1] < UP.sizeMin)
                   if (SP.debugLevel > 4)
                       println("Case 1: Dropping row", row[:encoded_size][1], " < ", UP.sizeMin);
                   end
                  continue
              end
              if (row[:samples][1] < UP.samplesMin)
                   if (SP.debugLevel > 4)
                      println("Case 2: Dropping row", row[:samples][1], " < ", UP.samplesMin);
                   end
                  continue
              end
          else
              if (row[:beacon_time][1] < UP.timeLowerMs)
                   if (SP.debugLevel > 4)
                      println("Case 3: Dropping row", row[:beacon_time][1], " < ", UP.timeLowerMs);
                   end
                  continue
              end
              if (row[:samples][1] < UP.samplesMin)
                   if (SP.debugLevel > 4)
                      println("Case 4: Dropping row", row[:samplese][1], " < ", UP.samplesMin);
                   end
                  continue
              end
          end
          push!(finalTable,[row[:url];row[:beacon_time];row[:request_count];row[:encoded_size];row[:samples]])
      end
  end

  deleterows!(finalTable,1)

  if (UP.orderBy == "size")
      sort!(finalTable,cols=:encoded_size, rev=true)
          additional = join(["(Sorted By Size Descending; Min Samples ";UP.samplesMin;"; Top ";UP.limitRows;" Page Views)"]," ")
  else
      sort!(finalTable,cols=:beacon_time, rev=true)
          additional = join(["(Sorted By Time Descending; Min Samples ";UP.samplesMin;"; Top ";UP.limitRows;" Page Views)"]," ")
  end


  ft = names!(finalTable[:,:],
  [symbol("Recent Urls $(additional)");symbol("Time");symbol("Request Made");symbol("Page Size");symbol("Samples")])
  beautifyDF(ft[1:min(100,end),:])

  catch y
      println("finalUrlTableOutput Exception ",y)
  end
end

# From Individual-Streamline-Body

function beaconStatsRow(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)

  #Make a para later if anyone want to control
  goal = 3000.0

  row = DataFrame()
  row[:url] = UP.urlFull

  dv = localTableDF[:beacon_time]
  statsBeaconTimeDF = limitedStatsFromDV(dv)
  row[:beacon_time] = statsBeaconTimeDF[:median]
  samples = statsBeaconTimeDF[:count]
  if (SP.debug)
      println("bt=",row[:beacon_time][1]," goal=",goal)
  end

  if (SP.devView)
      if (UP.usePageLoad)
          chartTitle = "Page Load Time Stats: $(UP.urlFull) for $(UP.deviceType)"
      else
          chartTitle = "Page Domain Ready Time Stats: $(UP.urlFull) for $(UP.deviceType)"
      end
      showLimitedStats(TV,statsBeaconTimeDF,chartTitle)
  end

  dv = localTableDF[:request_count]
  statsRequestCountDF = limitedStatsFromDV(dv)
  row[:request_count] = statsRequestCountDF[:median]
  if (SP.devView)
      chartTitle = "Request Count"
      showLimitedStats(TV,statsRequestCountDF,chartTitle)
  end

  dv = localTableDF[:encoded_size]
  statsEncodedSizeDF = limitedStatsFromDV(dv)
  row[:encoded_size] = statsEncodedSizeDF[:median]

  row[:samples] = samples

  if (SP.devView)
      chartTitle = "Encoded Size"
      showLimitedStats(TV,statsEncodedSizeDF,chartTitle)
  end

  if (SP.debug)
      beautifyDF(row[:,:])
  end
  return row
end

# From Individual-Streamline-Body
function criticalPathStreamline(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame,localTableRtDF::DataFrame)
  try
      full = join(localTableDF,localTableRtDF, on = [:session_id,:timestamp])
      io = 0
      s1String = ASCIIString("")

      criticalPathDF = DataFrame(urlgroup=ASCIIString[],time=Int64[])

      for subdf in groupby(full,[:session_id,:timestamp])
          s = size(subdf)
          if(SP.debug)
              println("Size=",s," Timer=",subdf[1,:timers_t_done]," rl=",UP.timeLowerMs," ru=",UP.timeUpperMs)
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
                  s1 = subdf[1,:session_id]
                  #println("Session_id $(s1)")
                  s1String = ASCIIString(s1)
                  timeStampVar = subdf[1,:timestamp]
                  timeVarSec = timeVar / 1000.0
                  # We may be missing requests such that the timers_t_done is a little bigger than the treemap
                  labelString = "$(UP.urlFull) $(timeVarSec) Seconds for $(UP.deviceType)"
                  if (SP.debugLevel > 8)
                      println("$(io) / $(SP.showLines): $(labelString),$(UP.urlRegEx)")
                      println("executeSingleSession(TV,UP,SP,",timeVar,",\"",s1,"\",",timeStampVar,") #    Time=",timeVar)
                  end
                  topPageUrl = individualPageData(TV,UP,SP,s1String,timeStampVar)
                  suitable  = individualCriticalPath(TV,UP,SP,topPageUrl,criticalPathDF,timeVar,s1String,timeStampVar)
                  if (!suitable)
                      SP.showLines += 1
                  end
              else
                  break
              end
          end
      end

      if (SP.debugLevel > 4)
          println("size of criticalPathDF is ",size(criticalPathDF))
      end

      finalCriticalPathDF = finalCriticalPath(TV,UP,SP,criticalPathDF)

      if (SP.debugLevel > 4)
          beautifyDF(finalCriticalPathDF)
      end

      criticalPathFinalTreemap(TV,UP,SP,finalCriticalPathDF)

  catch y
      println("showAvailSessions Exception ",y)
  end
end


function showAvailableSessionsStreamline(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame,localTableRtDF::DataFrame)
  try
      full = join(localTableDF,localTableRtDF, on = [:session_id,:timestamp])
      io = 0
      s1String = ASCIIString("")

      for subdf in groupby(full,[:session_id,:timestamp])
          s = size(subdf)
          if(SP.debugLevel > 2)
              println("Size=",s," Timer=",subdf[1,:timers_t_done]," rl=",UP.timeLowerMs," ru=",UP.timeUpperMs)
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
                  s1 = subdf[1,:session_id]
                  #println("Session_id $(s1)")
                  s1String = ASCIIString(s1)
                  timeStampVar = subdf[1,:timestamp]
                  timeVarSec = timeVar / 1000.0
                  # We may be missing requests such that the timers_t_done is a little bigger than the treemap
                  labelString = "$(UP.urlFull) $(timeVarSec) Seconds for $(UP.deviceType)"
                  if (SP.debugLevel > 2)
                      println("$(io)/$(SP.showLines): $(labelString),$(UP.urlRegEx)")
                      println("executeSingleSession(TV,UP,SP,",timeVar,",\"",s1,"\",",timeStampVar,") #    Time=",timeVar)
                  end
                  topPageUrl = individualPageData(TV,UP,SP,s1String,timeStampVar)
                  suitable  = individualPageReport(TV,UP,SP,topPageUrl,timeVar,s1String,timeStampVar)
                  if (!suitable)
                      if (SP.debugLevel > 2)
                          println("Not suitable: $(UP.urlRegEx),$(s1String),$(timeStampVar),$(timeVar)")
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
          if SP.debugLevel > 2
              println("calling sessionUrlTableDF")
          end
          toppageurl = sessionUrlTableDF(TV,UP,SP,studySession,studyTime)
          elseif (studySession != "None")
              if SP.debugLevel > 2
                  println("calling allSessionUrlTableDF")
              end
              toppageurl = allSessionUrlTableDF(TV,UP,SP,studySession)
          else
              if SP.debugLevel > 2
                  println("calling allPageUrlTableDF")
              end
              toppageurl = allPageUrlTableDF(TV,UP)
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
          println("Rejecting current page, no data")
          return false
      end

      if (!suitableTest(UP,SP,toppageurl))
          return false
      end

      toppageurl = names!(toppageurl[:,:],
      [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
          symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
          symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

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
          return false
      end

      if (SP.debugLevel > 6)
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
          println("Rejecting current page, no data")
          return false
      end

      if (SP.debugLevel > 8)
        println("Clean Up Data table",size(toppageurl))
      end

      if (!suitableTest(UP,SP,toppageurl))
          return false
      end


      toppageurl = names!(toppageurl[:,:],
      [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
          symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
          symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

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
          #println("Url ",url," newStartTime=$(newStartTime), newTotalTime=$(newTotalTime), target=$(timerDone)")

          #Sorted by start time ascending and largest total time decending
          #Anyone with same time has the previous is nested inside the current one and has no time

          if (newStartTime == prevStartTime)
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
          newTotalTime += toppageurl[i,:Total]
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

# From Page Group Details

function statsPGD(TV::TimeVars,UP::UrlParams)
    try
        localStatsDF = statsTableDF(UP.btView, UP.pageGroup, TV.startTimeMsUTC, TV.endTimeMsUTC);
        statsDF = basicStats(localStatsDF)

        displayTitle(chart_title = "Raw Data Stats $(UP.pageGroup) Based On Beacon Page Load Time", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[2:2,:])
        return statsDF
    catch y
        println("setupStats Exception ",y)
    end
end

# From Page Group Details

function concurrentSessionsPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)
    try
        if (UP.deviceType == "%")
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
        end

        if (UP.deviceType == "Mobile")
            timeString2 = timeString * " - Mobile Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup) - MOBILE ONLY", chart_info = [timeString2],showTimeStamp=false)
            setTable(mobileView)
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

        if (UP.deviceType == "Desktop")
            timeString2 = timeString * " - Desktop Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(productPageGroup) - DESKTOP ONLY", chart_info = [timeString],showTimeStamp=false)
            setTable(desktopView)
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

    catch y
        println("cell concurrentSessionsPGD Exception ",y)
    end
end

# From Page Group Details

function loadTimesPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)
    try

        #todo turn off title in chartLoadTimes
        #displayTitle(chart_title = "Median Load Times for $(productPageGroup)", chart_info = [timeString],showTimeStamp=false)
        if (UP.deviceType == "%")

            chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
        end

        #cannot use the other forms without creating the code for the charts.  Titles cannot be overwritten.
        if (UP.deviceType == "Mobile")

            displayTitle(chart_title = "Median Load Times for $(UP.pageGroup) - MOBILE ONLY", chart_info = [TV.timeString],showTimeStamp=false)
            setTable(mobileView)
            chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

        if (UP.deviceType == "Desktop")
            displayTitle(chart_title = "Median Load Times for $(UP.pageGroup) - DESKTOP ONLY", chart_info = [TV.timeString],showTimeStamp=false)
            setTable(desktopView)
            chartLoadTimes(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

    catch y
        println("cell loadTimesPGD Exception ",y)
    end
end

# From Page Group Details

function loadTimesParamsUPGD(TV::TimeVars,UP::UrlParams)
    try
        chartMedianLoadTimesByDimension(TV.startTimeUTC,TV.endTimeUTC,dimension=:params_u,minPercentage=0.5)

        df = getTopURLsByLoadTime(TV.startTimeUTC, TV.endTimeUTC, minPercentage=0.5);

        sort!(df, cols=:Requests, rev=true)
        display("text/html", """
        <h2 style="color:#ccc">Top URLs By Load Time for $(UP.pageGroup) (Ordered by Requests)</h2>
            """)
        beautifyDF(df);
        catch y
        println("loadTimesParamsUPGD Exception ",y)
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
            toppageurl = sessionUrlTableDF(TV,UP,SP,studySession,studyTime)
            elseif (studySession != "None")
              toppageurl = allSessionUrlTableDF(TV,UP,SP,studySession)
            else
                toppageurl = allPageUrlTableDF(TV,UP)
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

# Large Pages

function statsDetailsPrint(TV::TimeVars,UP::UrlParams,SP::ShowParams,joinTableSummary::DataFrame,row::Int64)
    try
        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])

        UP.deviceType = "Desktop"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Mobile"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Tablet"
        statsFullDF2 = statsTableDF2(TV,UP)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(statsFullDF2)
            dispDMT[3:3,:Unit] = statsDF2[2:2,:unit]
            dispDMT[3:3,:Count] = statsDF2[2:2,:count]
            dispDMT[3:3,:Mean] = statsDF2[2:2,:mean]
            dispDMT[3:3,:Median] = statsDF2[2:2,:median]
            dispDMT[3:3,:Min] = statsDF2[2:2,:min]
            dispDMT[3:3,:Max] = statsDF2[2:2,:max]
        end

        displayTitle(chart_title = "Large Request Stats for: $(topTitle)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(dispDMT)
    catch y
        println("statsDetailsPrint Exception ",y)
    end
end

# from FindATimeSpike

function graphLongTimesFATS(localStats2::DataFrame)
    dataNames = ["Current Long Page Views Completed"]
    axisLabels = ["Timestamps", "Milliseconds to Finish"]

    chart_title="Points above 3 Standard Dev"
    chart_info=["These are the long points only limited to the first 500"]

    colors = ["#EEC584", "rgb(85,134,140)"]

    # kwargs
    point_r = 2

    drawC3Viz(localStats2[1:500,:];  dataNames=dataNames, axisLabels=axisLabels, chart_title=chart_title, chart_info=chart_info, colors=colors, point_r=point_r);
end

# from SQL Data Mining Group

function displayGroup(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        currentPageGroupDF = defaultBeaconsToDF(TV,UP,SP)
        #println("$pageGroup Beacons: ",size(currentPageGroupDF)[1])

        finalPrintDF = DataFrame(count=Int64[],url=ASCIIString[],params_u=ASCIIString[])

        for subDF in groupby(currentPageGroupDF,[:url,:params_u])
            currentGroup = subDF[1:1,:url]
            currentParams = subDF[1:1,:params_u]
            #println(size(subDF,1),"  ",currentGroup[1],"  ",currentParams[1])
            push!(finalPrintDF,[size(subDF,1);subDF[1:1,:url];subDF[1:1,:params_u]])
        end

        displayTitle(chart_title = "Top Beacons Counts (limit $(SP.showLines)) For $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        sort!(finalPrintDF, cols=(order(:count, rev=true)))
        scrubUrlToPrint(SP,finalPrintDF,:params_u)
        beautifyDF(finalPrintDF[1:min(SP.showLines,end),:])
    catch y
        println("displayGroup Exception ",y)
    end
end

function displayTopUrlsByCount(TV::TimeVars,UP::UrlParams,SP::ShowParams,quickPageGroup::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
    UP.pageGroup = quickPageGroup
    defaultBeaconView(TV,UP,SP)
    setTable(UP.btView)
    topUrlTableByCount(TV,UP,SP;rowLimit=rowLimit, beaconsLimit=beaconsLimit, paginate=paginate)
    q = query(""" drop view if exists $(UP.btView);""")
end

function paginatePrintDf(printDF::DataFrame)
    try
        currentLine = 1
        linesOut = 25
        linesToPrint = size(printDF,1)

        while currentLine < linesToPrint
            beautifyDF(printDF[currentLine:min(currentLine+linesOut-1,end),:])
            currentLine += linesOut
        end

    catch y
        println("paginatePrintDf Exception ",y)
    end

end

function cleanupTableFTWP(TV::TimeVars,UP::UrlParams)

    CleanupTable = query("""\
        select
            page_group,
            count(*) as "Page Views"
        FROM $(UP.beaconTable)
        where
            beacon_type = 'page view'
            and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            and page_group in ('Adventure WPF','Animals WPF','Environment WPF','Games WPF','Images WPF',
                                'Movies WPF','Ocean WPF','Photography WPF','Science WPF','Travel WPF')
        GROUP BY page_group
        Order by count(*) desc
    """)

    beautifyDF(CleanupTable[1:min(10,end),:])
end

function setRangeUPT(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        statsDF = DataFrame()
        dv = localTableDF[:timers_t_done]
        statsDF = basicStatsFromDV(dv)

        displayTitle(chart_title = "Raw Data Stats for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        UP.timeLowerMs = statsDF[1:1,:LowerBy3Stddev][1]
        UP.timeUpperMs = statsDF[1:1,:UpperBy3Stddev][1]

        if (SP.debugLevel > 4)
            println("Found Time range $(UP.timeLowerMs) and $(UP.timeUpperMs)")
        end

    catch y
        println("setupStats Exception ",y)
    end

end

function findTopPageUrlUPT(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)

    try
        toppageurl = DataFrame()
        tableRt = UP.resourceTable

        if (LV.studyTime > 0)
            toppageurl = sessionUrlTableDF(tableRt,LV.studySession,LV.studyTime)
        elseif (LV.studySession != "None")
            toppageurl = allSessionUrlTableDF(tableRt,LV.studySession,TV.startTimeMsUTC,TV.endTimeMsUTC)
        else
            toppageurl = allPageUrlTableDF(TV,UP)
        end;

        toppageurl = names!(toppageurl[:,:],
        [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
            symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
            symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);
        return toppageurl
    catch y
        println("cell generate toppageurl Exception ",y)
    end
end

function findTopPageViewUPT(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        if LV.reportLevel > 0
            i = 0
            for url in toppageurl[:urlgroup]
                i += 1
                if url == UP.urlFull
                    printDf = DataFrame()
                    printDf[:Views] = toppageurl[i:i,:request_count]
                    printDf[:Time] = toppageurl[i:i,:beacon_time]
                    printDf[:Url] = toppageurl[i:i,:urlgroup]
                    chartString = "All URLs Used Fall Within ten percent of Mean"
                    displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
                    beautifyDF(names!(printDf[:,:],[symbol("Views"),symbol("Time (ms)"),symbol("Url Used")]))
                end
            end
        end
    catch y
        println("cell report on toppageurl Exception ",y)
    end
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
