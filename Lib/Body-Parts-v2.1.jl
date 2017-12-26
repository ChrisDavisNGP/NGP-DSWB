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
              sum($rt.encoded_size) as encoded_size
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

  x = 0
  for testUrl in topUrls

      if size(finalTable,1) > SP.showLines   #Counting the header line
          break
      end

      x += 1
      if (x % 10) == 0
          println("Urls Completed: $x")
      end
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
          additional = join(["(Sorted By Size Descending; Min Samples ";UP.samplesMin;"; Top ";SP.showLines;" Page Views)"]," ")
  else
      sort!(finalTable,cols=:beacon_time, rev=true)
          additional = join(["(Sorted By Time Descending; Min Samples ";UP.samplesMin;"; Top ";SP.showLines;" Page Views)"]," ")
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
  if (SP.debugLevel > 4)
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

  if (SP.debugLevel > 8)
      beautifyDF(row[:,:])
  end
  return row
end

# From Page Group Details

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
    defaultBeaconCreateView(TV,UP,SP)
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
            toppageurl = sessionUrlTableCreateDF(tableRt,LV.studySession,LV.studyTime)
        elseif (LV.studySession != "None")
            toppageurl = allSessionUrlTableCreateDF(tableRt,LV.studySession,TV.startTimeMsUTC,TV.endTimeMsUTC)
        else
            toppageurl = allPageUrlTableCreateDF(TV,UP)
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

function idImageMgrPolicy(SP::ShowParams,imageDf::DataFrame)
    try

        urlPatterns = knownPatterns()
        for (url in imageDf[:,:url])
            found = false
            for key in keys(urlPatterns)
                if (ismatch(key,url))
                    value = get(urlPatterns,key,"None")
                    #println("Found ",value)
                    found = true
                    urlPatterns[key] = [value[1],value[2],value[3],value[4],value[5]+1,url]
                    break
                end
            end
            if (!found)
                #println("Missed ",url)
                regExpStart = search(url,".com")[2]+3
                regExpStr = url[regExpStart:end]
                regExpEnd = min(60,length(regExpStr))
                regExpStr = regExpStr[1:regExpEnd]

                #println("(r\".*",regExpStr)
                println("Not Found: (r\".*",regExpStr,"\",[\"\",\"\",\"",regExpStr,"\",\"",regExpStr,"\",0,\"\"]),")
#(r".*/adventure/features/everest/first-woman-to-climb-everest-jun",["/adventure/features/everest/first-woman-to-climb-everest-jun"","",",0,""]),
            end
        end

        println("\n\n\nHere are the patterns matched for large files:\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Known")
                println("$(value[5])\tKnown: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Ignoring")
                println("$(value[5])\tIgnore: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Sponsor")
                println("$(value[5])\tSponsor: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "")
                println("$(value[5])\tPending: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")


    catch y
        println("idImageMgrPolicy Exception ",y)
    end
end


function knownPatterns()
    try

        urlPatterns = Dict([

            (r".*/content/dam/.*",["Image Mgr","Known",".*/content/dam/.*","Content Dam",0,""]),
            (r".*/interactive-assets/.*",["Interactive Assets","Ignoring",".*/interactive-assets/.*","Interactive Assets",0,""]),

#            (r".*/content/dam/travel/.*",["Content Dam Travel",0,""]),
#            (r".*/content/dam/photography/.*",["Photography",0,""]),
#            (r".*/content/dam/adventure/.*",["Content Dam Adventure",0,""]),
#            (r".*/content/dam/archaeologyandhistory/.*",["Content Dam Archaeologyandhistory",0,""]),
#            (r".*/content/dam/magazine/.*",["Content Dam Magazine",0,""]),
#            (r".*/content/dam/environment/.*",["Content Dam Environment",0,""]),
#            (r".*/content/dam/news/.*",["content Dam News",0,""]),
#            (r".*/content/dam/science/.*",["Content Dam Science",0,""]),
#            (r".*/content/dam/contributors/.*",["Content Dam Contributors",0,""]),
#            (r".*/content/dam/natgeo/video/.*",["Content Dam Video",0,""]),
#            (r".*/content/dam/parks/.*",["Content Dam Parks",0,""]),
#            (r".*/content/dam/animals/.*",["Content Dam Animals",0,""]),
#            (r".*/content/dam/ngdotcom/.*",["Content Dam Ngdotcom",0,""]),
#            (r".*/content/dam/peopleandculture/.*",["Content Dam People and Culture",0,""]),
#            (r".*/content/dam/books/.*",["Content Dam Books",0,""]),

            (r".*/adventure/features/.*",["","",".*/adventure/features/.*","Adventure Features",0,""]),
            (r".*/contributors/r/melody-rowell/.*",["","",".*/contributors/r/melody-rowell/.*","Contributors Melody Rowell",0,""]),
            (r".*/countryman/assets/.*",["","","/countryman/assets/.*","Countryman",0,""]),
            (r".*/foodfeatures/.*",["","",".*/foodfeatures/.*","Food Features",0,""]),
            (r".*/new-york-city-skyline-tallest.*",["","",".*/new-york-city-skyline-tallest.*","New York Skyline",0,""]),
            (r".*/cosmic-dawn/.*",["","",".*/cosmic-dawn/.*","Cosmic Dawn",0,""]),
            (r".*/taking-back-detroit/.*",["","",".*/taking-back-detroit/.*","Taking Back Detroit",0,""]),
            (r".*/americannile/.*",["","",".*/americannile/.*","American Nile",0,""]),
            (r".*/healing-soldiers/.*",["","",".*/healing-soldiers/.*","Healing Soldiers",0,""]),
            (r".*/environment/global-warming/.*",["","",".*/environment/global-warming/.*","Global Warming",0,""]),
            (r".*/magazines/pdf/.*",["","",".*/magazines/pdf/.*","Magazines Pdf",0,""]),
            (r".*/worldlegacyawards/.*",["","",".*/worldlegacyawards/.*","World Legacy Awards",0,""]),
            (r".*/news-features/son-doong-cave/.*",["","",".*/news-features/son-doong-cave/.*","News Features Son-doong-cave",0,""]),
            (r".*/trajan-column/.*",["","",".*/trajan-column/.*","Trajan Column",0,""]),
            (r".*/magazines/l/multisubs/images/.*",["","",".*/magazines/l/multisubs/images/.*","Magazines Multisubs",0,""]),
            (r".*/astrobiology/.*",["","",".*/astrobiology/.*","Astrobiology",0,""]),
            (r".*/alwaysexploring/.*",["","",".*/alwaysexploring/.*","Always Exploring",0,""]),
            (r".*/annual-report-.*",["","",".*/annual-report-.*","Annual Report",0,""]),
            (r".*/china-caves/.*",["","",".*/china-caves/.*","China Caves",0,""]),
            (r".*/clean-water-access-.*",["","",".*/clean-water-access-.*","Clean Water Access",0,""]),
            (r".*/climate-change/.*",["","",".*/climate-change/.*","Climate Change",0,""]),
            (r".*/discoverjapancontest/.*",["","",".*/discoverjapancontest/.*","Discover Japan Contest",0,""]),
            (r".*/gecpartnershowcase/.*",["","",".*/gecpartnershowcase/.*","Gec Partner Showcase",0,""]),
            (r".*/giftguide/.*",["","",".*/giftguide/.*","Gift Guide",0,""]),
            (r".*/hubble-timeline/.*",["","",".*/hubble-timeline/.*","Hubble Timeline",0,""]),
            (r".*/hurricane-katrina-new-orleans.*",["","",".*/hurricane-katrina-new-orleans.*","Hurricane Katrina",0,""]),
            (r".*/impact-report-.*",["","",".*/impact-report-.*","Impact Report",0,""]),
            (r".*/journeytojordan/.*",["","",".*/journeytojordan/.*","Journey To Jordan",0,""]),
            (r".*/love-collection-.*",["","",".*/love-collection-.*","Love Collection",0,""]),
            (r".*/loveswitzerland/.*",["","",".*/loveswitzerland/.*","Love Switzerland",0,""]),
            (r".*/magazine/201.*",["","",".*/magazine/201.*","Magazine 20xx",0,""]),
            (r".*/memorablemoments/.*",["","",".*/memorablemoments/.*","Memorable Moments",0,""]),
            (r".*/mindsuckers/.*",["","",".*/mindsuckers/.*","Mindsuckers",0,""]),
            (r".*/myaway/.*",["","",".*/myaway/.*","Myaway",0,""]),
            (r".*/people-and-culture/.*",["","",".*/people-and-culture/.*","People And Culture",0,""]),
            (r".*/promo/ngtseminars/.*",["","",".*/promo/ngtseminars/.*","Promo Ngtseminars",0,""]),
            (r".*/staralliance20/.*",["","",".*/staralliance20/.*","Star Alliance",0,""]),
            (r".*/sunrise-to-sunset/.*",["","",".*/sunrise-to-sunset/.*","Sunrise To Sunset",0,""]),
            (r".*/tracking-ivory/.*",["","",".*/tracking-ivory/.*","Tracking Ivory",0,""]),
            (r".*/travelmarketplace/.*",["","",".*/travelmarketplace/.*","Travel Marketplace",0,""]),
            (r".*/usofadventure/.*",["","",".*/usofadventure/.*","Us Of Adventure",0,""]),
            (r".*/voteyourpark/.*",["","",".*/voteyourpark/.*","Vote Your Park",0,""]),
            (r".*/west-snow-fail/.*",["","",".*/west-snow-fail/.*","West Snow Fail",0,""]),
            (r".*/year-in-review-.*",["","",".*/year-in-review-.*","Year In Review",0,""]),


            (r".*/visitpandora/.*",["Sponsor","Sponsor",".*/visitpandora/.*","Visit Pandora",0,""]),
            (r".*/microsoft/.*",["Sponsor","Sponsor",".*/microsoft/.*","Microsoft",0,""]),
            (r".*/stellaartois/.*",["Sponsor","Sponsor",".*/stellaartois/.*","Stella Artois",0,""]),
            (r".*/subaru/.*",["Sponsor","Sponsor",".*/subaru/.*","Subaru",0,""]),
            (r".*/visitcalifornia/.*",["Sponsor","Sponsor",".*/visitcalifornia/.*","Visit California",0,""]),
            (r".*/cisco/.*",["Sponsor","Sponsor",".*/cisco/.*","Cisco",0,""]),


            (r".*/unchartedwaters/.*",["Sponsor","Sponsor",".*/unchartedwaters/.*","Unchartedwaters",0,""])
            ]);

        return urlPatterns

    catch y
        println("knownPatterns Exception ",y)
    end
end

function resourceMatched(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        count(*)
        from $tableRt
        where
            "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and
             url ilike '$resourceUrl'
        """);

        displayTitle(chart_title = "Matches For Url Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceScreen(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        count(*),
        initiator_type,
        height,
        width,
        x,
        y,
        url
        from $tableRt
        where
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        group by initiator_type,height,width,x,y,url
        order by count(*) desc
        limit $(linesOut)
        """);

        displayTitle(chart_title = "Screen Details For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSize(tableRt::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
        joinTables = query("""\
        select
        count(*),
        encoded_size,
        transferred_size,
        decoded_size,
        url
        from $tableRt
        where
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and
        encoded_size > $(minEncoded)
        group by encoded_size,transferred_size,decoded_size,url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Size Details For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])

        dv1 = joinTables[:encoded_size]
        statsDF1 = limitedStatsFromDV(dv1)
        showLimitedStats(statsDF1,"Encoded Size Stats (Page Views are Groups In Above Table)")
        dv2 = joinTables[:transferred_size]
        statsDF2 = limitedStatsFromDV(dv2)
        showLimitedStats(statsDF2,"Transferred Size Stats")

    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSummary(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        count(*),url
        from $tableRt
        where
            "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and
             url ilike '$resourceUrl'
        group by
        url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSummaryAllFields(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        *
        from $tableRt
        where
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        limit $(linesOut)
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function resourceSummaryDomainUrl(tableRt::ASCIIString;linesOut::Int64=25)

    try
        joinTables = query("""\
        select
        count(*),
        url,params_u
        from $tableRt
        where 
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        group by
            url, params_u
        order by count(*) desc
        limit 25
        """);

        displayTitle(chart_title = "Domain Url For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end
