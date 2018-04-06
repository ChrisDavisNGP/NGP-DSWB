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
          row = individualStreamlineTableV2(TV,UP,SP)

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
          row = individualStreamlineTableV2(TV,UP,SP)

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
  [Symbol("Recent Urls $(additional)");Symbol("Time");Symbol("Request Made");Symbol("Page Size");Symbol("Samples")])
  beautifyDF(ft[1:min(100,end),:])

  catch y
      println("finalUrlTableOutput Exception ",y)
  end
end

# From Individual-Streamline-Body

function beaconStatsRow(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)

  try
      #Make a para later if anyone want to control
      goal = 3000.0

      row = DataFrame()
      row[:url] = UP.urlFull

      dv = localTableDF[:beacon_time]
      statsBeaconTimeDF = singleRowStatsFromDV(dv)
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
      statsRequestCountDF = singleRowStatsFromDV(dv)
      row[:request_count] = statsRequestCountDF[:median]
      if (SP.devView)
          chartTitle = "Request Count"
          showLimitedStats(TV,statsRequestCountDF,chartTitle)
      end

      dv = localTableDF[:encoded_size]
      statsEncodedSizeDF = singleRowStatsFromDV(dv)
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
  catch y
      println("beaconStatsRow Exception ",y)
  end
end

# From Page Group Details

function concurrentSessionsPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)
    try
        if SP.debugLevel > 8
            println("Starting concurrentSessionsPGD")
        end

        if (UP.deviceType == "%")
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
        end

        if (UP.deviceType == "Mobile")
            timeString2 = timeString * " - Mobile Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(UP.pageGroup) - MOBILE ONLY", chart_info = [TV.timeString2],showTimeStamp=false)
            setTable(mobileView)
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

        if (UP.deviceType == "Desktop")
            timeString2 = timeString * " - Desktop Only"
            #displayTitle(chart_title = "Concurrent Sessions and Beacons for $(UP.pageGroup) - DESKTOP ONLY", chart_info = [TV.timeString],showTimeStamp=false)
            setTable(desktopView)
            chartConcurrentSessionsAndBeaconsOverTime(TV.startTimeUTC, TV.endTimeUTC, TV.datePart)
            setTable(UP.btView)
        end

    catch y
        println("cell concurrentSessionsPGD Exception ",y)
    end
end


function loadTimesPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams,mobileView::ASCIIString,desktopView::ASCIIString)
    try
        if SP.debugLevel > 8
            println("Starting localTimesPGD")
        end

        #todo turn off title in chartLoadTimes
        #displayTitle(chart_title = "Median Load Times for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
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
        if SP.debugLevel > 8
            println("Starting statsDetailsPrint")
        end

        topUrl = string(joinTableSummary[row:row,:urlgroup][1],"%")
        topTitle = joinTableSummary[row:row,:urlgroup][1]

        dispDMT = DataFrame(RefGroup=["","",""],Unit=["","",""],Count=[0,0,0],Mean=[0.0,0.0,0.0],Median=[0.0,0.0,0.0],Min=[0.0,0.0,0.0],Max=[0.0,0.0,0.0])

        UP.deviceType = "Desktop"
        statsFullDF2 = statsBtViewTableToDF(UP)
        dispDMT[1:1,:RefGroup] = "Desktop"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(UP,statsFullDF2)
            dispDMT[1:1,:Unit] = statsDF2[2:2,:unit]
            dispDMT[1:1,:Count] = statsDF2[2:2,:count]
            dispDMT[1:1,:Mean] = statsDF2[2:2,:mean]
            dispDMT[1:1,:Median] = statsDF2[2:2,:median]
            dispDMT[1:1,:Min] = statsDF2[2:2,:min]
            dispDMT[1:1,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Mobile"
        statsFullDF2 = statsBtViewTableToDF(UP)
        dispDMT[2:2,:RefGroup] = "Mobile"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(UP,statsFullDF2)
            dispDMT[2:2,:Unit] = statsDF2[2:2,:unit]
            dispDMT[2:2,:Count] = statsDF2[2:2,:count]
            dispDMT[2:2,:Mean] = statsDF2[2:2,:mean]
            dispDMT[2:2,:Median] = statsDF2[2:2,:median]
            dispDMT[2:2,:Min] = statsDF2[2:2,:min]
            dispDMT[2:2,:Max] = statsDF2[2:2,:max]
        end
        UP.deviceType = "Tablet"
        statsFullDF2 = statsBtViewTableToDF(UP)
        dispDMT[3:3,:RefGroup] = "Tablet"
        if (size(statsFullDF2)[1] > 0)
            statsDF2 = basicStats(UP,statsFullDF2)
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

function displayGroupBody(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    try
        currentResourceDF = defaultResourcesToDF(TV,UP,SP)
        displayTitle(chart_title = "Resource Fields For $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(currentResourceDF[1:min(SP.showLines,end),:])

        currentPageGroupDF = defaultBeaconsToDF(TV,UP,SP)
        displayTitle(chart_title = "Beacon Fields For $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(currentPageGroupDF[1:min(SP.showLines,end),:])
        #println("$pageGroup Beacons: ",size(currentPageGroupDF)[1])

        finalPrintDF = DataFrame(count=Int64[],url=ASCIIString[],params_u=ASCIIString[])

        for subDF in groupby(currentPageGroupDF,[:url,:params_u])
            currentGroup = subDF[1:1,:url]
            currentParams = subDF[1:1,:params_u]
            #println(size(subDF,1),"  ",currentGroup[1],"  ",currentParams[1])
            push!(finalPrintDF,[size(subDF,1);subDF[1:1,:url];subDF[1:1,:params_u]])
        end

        displayTitle(chart_title = "Top Beacons Counts (show limit $(SP.showLines)) For $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        sort!(finalPrintDF, cols=(order(:count, rev=true)))
        scrubUrlToPrint(SP,finalPrintDF,:params_u)
        beautifyDF(finalPrintDF[1:min(SP.showLines,end),:])


    catch y
        println("displayGroupBody Exception ",y)
    end
end

function displayTopUrlsByCount(TV::TimeVars,UP::UrlParams,SP::ShowParams,quickPageGroup::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
    UP.pageGroup = quickPageGroup
    defaultBeaconCreateView(TV,UP,SP)
    setTable(UP.btView)
    topUrlTableByCountPrintTable(TV,UP,SP;rowLimit=rowLimit, beaconsLimit=beaconsLimit, paginate=paginate)
    q = query(""" drop view if exists $(UP.btView);""")
end

function paginatePrintDf(printDF::DataFrame)
    try
        currentLine = 1
        linesPerPage = 25
        linesToPrint = size(printDF,1)

        while currentLine < linesToPrint
            beautifyDF(printDF[currentLine:min(currentLine+linesPerPage-1,end),:])
            currentLine += linesPerPage
        end

    catch y
        println("paginatePrintDf Exception ",y)
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

function resourceMatched(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
        select count(*)
        from $(UP.resourceTable)
        where
            "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
             url ilike '$(UP.resRegEx)'
        """);

        displayTitle(chart_title = "Matches For Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceMatched Exception ",y)
    end
end

function resourceSize(TV::TimeVars,UP::UrlParams,SP::ShowParams;minEncoded::Int64=1000)

    try
        joinTables = query("""\
            select count(*),encoded_size,transferred_size,decoded_size,url
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                encoded_size > $(minEncoded)
            group by encoded_size,transferred_size,decoded_size,url
            order by count(*) desc
        """);

        displayTitle(chart_title = "Size Details For Resource Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])

        dv1 = Array{Float64}(joinTables[:encoded_size])
        statsDF1 = singleRowStatsFromDV(dv1)
        showLimitedStats(statsDF1,"Encoded Size Stats (Page Views are Groups In Above Table)")
        dv2 = Array{Float64}(joinTables[:transferred_size])
        statsDF2 = singleRowStatsFromDV(dv2)
        showLimitedStats(statsDF2,"Transferred Size Stats")

    catch y
        println("resourceSize Exception ",y)
    end
end

function resourceSummary(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
        select count(*),url
        from $(UP.resourceTable)
        where
            "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
             url ilike '$(UP.resRegEx)'
        group by
        url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Resource Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceSummary Exception ",y)
    end
end

function resourceSummaryAllFields(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
        select *
        from $(UP.resourceTable)
        where
          url ilike '$(UP.resRegEx)' and
          "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
        limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceSummaryAllFields Exception ",y)
    end
end

function resourceSummaryDomainUrl(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
            select count(*),url,params_u
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url, params_u
            order by count(*) desc
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Domain Url For Resource Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceSummaryDomainUrl Exception ",y)
    end
end

function resourceTime1(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
            select count(*), avg(start_time) as "Start Time",
                avg(fetch_start) as "Fetch Start",
                avg(dns_end-dns_start) as "DNS ms",
                avg(tcp_connection_end-tcp_connection_start) as "TCP ms",
                avg(request_start) as "Req Start",
                avg(response_first_byte) as "Req FB",
                avg(response_last_byte) as "Req LB",
                max(response_last_byte) as "Max Req LB",
                url,
                avg(redirect_start) as "Redirect Start",
                avg(redirect_end) as "Redirect End",
                avg(secure_connection_start) as "Secure Conn Start"
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            group by url
            order by count(*) desc
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceTime1 Exception ",y)
    end
end

function resourceTime2(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        timeTable = query("""\
            select (response_last_byte-start_time) as "Time Taken",
                (start_time) as "Start Time",
                (fetch_start) as "Fetch Start",
                (dns_end-dns_start) as "DNS ms",
                (tcp_connection_end-tcp_connection_start) as "TCP ms",
                (request_start) as "Req Start",
                (response_first_byte) as "Req FB",
                (response_last_byte) as "Req LB",
                url,
                (redirect_start) as "Redirect Start",
                (redirect_end) as "Redirect End",
                (secure_connection_start) as "Secure Conn Start"
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                (response_last_byte-start_time) > 75 and (response_last_byte-start_time) < 10000
            order by "Time Taken" desc
        """);

        #todo remove negitives

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(timeTable[1:min(SP.showLines,end),:])

        timeTable = names!(timeTable[:,:],
        [Symbol("taken"),Symbol("start"),Symbol("fetch"),Symbol("dns"),Symbol("tcp"),Symbol("req_start"),Symbol("req_fb"),Symbol("req_lb"),Symbol("url")
            ,Symbol("redirect_start"),Symbol("redirect_end"),Symbol("secure_conn_start")])

        dv1 = Array{Float64}(timeTable[:taken])
        statsDF1 = singleRowStatsFromDV(dv1)
        showLimitedStats(TV,statsDF1,"Time Taken Stats")

        dv2 = Array{Float64}(timeTable[:dns])
        statsDF2 = singleRowStatsFromDV(dv2)
        showLimitedStats(TV,statsDF2,"DNS Stats")

        dv3 = Array{Float64}(timeTable[:tcp])
        statsDF3 = singleRowStatsFromDV(dv3)
        showLimitedStats(TV,statsDF3,"TCP Stats")

        dv4 = Array{Float64}(timeTable[:start])
        statsDF4 = singleRowStatsFromDV(dv4)
        showLimitedStats(TV,statsDF4,"Start Time On Page Stats")

        dv5 = Array{Float64}(timeTable[:fetch])
        statsDF5 = singleRowStatsFromDV(dv5)
        showLimitedStats(TV,statsDF5,"Fetching Request Stats")

        dv6 = Array{Float64}(timeTable[:req_start])
        statsDF6 = singleRowStatsFromDV(dv6)
        showLimitedStats(TV,statsDF6,"Request Start Stats")

        dv7 = Array{Float64}(timeTable[:req_fb])
        statsDF7 = singleRowStatsFromDV(dv7)
        showLimitedStats(TV,statsDF7,"Request First Byte Stats")

        dv8 = Array{Float64}(timeTable[:req_lb])
        statsDF8 = singleRowStatsFromDV(dv8)
        showLimitedStats(TV,statsDF8,"Request Last Byte Stats")

    catch y
        println("resourceTime2 Exception ",y)
    end
end

function resourceTime3(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        joinTables = query("""\
            select (response_last_byte-start_time) as "Time Taken",
                (start_time) as "Start Time",
                (fetch_start) as "Fetch S",
                (dns_end-dns_start) as "DNS ms",
                (tcp_connection_end-tcp_connection_start) as "TCP ms",
                (request_start) as "Req S",
                (response_first_byte) as "Req FB",
                (response_last_byte) as "Req LB",
                url,
                params_u,
                (redirect_start) as "Redirect S",
                (redirect_end) as "Redirect E",
                (secure_connection_start) as "Secure Conn S"
            from $(UP.resourceTable)
            where
                url ilike '$(UP.resRegEx)' and
                "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                start_time > 10000
            order by start_time desc
            limit $(UP.limitQueryRows)
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(UP.resRegEx)", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceTime3 Exception ",y)
    end
end

function determinePageConstructionBody(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    defaultBeaconCreateView(TV,UP,SP)
    btv = UP.btView

    # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
    # where beacon_type = 'page view'
    cnt = query("""SELECT count(*) FROM $btv""")
    #Hide output from final report
    println("$btv count is ",cnt[1,1])

    try

        displayTitle(chart_title = "Big Pages Treemap Report (Min 3MB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        domSize = query("""\
            select count(*),AVG(params_dom_sz) beacons,
                AVG(timers_t_page)/1000 load_time,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL
                and params_dom_sz > 3000000
            group by urlgroup
            order by beacons desc
            limit $(UP.limitQueryRows)
        """);

        beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views");Symbol("Avg Size");Symbol("Avg Load Time (sec)");Symbol("URL Group")]))

        fieldNames = [:urlgroup]
        domSize[:x1] = "URLs Total Size"
        drawTree(domSize;fieldNames=fieldNames)
    catch y
        println("urlTotalSizeTreemap Exception ",y)
    end

    try
        displayTitle(chart_title = "Total Bytes Used (Size x Views) Treemap Report (Min 2 MB Pages)", chart_info = [TV.timeString], showTimeStamp=false)
        domSize = query("""\
            select count(*),SUM(params_dom_sz) beacons,
                AVG(timers_t_page)/1000 load_time,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL
                and params_dom_sz > 2000000
            group by urlgroup
            order by beacons desc
            limit $(UP.limitQueryRows)
        """);

        #display(names!(domSize[1:end,[1:4]],[Symbol("Page Views"),Symbol("Total Size"),Symbol("Avg Load Time(sec)"),Symbol("URL Group")]))
        beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views");Symbol("Total Size");Symbol("Avg Load Time (sec)");Symbol("URL Group")]))
        #display(domSize[1:end,:])

        fieldNames = [:urlgroup]
        domSize[:x1] = "URLs Total Size"
        drawTree(domSize;fieldNames=fieldNames)
    catch y
        println("urlTotalSizeTreemap Exception ",y)
    end

    try
        displayTitle(chart_title = "Unique Domains Used", chart_info = [TV.timeString], showTimeStamp=false)

        domSize = query("""\
            select count(*),AVG(params_dom_doms) avgsize,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_doms > 50
                and params_dom_doms IS NOT NULL
            group by urlgroup
            order by avgsize desc
            limit $(UP.limitQueryRows)
        """);
        beautifyDF(names!(domSize[1:end,[1:3]],[Symbol("Views"),Symbol("Avg Domains"),Symbol("URL Group")]))
    catch y
        println("uniqueDomainsUsed Exception ",y)
    end

    try
        displayTitle(chart_title = "Domains Nodes On Page (20k min)", chart_info = [TV.timeString], showTimeStamp=false)

        domSize = query("""\
            select count(*),AVG(params_dom_ln) avgsize,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_ln > 20000
                and params_dom_ln IS NOT NULL
            group by urlgroup
            order by avgsize desc
            limit $(UP.limitQueryRows)
        """);

        beautifyDF(names!(domSize[1:end,[1:3]],[Symbol("Views"),Symbol("Avg Nodes"),Symbol("URL Group")]))
    catch y
        println("domainNodesOnPage Exception ",y)
    end

    try
        displayTitle(chart_title = "Domains Images", chart_info = [TV.timeString], showTimeStamp=false)

        domSize = query("""\
            select count(*) cnt,AVG(params_dom_img) avgsize,
                AVG(params_dom_img_ext) avgsizeext,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_img > 100
                and params_dom_img IS NOT NULL
                and params_dom_img_ext IS NOT NULL
            group by urlgroup
            order by avgsize desc
            limit $(UP.limitQueryRows)
        """);
        beautifyDF(names!(domSize[:,[1:4]],[Symbol("Views"),Symbol("Avg Images"),Symbol("Avg Images External"),Symbol("URL Group")]))
    catch y
        println("domainsImages Exception ",y)
    end

    try
        displayTitle(chart_title = "Frequently Used Images", chart_info = [TV.timeString], showTimeStamp=false)

        domSize = query("""\
            select count(*) cnt,SUM(params_dom_img) avgsize,
                SUM(params_dom_img_ext) avgsizeext,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_img > 500
                and params_dom_img IS NOT NULL
                and params_dom_img_ext IS NOT NULL
            group by urlgroup
            order by CNT desc
            limit $(UP.limitQueryRows)
        """);
        beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Sum Images"),Symbol("Sum Images External"),Symbol("URL Group")]))
    catch y
        println("frequentlyUsedImages Exception ",y)
    end

    try
        displayTitle(chart_title = "Domains Scripts", chart_info = [TV.timeString], showTimeStamp=false)

        #params_dom_img,params_dom_img_ext,
        #params_dom_script,params_dom_script_ext,

        domSize = query("""\
            select count(*) cnt,AVG(params_dom_script) avgsize,
                AVG(params_dom_script_ext) avgsizeext,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_script > 100
                and params_dom_script IS NOT NULL
                and params_dom_script_ext IS NOT NULL
            group by urlgroup
            order by avgsize desc
            limit $(UP.limitQueryRows)
        """);
        beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Scripts"),Symbol("Avg Scripts External"),Symbol("URL Group")]))
    catch y
        println("domainScripts Exception ",y)
    end

    try
        displayTitle(chart_title = "Frequently Used Scripts", chart_info = [TV.timeString], showTimeStamp=false)

        #params_dom_img,params_dom_img_ext,
        #params_dom_script,params_dom_script_ext,

        domSize = query("""\
            select count(*) cnt,SUM(params_dom_script) avgsize,
                SUM(params_dom_script_ext) avgsizeext,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_script > 200
                and params_dom_script IS NOT NULL
                and params_dom_script_ext IS NOT NULL
            group by urlgroup
            order by cnt desc
            limit $(UP.limitQueryRows)
        """);
        beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Total Scripts"),Symbol("Total Scripts External"),Symbol("URL Group")]))
    catch y
        println("frequentlyUsedScripts Exception ",y)
    end

    sizeTrend = DataFrame()

    try
        #displayTitle(chart_title = "Big Pages Treemap Report (Min 3MB Pages)", chart_info = [TV.timeString], showTimeStamp=false)

        sizeTrend = query("""\
            select params_h_t,params_dom_sz size,
                CASE
                    when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
                    else trim('/' from params_u)
                end urlgroup
            from $btv
            where
                params_dom_sz IS NOT NULL
            limit $(UP.limitQueryRows)
        """);

        #beautifyDF(names!(domSize[1:end,[1:4]],[Symbol("Views"),Symbol("Avg Size"),Symbol("Avg Load Time (sec)"),Symbol("URL Group")]))
        sizeof(sizeTrend[1:end,:])
    catch y
        println("setupSizeTrend Exception ",y)
    end

    try
        delete!(sizeTrend,[:urlgroup])
    catch y
        println("cleanupSizeTrend Exception ",y)
    end

    try
        dataNames = ["Dom Byte Size"]
        drawC3Viz(sizeTrend, dataNames=dataNames);
    catch y
        println("graphSizeTrend Exception ",y)
    end
end

function bigPages1SRFLP(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if SP.debugLevel > 8
        println("Starting bigPages1SRFLP")
    end

    try
        btv = UP.btView

        statsDF = DataFrame()

        localDF = query("""SELECT params_dom_sz FROM $btv""")
        dv = localDF[:params_dom_sz]
        statsDF = basicStatsFromDV(dv)
        statsDF[:unit] = "KBytes"
        minSizeBytes = statsDF[1:1,:UpperBy3Stddev][1]

        displayTitle(chart_title = "Domain Size in KB", chart_info = [TV.timeString], showTimeStamp=false)
        beautifyDF(statsDF)

        return minSizeBytes

    catch y
        println("setupLocalTable Exception ",y)
    end
end

function largeResourceFileTypePrint(TV::TimeVars,UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    imagesDf = resourceImagesOnNatGeoToDF(UP,SP,fileType)
    if (size(imagesDf)[1] > 0)
        idImageMgrPolicy(SP,imagesDf)
    end

end
