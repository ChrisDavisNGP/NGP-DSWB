type LocalVars
    useJson::Bool
end

#function individualStreamlineMain(WellKnownHost::Dict,WellKnownPath::Dict,table::ASCIIString,tableRt::ASCIIString,pageGroup::ASCIIString,
#    fullUrl::ASCIIString,localUrl::ASCIIString,deviceType::ASCIIString,rangeLowerMs::Float64,rangeUpperMs::Float64;
#    showDevView::Bool=false,repeat::Int64=1,showCriticalPathOnly::Bool=true,showDebug::Bool=false,usePageLoad::Bool=true)
  function individualStreamlineMain(TV::TimeVars,UP::UrlParams,SP::ShowParams,WellKnownHost::Dict,WellKnownPath::Dict,
    deviceType::ASCIIString,rangeLowerMs::Float64,rangeUpperMs::Float64)
    try

        #customer = "Nat Geo"
        #reportLevel = 10 # 1 for min output, 5 medium output, 10 all output
        UP.deviceType = deviceType
        UP.timeLowerMs = rangeLowerMs
        UP.timeUpperMs = rangeUpperMs

        localTableDF = DataFrame()
        localTableRtDf = DataFrame()
        statsDF = DataFrame()

        localTableDF = estimateBeacons(TV,UP,SP)
        recordsFound = nrow(localTableDF)

        #println("part 1 done with ",recordsFound, " records")
        if recordsFound == 0
            displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(TV.timeString)",showTimeStamp=false)
            #println("$(fullUrl) for $(deviceType) was not found during $(tv.timeString)")
            return
        end

        # Stats on the data
        statsDF = beaconStats(UP,SP;showAdditional=true)
        rangeLowerMs = statsDF[1:1,:median][1] * 0.95
        rangeUpperMs = statsDF[1:1,:median][1] * 1.05

        #println("part 2 done")
        localTableRtDF = getResourcesForBeacon(TV,UP)
        recordsFound = nrow(localTableRtDF)

        #println("part 1 done with ",recordsFound, " records")
        if recordsFound == 0
            displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) has no resource matches during this time",showTimeStamp=false)
            #println("$(fullUrl) for $(deviceType) was not found during $(tv.timeString)")
            return
        end

        #println("part 3 done")
        showAvailableSessionsStreamline(TV,UP,SP,WellKnownHost,WellKnownPath,localTableDF,localTableRtDF)
        #println("part 4 done")


    catch y
        println("Individual Streamline Main Exception ",y)
    end
end

function individualStreamlineTableV2(UP::UrlParams,SP::ShowParams;repeat::Int64=1)
    try

        # Get Started

        localTableDF = DataFrame()
        localTableRtDf = DataFrame()
        statsDF = DataFrame()

        localTableDF = estimateFullBeaconsV2(TV,UP,SP)
        recordsFound = nrow(localTableDF)

        if (SP.debugLevel > 0)
            println("part 1 done with ",recordsFound, " records")
            if recordsFound == 0
                displayTitle(chart_title = "$(UP.urlFull) for $(UP.deviceType) was not found during $(tv.timeString)",showTimeStamp=false)
            end
        end

        if recordsFound == 0
            row = DataFrame(url=UP.urlFull,beacon_time=0,request_count=0,encoded_size=0,samples=0)
            return row
        end

        # Stats on the data
        row = beaconStatsRow(UP,SP,localTableDF)

        # record the latest record and save to print outside the final loop
        return row

    catch y
        println("Individual Streamline Table Exception ",y)
    end
end

function estimateFullBeaconsV2(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        table = UP.beaconTable
        tableRt = UP.resourceTable

        if (UP.usePageLoad)
            localTableDF = query("""\
            select
                'None' as urlpagegroup,
                avg($tableRt.start_time),
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.start_time) END) as total,
                avg($tableRt.redirect_end-$tableRt.redirect_start) as redirect,
                avg(CASE WHEN ($tableRt.dns_start = 0 and $tableRt.request_start = 0) THEN (0) WHEN ($tableRt.dns_start = 0) THEN ($tableRt.request_start-$tableRt.fetch_start) ELSE ($tableRt.dns_start-$tableRt.fetch_start) END) as blocking,
                avg($tableRt.dns_end-$tableRt.dns_start) as dns,
                avg($tableRt.tcp_connection_end-$tableRt.tcp_connection_start) as tcp,
                avg($tableRt.response_first_byte-$tableRt.request_start) as request,
                avg(CASE WHEN ($tableRt.response_first_byte = 0) THEN (0) ELSE ($tableRt.response_last_byte-$tableRt.response_first_byte) END) as response,
                avg(0) as gap,
                avg(0) as critical,
                CASE WHEN (position('?' in $tableRt.url) > 0) then trim('/' from (substring($tableRt.url for position('?' in substring($tableRt.url from 9)) +7))) else trim('/' from $tableRt.url) end as urlgroup,
                count(*) as request_count,
                'Label' as label,
                avg(CASE WHEN ($tableRt.response_last_byte = 0) THEN (0) ELSE (($tableRt.response_last_byte-$tableRt.start_time)/1000.0) END) as load,
                avg($table.timers_t_done) as beacon_time
            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
                where
                $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
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
                #debugTableDF = query("""\
                #select
                #    count(*) as Count
                #FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
                #    where
                #    $tableRt."timestamp" between $startTimeMs and $endTimeMs
                #    and $table.session_id IS NOT NULL
                #    and $table.page_group ilike '$(UP.pageGroup)'
                #    and $table.params_u ilike '$(UP.urlRegEx)'
                #    and $table.user_agent_device_type ilike '$(UP.deviceType)'
                #    and $table.timers_domready >= $(UP.timeLowerMs) and $table.timers_domready <= $(UP.timeUpperMs)
                #    and $table.params_rt_quit IS NULL
                #group by $table.params_u,$table.session_id,$table."timestamp",errors
                #    """);

                #beautifyDF(debugTableDF[1:min(30,end),:])

                debugTableDF = query("""\
                select
                    *
                FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
                    where
                    $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
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
                sum($tableRt.encoded_size) as encoded_size,
                $table.errors as errors, $table.session_id,$table."timestamp"

            FROM $tableRt join $table on $tableRt.session_id = $table.session_id and $tableRt."timestamp" = $table."timestamp"
                where
                $tableRt."timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
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
        if (SP.mobile)
            UP.deviceType = "mobile"
            row = individualStreamlineTableV2(UP,SP,repeat=1)

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

        if (SP.desktop)
            UP.deviceType = "desktop"
            row = individualStreamlineTableV2(UP,SP,repeat=1)

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

function beaconStatsRow(UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)

    #Make a para later if anyone want to control
    goal = 3000.0

    row = DataFrame()
    row[:url] = UP.urlFull

    dv = localTableDF[:beacon_time]
    statsBeaconTimeDF = limitedStatsFromDV(dv)
    row[:beacon_time] = statsBeaconTimeDF[:median]
    samples = statsBeaconTimeDF[:count]
    if (SP.showDebug)
        println("bt=",row[:beacon_time][1]," goal=",goal)
    end

    if (SP.showDevView)
        if (UP.usePageLoad)
            chartTitle = "Page Load Time Stats: $(UP.urlFull) for $(UP.deviceType)"
        else
            chartTitle = "Page Domain Ready Time Stats: $(UP.urlFull) for $(UP.deviceType)"
        end
        showLimitedStats(statsBeaconTimeDF,chartTitle)
    end

    dv = localTableDF[:request_count]
    statsRequestCountDF = limitedStatsFromDV(dv)
    row[:request_count] = statsRequestCountDF[:median]
    if (SP.showDevView)
        chartTitle = "Request Count"
        showLimitedStats(statsRequestCountDF,chartTitle)
    end

    dv = localTableDF[:encoded_size]
    statsEncodedSizeDF = limitedStatsFromDV(dv)
    row[:encoded_size] = statsEncodedSizeDF[:median]

    row[:samples] = samples

    if (SP.showDevView)
        chartTitle = "Encoded Size"
        showLimitedStats(statsEncodedSizeDF,chartTitle)
    end

    if (SP.showDebug)
        beautifyDF(row[:,:])
    end
    return row
end

function showAvailableSessionsStreamline(TV::TimeVars,UP::UrlParams,SP::ShowParams,WellKnownHost::Dict,WellKnownPath::Dict,localTableDF::DataFrame,localTableRtDF::DataFrame)
    try
        full = join(localTableDF,localTableRtDF, on = [:session_id,:timestamp])
        io = 0
        s1String = ASCIIString("")

        for subdf in groupby(full,[:session_id,:timestamp])
            s = size(subdf)
            if(showDebug)
                println("Size=",s," Timer=",subdf[1,:timers_t_done]," rl=",rangeLowerMs," ru=",rangeUpperMs)
            end
            if (usePageLoad)
                timeVar = subdf[1,:timers_t_done]
            else
                timeVar = subdf[1,:timers_domready]
            end
            if (timeVar >= rangeLowerMs && timeVar <= rangeUpperMs)
                io += 1
                #println("Testing $(io) against $(showLines)")
                if io <= showLines
                    s1 = subdf[1,:session_id]
                    #println("Session_id $(s1)")
                    s1String = ASCIIString(s1)
                    timeStampVar = subdf[1,:timestamp]
                    timeVarSec = timeVar / 1000.0
                    # We may be missing requests such that the timers_t_done is a little bigger than the treemap
                    labelString = "$(fullUrl) $(timeVarSec) Seconds for $(deviceType)"
                    if (showDebug)
                        println("$(io) / $(showLines): $(pageGroup),$(labelString),$(localUrl),$(s1String),$(timeStampVar),$(timeVar),$(showCriticalPathOnly),$(showDevView)")
                    end
                    topPageUrl = individualPageData(pageGroup,localUrl,s1String,timeStampVar,showAdditionals=showDevView,showDebug=showDebug,usePageLoad=usePageLoad)
                    suitable  = individualPageReportV2(WellKnownHost,WellKnownPath,topPageUrl,fullUrl,timeVar,s1String,timeStampVar,showCriticalPathOnly=showCriticalPathOnly,showAdditionals=showDevView,showDebug=showDebug)
                    if (!suitable)
                        showLines += 1
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

function individualPageData(pageGroup::ASCIIString,localUrl::ASCIIString,studySession::ASCIIString,studyTime::Int64
    ;showAdditionals::Bool=true,showDebug::Bool=false,usePageLoad::Bool=true)
    try

        toppageurl = DataFrame()

        if studyTime > 0
            toppageurl = sessionUrlTableDF(tableRt,studySession,studyTime)
            elseif (studySession != "None")
                toppageurl = allSessionUrlTableDF(tableRt,studySession,tv.startTimeMsUTC,tv.endTimeMsUTC)
            else
                toppageurl = allPageUrlTableDF(tableRt,pageGroup,localUrl,rangeLower,rangeUpper,tv.startTimeMsUTC,tv.endTimeMsUTC,deviceType=deviceType,usePageLoad=usePageLoad)
        end;

        return toppageurl

    catch y
        println("individual page report Exception ",y)
    end
end

function individualPageReportV2(WellKnownHost::Dict,WellKnownPath::Dict,toppageurl::DataFrame,fullUrl::ASCIIString,timerDone::Int64,studySession::ASCIIString,studyTime::Int64;
    showCriticalPathOnly::Bool=false,showAdditionals::Bool=true,showDebug::Bool=false)
    try

        #println("Clean Up Data table")
        toppageurl = names!(toppageurl[:,:],
        [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
            symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
            symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);

        toppageurlbackup = deepcopy(toppageurl);
        toppageurl = deepcopy(toppageurlbackup)
        #if showDebug
        #    beautifyDF(toppageurl)
        #end

        removeNegitiveTime(toppageurl,:Total)
        removeNegitiveTime(toppageurl,:Redirect)
        removeNegitiveTime(toppageurl,:Blocking)
        removeNegitiveTime(toppageurl,:DNS)
        removeNegitiveTime(toppageurl,:TCP)
        removeNegitiveTime(toppageurl,:Request)
        removeNegitiveTime(toppageurl,:Response)

        #println("Scrub Data");
        scrubUrlToPrint(toppageurl);
        #println("Classify Data");
        classifyUrl(toppageurl);

        #println("Add Gap and Critical Path")
        toppageurl = gapAndCriticalPathV2(toppageurl,timerDone);
        if (!suitableTest(toppageurl,showDebug=showDebug))
            return false
        end

        if (showAdditionals)
            waterFallFinder(table,studySession,studyTime,tv)
        end

        if (showDebug)
            beautifyDF(toppageurl)
        end

        labelField = fullUrl
        criticalPathTreemapV2(labelField,toppageurl;showTable=showAdditionals,limit=40)

        if (showAdditionals)
            gapTreemapV2(toppageurl,showTable=true,showPageUrl=true,showTreemap=false,limit=40)
        end

        if (!showCriticalPathOnly)
            #itemCountTreemap(toppageurl,showTable=true)      All entries are 1
            endToEndTreemap(toppageurl,showTable=true)
            blockingTreemap(toppageurl,showTable=true)
            requestTreemap(toppageurl,showTable=true)
            responseTreemap(toppageurl,showTable=true)
            dnsTreemap(toppageurl,showTable=true)
            tcpTreemap(toppageurl,showTable=true)
            redirectTreemap(toppageurl,showTable=true)
        end

        return true

    catch y
        println("individual page report Exception ",y)
    end
end


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

        i = 1
        lastRow = 0
        for url in toppageurl[2:end,:urlgroup]
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

        #i = 1
        #for url in toppageurl2[2:end,:urlgroup]
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
        i = 1
        toppageurl[:Critical] = toppageurl[1,:Total]

        for url in toppageurl[2:end,:urlgroup]
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
        println("gapAndCritcalPath Exception ",y)
    end
end

function suitableTest(toppageurl::DataFrame;timerLimitMs::Int64=2000,showDebug::Bool=false)
    try
        i = 1
        lastRow = 0
        for url in toppageurl[2:end,:urlgroup]
            i += 1
            newTotalTime = toppageurl[i,:Total]
            if (newTotalTime > timerLimitMs)
                if (showDebug)
                    println("Dropping page $(url) due to total time of $(newTotalTime)")
                end
                return false
            end
        end

        return true

     catch y
        println("suitableTest Exception ",y)
    end
end

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
