# Version 1.1 add timers_domready including in the SQL queries

function individualStreamlineMain(WellKnownHost::Dict,WellKnownPath::Dict,table::ASCIIString,tableRt::ASCIIString,pageGroup::ASCIIString,
    fullUrl::ASCIIString,localUrl::ASCIIString,deviceType::ASCIIString,rangeLowerMs::Float64,rangeUpperMs::Float64; 
    showDevView::Bool=false,repeat::Int64=1,showCriticalPathOnly::Bool=true,showDebug::Bool=false,usePageLoad::Bool=true)
    try 

        #customer = "Nat Geo" 
        #reportLevel = 10 # 1 for min output, 5 medium output, 10 all output
        
        localTableDF = DataFrame()
        localTableRtDf = DataFrame()
        statsDF = DataFrame()
        
        localTableDF = estimateBeacons(table,tv.startTimeMsUTC,tv.endTimeMsUTC,pageGroup=pageGroup,localUrl=localUrl,deviceType=deviceType,rangeLowerMs=rangeLowerMs,rangeUpperMs=rangeUpperMs)
        recordsFound = nrow(localTableDF)
        
        #println("part 1 done with ",recordsFound, " records")
        if recordsFound == 0
            displayTitle(chart_title = "$(fullUrl) for $(deviceType) was not found during $(tv.timeString)",showTimeStamp=false)
            #println("$(fullUrl) for $(deviceType) was not found during $(tv.timeString)")
            return
        end
        
        # Stats on the data
        statsDF = beaconStatsPBI(localTableDF,fullUrl,deviceType;showAdditional=true,usePageLoad=usePageLoad)
        rangeLowerMs = statsDF[1:1,:median][1] * 0.95
        rangeUpperMs = statsDF[1:1,:median][1] * 1.05

        #println("part 2 done")        
        localTableRtDF = getResourcesForBeacon(table,tableRt,pageGroup,localUrl,deviceType,rangeLowerMs,rangeUpperMs) 
        recordsFound = nrow(localTableRtDF)
        
        #println("part 1 done with ",recordsFound, " records")
        if recordsFound == 0
            displayTitle(chart_title = "$(fullUrl) for $(deviceType) has no resource matches during this time",showTimeStamp=false)
            #println("$(fullUrl) for $(deviceType) was not found during $(tv.timeString)")
            return
        end
        
        #println("part 3 done")
        showAvailableSessionsStreamline(WellKnownHost,WellKnownPath,localTableDF,localTableRtDF,pageGroup,deviceType,rangeLowerMs,rangeUpperMs,fullUrl,localUrl,showLines=repeat,
        showCriticalPathOnly=showCriticalPathOnly,showDevView=showDevView,showDebug=showDebug,usePageLoad=usePageLoad)
        #println("part 4 done")
        
        
    catch y
        println("Individual Streamline Main Exception ",y)
    end  
end

function beaconStatsPBI(localTableDF::DataFrame,fullUrl::ASCIIString,deviceType::ASCIIString;showAdditional::Bool=true,usePageLoad::Bool=true)
    if (usePageLoad)
        dv = localTableDF[:timers_t_done]
    else
        dv = localTableDF[:timers_domready]
    end
    statsDF = limitedStatsFromDV(dv)
    if (showAdditional)
        if (usePageLoad)
            chartTitle = "Page Load Time Stats: $(fullUrl) for $(deviceType)"
        else
            chartTitle = "Page Domain Ready Time Stats: $(fullUrl) for $(deviceType)"
        end
        showLimitedStats(statsDF,chartTitle)
    end
    return statsDF
end

function showAvailableSessionsStreamline(WellKnownHost::Dict,WellKnownPath::Dict,localTableDF::DataFrame,localTableRtDF::DataFrame,pageGroup::ASCIIString,deviceType::ASCIIString,
    rangeLowerMs::Float64,rangeUpperMs::Float64,fullUrl::ASCIIString,localUrl::ASCIIString;
    showCriticalPathOnly::Bool=true,showLines::Int64=10,showDevView::Bool=false,showDebug::Bool=false,usePageLoad::Bool=true)
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

