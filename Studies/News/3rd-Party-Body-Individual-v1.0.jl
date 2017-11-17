function beaconStatsPBI(localTableDF::DataFrame;showAdditional::Bool=true)
    dv = localTableDF[:timers_t_done]
    statsDF = basicStatsFromDV(dv)

    displayTitle(chart_title = "Beacon Data Stats for $(productPageGroup)", chart_info = [tv.timeString],showTimeStamp=false)
    if showAddtional
        beautifyDF(statsDF[:,:])
    end
    
    return statsDF
end

function individualPageReport(studySession::ASCIIString,studyTime::Int64;showCriticalPathOnly::Bool=false)
    try

        if studyTime > 0 && reportLevel > 0 
            waterFallFinder(table,studySession,studyTime,tv.startTimeMsUTC,tv.endTimeMsUTC)
        end        

        toppageurl = DataFrame()

        if studyTime > 0
            toppageurl = sessionUrlTableDF(tableRt,studySession,studyTime)
            elseif (studySession != "None")
            toppageurl = allSessionUrlTableDF(tableRt,studySession,tv.startTimeMsUTC,tv.endTimeMsUTC)
        else
            toppageurl = allPageUrlTableDF(tableRt,productPageGroup,localUrl,rangeLower,rangeUpper,tv.startTimeMsUTC,tv.endTimeMsUTC,deviceType=deviceType)
            end;

        toppageurl = names!(toppageurl[:,:],
        [symbol("urlpagegroup"),symbol("Start"),symbol("Total"),symbol("Redirect"),symbol("Blocking"),symbol("DNS"),
            symbol("TCP"),symbol("Request"),symbol("Response"),symbol("Gap"),symbol("Critical"),symbol("urlgroup"),
            symbol("request_count"),symbol("label"),symbol("load_time"),symbol("beacon_time")]);
        
        toppageurlbackup = deepcopy(toppageurl);        
        toppageurl = deepcopy(toppageurlbackup)
        if studyTime > 0 && reportLevel > 0 && !showCriticalPathOnly
            beautifyDF(toppageurl)
        end

        removeNegitiveTime(toppageurl,:Total)
        removeNegitiveTime(toppageurl,:Redirect)
        removeNegitiveTime(toppageurl,:Blocking)
        removeNegitiveTime(toppageurl,:DNS)
        removeNegitiveTime(toppageurl,:TCP)
        removeNegitiveTime(toppageurl,:Request)
        removeNegitiveTime(toppageurl,:Response)

        scrubUrlToPrint(toppageurl);
        classifyUrl(toppageurl);        

        toppageurl = gapAndCriticalPath(toppageurl);        
        
        if studyTime > 0 && reportLevel > 0 && !showCriticalPathOnly
            beautifyDF(toppageurl)
        end        

        
        criticalPathTreemap(toppageurl;showTable=true,limit=12)
        if (!showCriticalPathOnly)
            gapTreemap(toppageurl;showTable=true,limit=12)
            #itemCountTreemap(toppageurl,showTable=true)      All entries are 1
            endToEndTreemap(toppageurl,showTable=true)        
            blockingTreemap(toppageurl,showTable=true)        
            requestTreemap(toppageurl,showTable=true)
            responseTreemap(toppageurl,showTable=true)
            dnsTreemap(toppageurl,showTable=true)
            tcpTreemap(toppageurl,showTable=true)
            redirectTreemap(toppageurl,showTable=true)            
        end
        
    catch y
        println("studySession Exception ",y)
    end  
end


        