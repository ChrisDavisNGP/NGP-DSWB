type LocalVars
    linesOutput::Int64
    reportLevel::Int64
    rangeLower::Float64
    rangeUpper::Float64
    studySession::ASCIIString
    studyTime::Int64
end

function setRangeUPT(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    
    try
        statsDF = DataFrame()
        dv = localTableDF[:timers_t_done]
        statsDF = basicStatsFromDV(dv)

        displayTitle(chart_title = "Raw Data Stats for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        LV.rangeLower = statsDF[1:1,:LowerBy3Stddev][1]
        LV.rangeUpper = statsDF[1:1,:UpperBy3Stddev][1]
    catch y
        println("setupStats Exception ",y)
    end  

end

function findTopPageUrlUPT(TV::TimeVars,UP::UrlParams,LV::LocalVars)

    try
        toppageurl = DataFrame()
        tableRt = UP.resourceTable
        
        if (LV.studyTime > 0)
            toppageurl = sessionUrlTableDF(tableRt,LV.studySession,LV.studyTime)
        elseif (LV.studySession != "None")
            toppageurl = allSessionUrlTableDF(tableRt,LV.studySession,TV.startTimeMsUTC,TV.endTimeMsUTC)
        else
            toppageurl = allPageUrlTableDF(tableRt,UP.pageGroup,UP.urlRegEx,LV.rangeLower,LV.rangeUpper,TV.startTimeMsUTC,TV.endTimeMsUTC)
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

function findTopPageViewUPT(TV::TimeVars,UP::UrlParams,LV::LocalVars)
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
