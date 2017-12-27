using URIParser

function scrubUrlToPrint(SP::ShowParams,urlDF::DataFrame,urlColumn::Symbol)
    try
        i = 0
        todo = 0
        for url in urlDF[:,urlColumn]
            i += 1
            if Bool[ismatch(r"Not Blocking",url)][1]
                deleterows!(urlDF,i)
                continue
            end

            newUrl = "None"
            try
                newUrl = scrubUrlString(SP,url)
            catch
                #if (SP.debugLevel > 8) very noisy
                #    println("str ",url,typeof(url))
                #end
                newUrl = url
            end

            #if (SP.debugLevel > 8)
            #    println("newUrl $newUrl")
            #end

            urlLength = length(newUrl)
            if (urlLength > SP.scrubUrlChars)
                urlDF[i,urlColumn] = newUrl[1:SP.scrubUrlChars] * "..."
            else
                urlDF[i,urlColumn] = newUrl
            end
        end
    catch y
        println("scrubUrlToPrint Exception ",y)
    end

end

function scrubUrlString(SP::ShowParams,url::UTF8String)

    #try

        if Bool[ismatch(r".*/\?utm_source=Facebook.*",url)][1]
            url = map!(x->replace(x,r"utm_source=Facebook.*","utm_source=Facebook"),[url])[1]
        end

        # Remove the non-print "%" from all url strings
        # url = map!(x->replace(x,"%","\045"),[url])[1]

        url = map!(x->replace(x,"%","_"),[url])[1]
        url = map!(x->replace(x,";","_"),[url])[1]
        url = map!(x->replace(x,"#","_"),[url])[1]
        url = map!(x->replace(x,"|","_"),[url])[1]
        url = map!(x->replace(x,"&","_"),[url])[1]
        url = map!(x->replace(x,"~","_"),[url])[1]
        url = map!(x->replace(x,"!","_"),[url])[1]
        url = map!(x->replace(x,"\$","_"),[url])[1]

        #uri = URI(url)
        urlLength = length(url)

    #            if (urlLength > SP.scrubUrlChars)
    #                println("Fixing length ",urlLength," for ",url[1:50])
    #            end

        groupSize = 0
        groupStart = 1
        groupField = ""
        newUrl = ""
        for pos=1:urlLength
            groupSize += 1
            cChar = url[pos]
            groupField = "$groupField$cChar"
            #println("newUrl: ",newUrl," size ",groupSize," pos ",pos)
            if url[pos] == '/'
                if groupSize > SP.scrubUrlSections
                    newUrl = "$newUrl.../"
                else
                    newUrl = "$newUrl$groupField"
                end
                groupSize = 0
                groupStart = pos
                groupField = ""
            end
        end

        if groupSize > SP.scrubUrlSections
            newUrl = "$newUrl..."
        else
            newUrl = "$newUrl$groupField"
        end

        return newUrl
    #catch y
    #    println("scrubUrlString Exception ",y)
    #    println("Exception Extra $url")
    #end
end

function cleanupTopUrlTable(topUrlList::DataVector)
    try
        dv = deepcopy(topUrlList)
        i = 1
        for url in topUrlList
            trim1FrontIdx = searchindex(url,"http:/")
            trim2FrontIdx = searchindex(url,"https:/")
            # Leave the leading slash
            if (trim1FrontIdx > 0)
                testUrl = url[7:end]
            end

            if (trim2FrontIdx > 0)
                testUrl = url[8:end]
            end

            trimEndIdx = searchindex(testUrl,"/?")
            if (trimEndIdx > 0)
                testUrl = testUrl[1:trimEndIdx]
            end

            # Leave the slash
            #if (testUrl[end] == '/')
            #    testUrl = testUrl[1:end-1]
            #end
            #println("i=",i," testUrl=",testUrl)
            dv[i] = testUrl
            i = i+1
        end

        #Debug

        return dv

        catch y
        println("cleanupTopUrlTable Exception",y)
    end
end

function returnMatchingUrlTableV2(TV::TimeVars,UP::UrlParams)
    try

        topUrlDF = query("""\

        select
            count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,
            CASE WHEN (position('?' in params_u) > 0) then (trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) || '/%') else params_u || '%' end as urlgroup
        FROM $(UP.beaconTable)
        where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            page_group ilike '$(UP.pageGroup)' and
            params_u ilike '$(UP.urlRegEx)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs) and
            user_agent_device_type ilike '$(UP.deviceType)' and
            user_agent_os ilike '$(UP.agentOs)' and
            params_rt_quit IS NULL
        group by urlgroup
        order by cnt desc
        limit $(UP.limitRows)
         """);

        return topUrlDF

    catch y
        println("returnTopUrlTableV2 Exception ",y)
    end
end

function returnTopUrlTable(ltName::ASCIIString,pageGroup::ASCIIString,startTimeMs::Int64,endTimeMs::Int64; limit::Int64=20)
    try
        topUrl = query("""\

        select
            count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,params_u as urlgroup
        FROM $(ltName)
        where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            "timestamp" between $startTimeMs and $endTimeMs and
            page_group ilike '$(pageGroup)' and
            timers_t_done >= 1000 and timers_t_done < 600000 and
            params_rt_quit IS NULL
        group by params_u
        order by cnt desc
        limit $(limit)

         """);



        return topUrl

    catch y
        println("returnTopUrlTable Exception ",y)
    end
end


function topUrlTable(TV::TimeVars,UP::UrlParams,SP::ShowParams)


    showCount=true
    showCountDetails=true

    try
        btv = UP.btView

        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup)", chart_info = ["Pages Load Used",TV.timeString],showTimeStamp=false)

        if (showCount)
            topurl = query("""\

            select count(*),
            CASE
            when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
            else trim('/' from params_u)
            end urlgroup
            FROM $(btv)
            where
            beacon_type = 'page view'
            group by urlgroup
            order by count(*) desc
            limit $(UP.limitRows)
            """);

            scrubUrlToPrint(SP,topurl,:urlgroup)
            beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Url - With Grouping After Parameters Dropped")]))
        end

        if (showCountDetails)
            topurl = query("""\

            select count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,params_u as urlgroup
            FROM $(btv)
            where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            timers_t_done > 0
            group by params_u
            order by cnt desc
            limit $(UP.limitRows)
            """);

            scrubUrlToPrint(SP,topurl,:urlgroup)
            beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Avg MB"),Symbol("Avg MS"),Symbol("Url - Individual")]))
        end

    catch y
        println("topUrlTable Exception ",y)
    end

end

function topUrlTableByTime(TV::TimeVars,UP::UrlParams,SP::ShowParams)
      try

        ltName = UP.beaconTable
        displayTitle(chart_title = "Top URL Page Views for $(UP.pageGroup) Dev=$(UP.deviceType), OS=$(UP.agentOs)",
         chart_info = [TV.timeString],showTimeStamp=false)

        topurl = query("""\

        select count(*),
        CASE
        when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
        else trim('/' from params_u)
        end urlgroup
        FROM $(ltName)
        where
          beacon_type = 'page view' and
          "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
          session_id IS NOT NULL and
          params_rt_quit IS NULL and
          params_u ilike '$(UP.urlRegEx)' and
          user_agent_device_type ilike '$(UP.deviceType)' and
          user_agent_os ilike '$(UP.agentOs)' and
          page_group ilike '$(UP.pageGroup)' and
          timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
        group by urlgroup
        order by count(*) desc
        limit $(SP.showLines)
        """);

        scrubUrlToPrint(SP,topurl,:urlgroup)
        beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Url - With Grouping After Parameters Dropped")]))

        topurl = query("""\

        select count(*) cnt, AVG(params_dom_sz), AVG(timers_t_page) ,params_u as urlgroup
        FROM $(ltName)
        where
            beacon_type = 'page view' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            params_dom_sz > 0 and
            timers_t_page > 0 and
            session_id IS NOT NULL and
            params_rt_quit IS NULL and
            params_u ilike '$(UP.urlRegEx)' and
            user_agent_device_type ilike '$(UP.deviceType)' and
            user_agent_os ilike '$(UP.agentOs)' and
            page_group ilike '$(UP.pageGroup)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs)
        group by params_u
        order by cnt desc
        limit $(SP.showLines)
        """);

        scrubUrlToPrint(SP,topurl,:urlgroup)
        beautifyDF(names!(topurl[:,:],[Symbol("Views"),Symbol("Avg MB"),Symbol("Avg MS"),Symbol("Url - Individual")]))
    catch y
        println("topUrlTableByTime Exception ",y)
    end

end

function setRangeUPT(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame)

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

        if (studyTime > 0)
            toppageurl = sessionUrlTableCreateDF(UP,SP,studySession,studyTime)
        elseif (studySession != "None")
            toppageurl = allSessionUrlTableCreateDF(TV,UP,SP,studySession)
        else
            toppageurl = allPageUrlTableCreateDF(TV,UP)
        end;

        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")]);
        return toppageurl
    catch y
        println("findTopPageUrlUPT Exception ",y)
    end
end

function findTopPageViewUPT(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame)
    try
        if SP.reportLevel > 0
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
                    beautifyDF(names!(printDf[:,:],[Symbol("Views"),Symbol("Time (ms)"),Symbol("Url Used")]))
                end
            end
        end
    catch y
        println("findTopPageUrlUPT Exception ",y)
    end
end
