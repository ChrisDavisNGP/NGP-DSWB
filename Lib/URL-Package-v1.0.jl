using URIParser

function scrubUrlToPrint(urlDF::DataFrame;limit::Int64=120)
    try
    i = 0
    todo = 0
    for url in urlDF[:,:urlgroup]
        i += 1
        if Bool[ismatch(r"Not Blocking",url)][1]
            deleterows!(urlDF,i)
            continue
        end

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
        if (urlLength > limit)
            urlDF[i,:urlgroup] = url[1:limit] * "..."
        else
            urlDF[i,:urlgroup] = url
        end
    end
    catch y
        println("scrubUrlToPrint Exception ",y)
    end

end

function scrubUrlFieldToPrint(urlDF::DataFrame,urlField::Symbol;limit::Int64=120;)
    try
    i = 0
    todo = 0
        for url in urlDF[:,urlField]
        i += 1
        if Bool[ismatch(r"Not Blocking",url)][1]
            deleterows!(urlDF,i)
            continue
        end

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
        if (urlLength > limit)
            urlDF[i,urlField] = url[1:limit] * "..."
        else
            urlDF[i,urlField] = url
        end
    end
    catch y
        println("scrubUrlFieldToPrint Exception ",y)
    end

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

function returnMatchingUrlTableV2(UP::UrlParams,startTimeMs::Int64,endTimeMs::Int64)
    try
#            CASE WHEN (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) else trim('/' from params_u) end as urlgroup
        topUrl = query("""\

        select
            count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,
            CASE WHEN (position('?' in params_u) > 0) then (trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) || '/%') else params_u || '%' end as urlgroup
        FROM $(UP.beaconTable)
        where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            "timestamp" between $startTimeMs and $endTimeMs and
            page_group ilike '$(UP.pageGroup)' and
            params_u ilike '$(UP.urlRegEx)' and
            timers_t_done >= $(UP.timeLowerMs) and timers_t_done < $(UP.timeUpperMs) and
            user_agent_device_type ilike '$(UP.deviceType)' and
            params_rt_quit IS NULL
        group by urlgroup
        order by cnt desc
        limit $(UP.limitRows)
         """);

#                    user_agent_device_type ilike '$(UP.deviceType)' and

        return topUrl

    catch y
        println("returnTopUrlTable Exception ",y)
    end
end

function returnMatchingUrlTable(ltName::ASCIIString,pageGroup::ASCIIString,startTimeMs::Int64,endTimeMs::Int64; lowerLimitMs::Float64=1000.0, upperLimitMs::Float64=600000.0, limit::Int64=20)
    try
        topUrl = query("""\

        select
            count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,
            CASE WHEN (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7))) else trim('/' from params_u) end as urlgroup
        FROM $(ltName)
        where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            "timestamp" between $startTimeMs and $endTimeMs and
            page_group ilike '$(pageGroup)' and
        timers_t_done >= $(lowerLimitMs) and timers_t_done < $(upperLimitMs) and
            params_rt_quit IS NULL
        group by urlgroup
        order by cnt desc
        limit $(limit)

         """);



        return topUrl

    catch y
        println("returnTopUrlTable Exception ",y)
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


function topUrlTable(ltName::ASCIIString, pageGroup::ASCIIString,timeString::ASCIIString; limit::Int64=20, showCount::Bool=true, showCountDetails::Bool=true)
    try
        displayTitle(chart_title = "Top URL Page Views for $(pageGroup)", chart_info = ["Pages Load Used",timeString],showTimeStamp=false)

        if (showCount)
            topurl = query("""\

            select count(*),
            CASE
            when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
            else trim('/' from params_u)
            end urlgroup
            FROM $(ltName)
            where
            beacon_type = 'page view'
            group by urlgroup
            order by count(*) desc
            limit $(limit)
            """);

            scrubUrlToPrint(topurl)
            beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))
        end

        if (showCountDetails)
            topurl = query("""\

            select count(*) cnt, AVG(params_dom_sz), AVG(timers_t_done) ,params_u as urlgroup
            FROM $(ltName)
            where
            beacon_type = 'page view' and
            params_dom_sz > 0 and
            timers_t_done > 0
            group by params_u
            order by cnt desc
            limit $(limit)
            """);

            scrubUrlToPrint(topurl)
            beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Avg MB"),symbol("Avg MS"),symbol("Url - Individual")]))
        end

    catch y
        println("topUrlTable Exception ",y)
    end

end

function topUrlTableByTime(ltName::ASCIIString, pageGroup::ASCIIString,timeString::ASCIIString, startTimeMs::Int64, endTimeMs::Int64; limit::Int64=20)
    try
        displayTitle(chart_title = "Top URL Page Views for $(pageGroup)", chart_info = [timeString],showTimeStamp=false)

        topurl = query("""\

        select count(*),
        CASE
        when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
        else trim('/' from params_u)
        end urlgroup
        FROM $(ltName)
        where
        beacon_type = 'page view' and
        "timestamp" between $startTimeMs and $endTimeMs
        group by urlgroup
        order by count(*) desc
        limit $(limit)
        """);

        scrubUrlToPrint(topurl)
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))

        topurl = query("""\

        select count(*) cnt, AVG(params_dom_sz), AVG(timers_t_page) ,params_u as urlgroup
        FROM $(ltName)
        where
        beacon_type = 'page view' and
        "timestamp" between $startTimeMs and $endTimeMs and
        params_dom_sz > 0 and
        timers_t_page > 0
        group by params_u
        order by cnt desc
        limit $(limit)
        """);

        scrubUrlToPrint(topurl)
        beautifyDF(names!(topurl[:,:],[symbol("Views"),symbol("Avg MB"),symbol("Avg MS"),symbol("Url - Individual")]))
    catch y
        println("topUrlTableByTime Exception ",y)
    end

end
