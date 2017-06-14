function requestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    rc = query("""\

        select 
            count(*) reqcnt, substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        group by urlgroup
        order by reqcnt desc
        LIMIT 15
    """)

    linesOut = 15
    displayTitle(chart_title = "$(typeStr): Request Counts By URL Group", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(rc[1:min(linesOut,end),:])    
    
end

function blockingRequestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    br = query("""\

        select 
            count(*) reqcnt, sum(request_start-start_time) totalblk, (sum(request_start-start_time)/count(*)) avgblk,substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (request_start-start_time) > 0
        group by urlgroup
        order by totalblk desc
         LIMIT 30
    """)

    displayTitle(chart_title = "$(typeStr): Blocking Requests By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    linesOut = 30
    beautifyDF(br[1:min(linesOut,end),:])    
end

function nonCacheRequestCountByGroupSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    nc = query("""\    
        select 
            count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where
            (response_last_byte-response_first_byte) > 0
        group by urlgroup
        order by count(*) desc
        LIMIT 15
    """)

    displayTitle(chart_title = "$(typeStr): Non Cache Requests Total By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    linesOut = 15
    beautifyDF(nc[1:min(linesOut,end),:])        
end

function cacheHitRatioSDMRS(TV::TimeVars,UP::UrlParams,typeStr::ASCIIString)

    cached = query("""\
        select 
            count(*), substring(url for position('/' in substring(url from 9)) +7) urlgroup
        FROM $(UP.rtView)
        where 
            (response_last_byte-response_first_byte) = 0 
        group by urlgroup 
        order by count(*) desc 
        LIMIT 250
    """)

    ratio = query("""\
        select 
            substring(url for position('/' in substring(url from 9)) +7) urlgroup, count(*) notCachedCount, 0 cachedCount, 0.0 ratio
        FROM $(UP.rtView) 
        where 
            (response_last_byte-response_first_byte) > 0 
        group by urlgroup 
        order by count(*) desc 
        LIMIT 250
    """)

    for x in eachrow(ratio)
        cnt = cached[Bool[isequal(x[:urlgroup],y) for y in cached[:urlgroup]],:count]
        if isempty(cnt)
            cnt = [1]
        end
        x[:cachedcount] = cnt[1]
        x[:ratio] = (cnt[1] / (x[:notcachedcount] + cnt[1])) * 100.0
    end

    displayTitle(chart_title = "$(typeStr): Cache Hit Ratio By URL Groups Across All Sessions", chart_info = [TV.timeString], showTimeStamp=false)
    beautifyDF(names!(ratio[1:min(30, end),[1:4;]],[symbol("Url Group"), symbol("Not Cached Cnt"), symbol("Cached Cnt"), symbol("Cached Ratio")]))
end
