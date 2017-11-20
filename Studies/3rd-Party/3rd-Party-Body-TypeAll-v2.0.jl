function typeAllBody(
    table::ASCIIString,startTimeMs::Int64,endTimeMs::Int64,timeString::ASCIIString,productPageGroup::ASCIIString,localUrl::ASCIIString,deviceType::ASCIIString
    )
    try
        # Is there data?
        localTableDF = estimateBeacons(TV,UP,SP)
        println("$table count is ",size(localTableDF))        
        
        # Stats on the data
        statsDF = DataFrame()
        dv = localTableDF[:timers_t_done]
        statsDF = basicStatsFromDV(dv)

        displayTitle(chart_title = "Beacon Data Stats for $(productPageGroup)", chart_info = [timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
        
        rangeLower = statsDF[1:1,:q25][1]
        rangeUpper = statsDF[1:1,:q75][1]

        studyTime = 0
        studySession = "None"

        toppageurl = DataFrame()
        if studyTime > 0
            toppageurl = sessionUrlTableDF(tableRt,studySession,studyTime)
            elseif (studySession != "None")
            toppageurl = allSessionUrlTableDF(tableRt,studySession,startTimeMs,endTimeMs)
            else
                toppageurl = allPageUrlTableDF(tableRt,productPageGroup,localUrl,rangeLower,rangeUpper,startTimeMs,endTimeMs,deviceType=deviceType)
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

        displayTitle(chart_title = "RT Data Stats for $(productPageGroup)", chart_info = [timeString],showTimeStamp=false)
        beautifyDF(summaryStatsDF[:,:])

        scrubUrlToPrint(toppageurl);
        classifyUrl(toppageurl);        

        summaryPageGroup = summarizePageGroups(toppageurl)
        beautifyDF(summaryPageGroup[1:min(end,10),:])        
        
        # This is the non-Url specific report so get the summary table and overwrite toppageurl
        toppageurl = deepcopy(summaryPageGroup);        
        
        itemCountTreemap(toppageurl,showTable=true)      
        endToEndTreemap(toppageurl,showTable=true,limit=100)        
        blockingTreemap(toppageurl,showTable=true)        
        requestTreemap(toppageurl,showTable=true)
        responseTreemap(toppageurl,showTable=true)
        dnsTreemap(toppageurl,showTable=true)
        tcpTreemap(toppageurl,showTable=true)
        redirectTreemap(toppageurl,showTable=true)
    catch y
        println("typeAll Exception ",y)
    end  

end

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

