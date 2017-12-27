using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")
include("../../../Lib/URL-Classification-Package-v2.0.jl")

TV = pickTime()
#TV = timeVariables(2017,6,8,10,59,2017,6,8,12,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

#studySession::ASCIIString
#studyTime::Int64

localTableDF = defaultBeaconsToDF(TV,UP,SP)
println("$table count is ",size(localTableDF))

localTableRtDF = treemapsLocalTableRtCreateDF(TV,UP,SP)
println("$tableRt count is ",size(localTableRtDF))

setRangeUPT(TV,UP,SP)

showAvailableSessions(TV,UP,SP,localTableDF,localTableRtDF)

# Individual pages uses the numbers above make the best tree maps

# Test 1
studySession = "07c49d94-2cb4-4af8-8115-9a245e3b317e-p032id"
studyTime =  1511797648155;

# Group pages work but obscure gaps and critical path
# Test 2
#studySession = "060212ca-9fdb-4b55-9aa9-b2ff9f6c5032-odv5lh"
#studyTime = 0;

# Test 3

#studySession = "None"
#studyTime = 0;

if studyTime > 0 && SP.reportLevel > 0
    waterFallFinder(TV,UP,SP,studySession,studyTime)
end

toppageurl = findTopPageUrlUPT(TV,UP,SP,studySession,studyTime)

findTopPageViewUPT(TV,UP,SP)

removeNegitiveTime(toppageurl,:Total)
removeNegitiveTime(toppageurl,:Redirect)
removeNegitiveTime(toppageurl,:Blocking)
removeNegitiveTime(toppageurl,:DNS)
removeNegitiveTime(toppageurl,:TCP)
removeNegitiveTime(toppageurl,:Request)
removeNegitiveTime(toppageurl,:Response)

#display(toppageurl[Bool[x < 0 for x in negDf[:blocking]],:])

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP.debug);
WellKnownPath = wellKnownPathDictionary();
scrubUrlToPrint(SP,toppageurl,:url);
classifyUrl(SP,toppageurl);

#beautifyDF(toppageurl[:,:])

toppageurl = gapAndCriticalPath(toppageurl);

#beautifyDF(toppageurl[:,:])

criticalPathTreemapV2(TV,UP,SP,UP.urlFull,toppageurl)

# Gap Graph

toppageurl = names!(toppageurl[:,:],
[Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
    Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("beacons"),Symbol("Critical"),Symbol("urlgroup"),
    Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Critical Path (Not Gap)"
toppageurl[toppageurl[:,12] .== "Not Blocking",9] = 0
toppageurl[toppageurl[:,12] .== "Not Blocking",10] = sum(toppageurl[:,:Critical])

treeDf = deepcopy(toppageurl)
delete!(treeDf,:Start)
delete!(treeDf,:Total)
delete!(treeDf,:Redirect)
delete!(treeDf,:Blocking)
delete!(treeDf,:DNS)
delete!(treeDf,:TCP)
delete!(treeDf,:Request)
delete!(treeDf,:Response)
delete!(treeDf,:Critical)
delete!(treeDf,:urlgroup)
delete!(treeDf,:request_count)
delete!(treeDf,:beacon_time)

fieldNames = [:urlpagegroup]
treeDf[:label] = "Gap"
drawTree(treeDf; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

if SP.reportLevel > 1
    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        totalPercentTime = sum(list[:,:Gap]) * 0.010
        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:Blocking)
        delete!(list,:DNS)
        delete!(list,:TCP)
        delete!(list,:Request)
        delete!(list,:Response)
        delete!(list,:Critical)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:Gap,rev=true)])
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        displayTitle(chart_title = "Top Times By Gap Time (ms)",showTimeStamp=false)
        totalPercentTime = list[1:1,:Gap] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:Gap]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Gap Time"),Symbol("Url Without Params")]))
    end
end

# End to End Time Display

toppageurl = names!(toppageurl[:,:],
[Symbol("urlpagegroup"),Symbol("Start"),Symbol("beacons"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
    Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
    Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

treeDf = deepcopy(toppageurl)
delete!(treeDf,:request_count)
delete!(treeDf,:Start)
delete!(treeDf,:Gap)
delete!(treeDf,:Critical)
delete!(treeDf,:Redirect)
delete!(treeDf,:Blocking)
delete!(treeDf,:DNS)
delete!(treeDf,:TCP)
delete!(treeDf,:Request)
delete!(treeDf,:Response)
delete!(treeDf,:urlgroup)
delete!(treeDf,:beacon_time)

fieldNames = [:urlpagegroup]
treeDf[:label] = "End to End Time"
drawTree(treeDf; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

if SP.reportLevel > 1
    list = deepcopy(toppageurl)

    list = names!(list,[Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
        Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalPercentTime = sum(list[:,:Total]) * 0.10

    delete!(list,:label)
    delete!(list,:request_count)
    delete!(list,:load_time)
    delete!(list,:gap)
    delete!(list,:critical)
    delete!(list,:Start)
    delete!(list,:Redirect)
    delete!(list,:Blocking)
    delete!(list,:DNS)
    delete!(list,:TCP)
    delete!(list,:Request)
    delete!(list,:Response)
    delete!(list,:beacon_time)

    sort!(list,cols=[order(:Total,rev=true)])

    titlestring = "This includes time which is overlapped."
    title2string = "Note: beacons field is used for load time and load_time field is used fractional load time"
    displayTitle(chart_title = "Total Time (K ms) For All Pages In Sample", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

    totalPercentTime = list[1:1,:Total] * 0.1
    list = list[Bool[x > totalPercentTime[1] for x in list[:Total]],:]
    beautifyDF(names!(list[1:min(15,end),:],
        [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Url Without Params")]))
end

if (SP.reportLevel > 2)

    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("beacons"),Symbol("DNS"),
        Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalTime = sum(toppageurl[:,:Total])
    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        titlestring = "This includes time which is overlapped but does not include the gaps."
        title2string = "Note: beacons field is used for load time"
        displayTitle(chart_title = "Total Time (K ms) For Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not Blocking"
        toppageurl[toppageurl[:,12] .== "Not Blocking",4] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",5] = missingTime
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "Blocking Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:DNS)
        delete!(list,:TCP)
        delete!(list,:Request)
        delete!(list,:Response)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:Blocking,rev=true)]);
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end;

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        totalPercentTime = list[1:1,:Blocking] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:Blocking]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Blocking Time"),Symbol("Url Without Params")]))
    else
        println("No Blocking time.  Output nothing in report")
    end
end

if (SP.reportLevel > 2)

    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
        Symbol("TCP"),Symbol("beacons"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalTime = sum(toppageurl[:,:Total])
    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        titlestring = "This includes time which is overlapped but does not include the gaps."
        title2string = "Note: beacons field is used for load time"
        displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not Request Time"
        toppageurl[toppageurl[:,12] .== "Not Blocking",7] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",8] = missingTime
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "Request Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        totalPercentTime = sum(list[:,:Request]) * 0.01
        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:Blocking)
        delete!(list,:DNS)
        delete!(list,:TCP)
        delete!(list,:Response)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:Request,rev=true)])
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        totalPercentTime = list[1:1,:Request] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:Request]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Request Time"),Symbol("Url Without Params")]))
    else
        println("No Request time.  Output nothing in report")
    end
end

if (SP.reportLevel > 2)
    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
        Symbol("TCP"),Symbol("Request"),Symbol("beacons"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalTime = sum(toppageurl[:,:Total])
    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        titlestring = "This includes time which is overlapped but does not include the gaps."
        title2string = "Note: beacons field is used for load time"
        displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not Response Time"
        toppageurl[toppageurl[:,12] .== "Not Blocking",8] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",9] = missingTime
        #display(toppageurl[toppageurl[:,12] .== "Not Blocking",:])
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "Response Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)


        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        totalPercentTime = sum(list[:,:Response]) * 0.01
        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:Blocking)
        delete!(list,:DNS)
        delete!(list,:TCP)
        delete!(list,:Request)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:Response,rev=true)])
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        totalPercentTime = list[1:1,:Response] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:Response]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Response Time"),Symbol("Url Without Params")]))
    else
        println("No Response time.  Output nothing in report")
    end
end

if (SP.reportLevel > 2)

    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("beacons"),
        Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        totalTime = sum(toppageurl[:,:Total])
        titlestring = "This includes time which is overlapped but does not include the gaps."
        title2string = "Note: beacons field is used for load time and load_time field is used for count of requests"
        displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not DNS"
        toppageurl[toppageurl[:,12] .== "Not Blocking",5] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",6] = missingTime
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "DNS Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        totalPercentTime = sum(list[:,:Total]) * 0.01
        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:Blocking)
        delete!(list,:TCP)
        delete!(list,:Request)
        delete!(list,:Response)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:DNS,rev=true)])
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        totalPercentTime = list[1:1,:DNS] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:DNS]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("DNS Time"),Symbol("Url Without Params")]))
    else
        println("No DNS time.  Output nothing in report")
    end
end

if (SP.reportLevel > 2)
    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
        Symbol("beacons"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalTime = sum(toppageurl[:,:Total])
    currentTime = sum(toppageurl[:,:beacons])
    if currentTime > 0
        titlestring = "This includes time which is overlapped but does not include the gaps."
        title2string = "Note: beacons field is used for load time"
        displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not TCP"
        toppageurl[toppageurl[:,12] .== "Not Blocking",6] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",7] = missingTime
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "TCP Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        totalPercentTime = sum(list[:,:TCP]) * 0.10
        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Redirect)
        delete!(list,:Blocking)
        delete!(list,:DNS)
        delete!(list,:Request)
        delete!(list,:Response)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:TCP,rev=true)])
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        totalPercentTime = list[1:1,:TCP] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:TCP]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("TCP Time"),Symbol("Url Without Params")]))
    else
        println("No TCP time.  Output nothing in report")
    end
end

if (SP.reportLevel > 2)

    toppageurl = names!(toppageurl[:,:],
    [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("beacons"),Symbol("Blocking"),Symbol("DNS"),
        Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
        Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

    totalTime = sum(toppageurl[:,:Total])
    currentTime = sum(toppageurl[:,:beacons])

    if currentTime > 0
        missingTime = totalTime - currentTime
        toppageurl[toppageurl[:,12] .== "Not Blocking",1] = "Not Redirecting"
        toppageurl[toppageurl[:,12] .== "Not Blocking",3] = 0
        toppageurl[toppageurl[:,12] .== "Not Blocking",4] = missingTime
        fieldNames = [:urlpagegroup]
        toppageurl[:label] = "Redirect Time"
        drawTree(toppageurl; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

        list = deepcopy(toppageurl)

        list = names!(list,
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("gap"),Symbol("critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        delete!(list,:label)
        delete!(list,:request_count)
        delete!(list,:load_time)
        delete!(list,:gap)
        delete!(list,:critical)
        delete!(list,:Start)
        delete!(list,:Blocking)
        delete!(list,:DNS)
        delete!(list,:TCP)
        delete!(list,:Request)
        delete!(list,:Response)
        delete!(list,:beacon_time)

        sort!(list,cols=[order(:Redirect,rev=true)]);
        if Bool[ismatch(r"Not Blocking",x) for x in list[:urlgroup]][1]
            deleterows!(list,1)
        end

        totalPercentTime = sum(list[:,:Redirect]) * 0.01
        displayTitle(chart_title = "Top Times (ms)",showTimeStamp=false)

        map!(x->replace(x,"%","\%"),list[:,:urlgroup])
        totalPercentTime = list[1:1,:Redirect] * 0.1
        list = list[Bool[x > totalPercentTime[1] for x in list[:Redirect]],:]
        beautifyDF(names!(list[1:min(15,end),:],
            [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Redirect Time"),Symbol("Url Without Params")]))
    else
        println("No redirect time.  Output nothing in report")
    end
end

if (SP.reportLevel > 11)
    if studyTime > 0
        topurl = query("""\
        select substring(url for position('/' in substring(url from 9)) +7) urlgroup,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
        count(*) as request_count
        FROM $(tableRt)
        where session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
        group by urlgroup
        """);
    elseif (studySession != "None")
        topurl = query("""\
        select substring(url for position('/' in substring(url from 9)) +7) urlgroup,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
        count(*) as request_count
        FROM $(localTableRt)
        where session_id = '$(studySession)'
        group by urlgroup
        """);
    else
        topurl = query("""\
        select substring(url for position('/' in substring(url from 9)) +7) urlgroup,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
        count(*) as request_count
        FROM $(localTableRt)
        group by urlgroup
        """);
    end

    #displayTitle(chart_title = "Top URL Page Views for $(productPageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
    #topurl = names!(topurl[:,:],[Symbol("beacons"),Symbol("urlgroup"),Symbol("load_time"),Symbol("start_time"),Symbol("redirect"),Symbol("blocking"),Symbol("dns"),Symbol("tcp"),Symbol("request"),Symbol("response")])
    topurl = names!(topurl[:,:],[Symbol("urlgroup"),Symbol("load_time_int"),Symbol("beacons"),Symbol("request_count")]);

    # Note: this cell turns the :urlgroup from a URL to a string.  Run cell above each time before this cell

    topurl[:load_time] = 0.0
    i = 0
    for url in topurl[:,:urlgroup]
        i += 1
        #@show url
        uri = URI(url)
        #@show uri,uri.path,uri.scheme,uri.host
        #println("")

        topurl[i:i,:urlgroup] = uri.host
        topurl[i:i,:load_time] = (topurl[i:i,:load_time_int]) / 1000.0

    end

    titlestring = "This includes time which is overlapped but does not include the gaps."
    title2string = "Note: beacons field is used for load time and load_time field is used for count of requests"
    displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

    fieldNames = [:urlgroup]
    topurl[:x1] = "Grouped By Host"
    drawTree(topurl; titleCol = :x1, fieldNames = fieldNames,resourceColors=true)
end

if (reportLevel > 11)
    if (studyTime > 0)
        topdetailurl = query("""\
        select
        CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
        CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END as load_time,
        CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END as beacons,
        1 as request_count
        FROM $(tableRt) where session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
        """);
    elseif (studySession != "None")
        topdetailurl = query("""\
        select
        CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
        count(*) as request_count
        FROM $(localTableRt) where session_id = '$(studySession)'
        group by urlgroup
        """);
    else
        topdetailurl = query("""\
        select
        CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
        avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
        count(*) as request_count
        FROM $(localTableRt)
        group by urlgroup
        """);
    end

    #topdetailurl = names!(topurl[:,:],[Symbol("beacons"),Symbol("urlgroup"),Symbol("load_time"),Symbol("start_time"),Symbol("redirect"),Symbol("blocking"),Symbol("dns"),Symbol("tcp"),Symbol("request"),Symbol("response")])
    topdetailurl = names!(topdetailurl[:,:],[Symbol("urlgroup"),Symbol("load_time_int"),Symbol("beacons"),Symbol("request_count")]);

    # Note: this cell turns the :urlgroup from a URL to a string.  Run cell above each time before this cell

    topdetailurl[:load_time] = 0.0

    i = 0
    for url in topdetailurl[:,:urlgroup]
        i += 1
        #@show url
        uri = URI(url)
        newuristring = uri.host * uri.path
        #println("$(newuristring)")
        topdetailurl[i:i,:urlgroup] = newuristring
        topdetailurl[i:i,:load_time] = (topdetailurl[i:i,:load_time_int]) / 1000.0
    end

    treeDf = deepcopy(topdetailurl)
    delete!(treeDf,:load_time_int)
    delete!(treeDf,:request_count)
    #display(treeDf[1:10,:])

    titlestring = "This includes time which is overlapped but does not include the gaps."
    title2string = "Note: beacons field is used for load time and load_time field is used for count of requests"
    displayTitle(chart_title = "Total Time (K ms) For Single Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

    fieldNames = [:urlgroup]
    treeDf[:label] = "All URLs"
    drawTree(treeDf; titleCol = :label, fieldNames = fieldNames,resourceColors=true)

end

q = query(""" drop view if exists $(UP.btView);""")
q = query(""" drop view if exists $(UP.rtView);""")
;
