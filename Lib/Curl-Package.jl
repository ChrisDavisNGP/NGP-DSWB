function debugPrintCurlCommand(SP::ShowParams,curlStr::ASCIIString,sqlStr::ASCIIString)

    println("sql=",sqlStr)

    if SP.debugLevel > 6
        println(curlStr)
    end

    if SP.debugLevel > 4
        sqlStr = replace(sqlStr,"%20"," ")
        sqlStr = replace(sqlStr,"%27","'")
        sqlStr = replace(sqlStr,"%2C",",")
        sqlStr = replace(sqlStr,"%3A",":")
        sqlStr = replace(sqlStr,"%3D","=")
        println(sqlStr)
    end

end

function curlSelectByMonitorOnPageLoad(TV::TimeVars,SP::ShowParams,CU::CurlParams,monitor::UTF8String)

# Select all monitors and call the following

#SELECT timestamp,checkId,monitorName,onPageLoad FROM SyntheticRequest
#where pageref = 'page_0' and externalResource is false and responseStatus = 'OK'
#and port = 443 and contentType = 'text/html' and verb = 'GET' and
#monitorName = 'JTP-Gallery-Beach' since 1 day ago limit 1000

    if SP.debugLevel > 8
        println("Started Page Loaded ")
    end

    monitor = replace(monitor," ","%20")

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    startTimeNR = replace(TV.startTimeStr," ","%20")
    startTimeNR = replace(startTimeNR,":","%3A")
    endTimeNR = replace(TV.endTimeStr," ","%20")
    endTimeNR = replace(endTimeNR,":","%3A")

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand = "SELECT%20timestamp%2CcheckId%2CmonitorName%2ConPageLoad%20FROM%20SyntheticRequest%20" *
        "where%20pageref%20%3D%20%27page_0%27%20and%20externalResource%20is%20false%20and%20" *
        "responseStatus%20%3D%20%27OK%27%20and%20port%20%3D%20443%20and%20contentType%20%3D%20" *
        "%27text%2Fhtml%27%20and%20verb%20%3D%20%27GET%27%20and%20" *
        "monitorName%20%3D%20%27" * monitor * "%27%20" *
        "since%20%27" * startTimeNR * "%27%20until%20%27" * endTimeNR * "%27%20with%20TIMEZONE%20%27America%2FNew_York%27" *
        "%20limit%201000"
#        since%20" * day * "%20day%20ago


    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    return jsonString

end

function curlSelectActiveSyntheticMonitors(SP::ShowParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Started curlSelectActiveSyntheticMonitors ")
    end

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    #SELECT uniques(monitorName) FROM SyntheticCheck where type in ('BROWSER','SCRIPT_BROWSER') SINCE 1 day AGO

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand =  "SELECT%20uniques(monitorName)%20FROM%20SyntheticCheck%20" *
        "where%20type%20in%20(%27BROWSER%27%2C%27SCRIPT_BROWSER%27)%20SINCE%201%20day%20AGO"

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    return jsonString

end



function curlSelectDurationAndSize(SP::ShowParams,CU::CurlParams,startTimeNR::ASCIIString,endTimeNR::ASCIIString)

    if SP.debugLevel > 8
        println("Time Range ",TV.timeString)
    end

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    apiKey = "X-Query-Key:" * CU.apiQueryKey
    compareWith = "2%20days%20ago"

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand =
        "SELECT%20stddev(totalResponseBodySize)%2Caverage(totalResponseBodySize)%2Cstddev(duration)%2Caverage(duration)%20" *
        "FROM%20SyntheticCheck%20facet%20monitorName%20since%20%27" * startTimeNR * "%27%20until%20%27" * endTimeNR *
        "%27%20with%20TIMEZONE%20%27America%2FNew_York%27%20limit%20500%20COMPARE%20WITH%20" * compareWith

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    return jsonString

end

function curlSelectAllByTimeAndUrl(TV::TimeVars,SP::ShowParams,CU::CurlParams,startTimeNR::ASCIIString,endTimeNR::ASCIIString,monitor::ASCIIString)

    if SP.debugLevel > 8
        println("Time Range ",TV.timeString)
    end

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    #Grab the UP urlRegEx and convert it

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand = "SELECT%20*%20FROM%20SyntheticRequest%20SINCE%20%27" * startTimeNR *
        "%27%20UNTIL%20%27" * endTimeNR * "%27%20WHERE%20monitorName%20%3D%20%27" * monitor * "%27%20" *
        "and%20URL%20like%20%27" * CU.urlRegEx * "%27%20" *
        "with%20timezone%20%27America%2FNew_York%27"

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

function curlCritAggLimitedBeaconsToDFNR(TV::TimeVars,SP::ShowParams,CU::CurlParams)

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    startTimeNR = replace(TV.startTimeStr," ","%20")
    startTimeNR = replace(startTimeNR,":","%3A")
    endTimeNR = replace(TV.endTimeStr," ","%20")
    endTimeNR = replace(endTimeNR,":","%3A")

    #Grab the UP urlRegEx and convert it

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand =
        "SELECT%20jobId%2Ctimestamp%2ConPageLoad%2ConPageContentLoad%20FROM%20SyntheticRequest%20SINCE%20%27" *
        startTimeNR * "%27%20UNTIL%20%27" * endTimeNR * "%27%20WHERE%20monitorName%20%3D%20%27" * CU.syntheticMonitor * "%27%20" *
        "and%20URL%20like%20%27" * CU.urlRegEx * "%27%20" *
        "with%20timezone%20%27America%2FNew_York%27%20limit%201000"

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    println("curlStr=",curlStr)

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

function curlCritAggStudySessionToDFNR(TV::TimeVars,SP::ShowParams,CU::CurlParams,studySession::ASCIIString,studyTime::Int64)

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    startTimeNR = replace(TV.startTimeStr," ","%20")
    startTimeNR = replace(startTimeNR,":","%3A")
    endTimeNR = replace(TV.endTimeStr," ","%20")
    endTimeNR = replace(endTimeNR,":","%3A")

    #Grab the UP urlRegEx and convert it

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand =
        "SELECT%20*%20FROM%20SyntheticRequest%20SINCE%20%27" *
        startTimeNR * "%27%20UNTIL%20%27" * endTimeNR * "%27%20WHERE%20monitorName%20%3D%20%27" * CU.syntheticMonitor * "%27%20" *
        "and%20jobId%20%3D%20%27" * studySession * "%27%20" *
        "with%20timezone%20%27America%2FNew_York%27%20limit%201000"

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

function curlSelectAllByTime(TV::TimeVars,SP::ShowParams,CU::CurlParams,startTimeNR::ASCIIString,endTimeNR::ASCIIString,monitor::ASCIIString)

    if SP.debugLevel > 8
        println("Time Range ",TV.timeString)
    end

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    #Grab the UP urlRegEx and convert it

    apiKey = "X-Query-Key:" * CU.apiQueryKey

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="

    sqlCommand = "SELECT%20*%20FROM%20SyntheticRequest%20SINCE%20%27" * startTimeNR *
        "%27%20UNTIL%20%27" * endTimeNR * "%27%20WHERE%20monitorName%20%3D%20%27" * monitor * "%27%20" *
        "with%20timezone%20%27America%2FNew_York%27"

    curlStr = ["-H","$apiKey","$curlCommand","$sqlCommand"]

    debugPrintCurlCommand(SP,curlStr,sqlCommand)

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

function curlCommands(TV::TimeVars,SP::ShowParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Time Range ",TV.timeString)
    end

    if CU.apiAdminKey != "no id"
    else
        Key = "unknown"
    end

    if CU.syntheticListAllMonitors
        apiKey = "X-Api-Key:" * CU.apiAdminKey
        curlCommand = "https://synthetics.newrelic.com/synthetics/api/v3/monitors"
    elseif CU.syntheticListOneMonitor
        apiKey = "X-Api-Key:" * CU.apiAdminKey
        curlCommand = "https://synthetics.newrelic.com/synthetics/api/v3/monitors/" * CU.syntheticMonitorId
    elseif CU.syntheticBodySize
        apiKey = "X-Query-Key:" * CU.apiQueryKey
        curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=" *
        "select%20average(totalResponseBodySize)%20FROM%20SyntheticCheck%20" *
        "WHERE%20%20monitorName%20%3D%27" * CU.syntheticMonitor * "%27%20SINCE%2030%20days%20ago%20TIMESERIES%20%20auto"
    elseif CU.syntheticBodySizeByRequest
        apiKey = "X-Query-Key:" * CU.apiQueryKey
        curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=" *
        "SELECT%20average(responseBodySize)%20FROM%20SyntheticRequest%20" *
        "WHERE%20monitorId%20%3D%20%27" * CU.syntheticMonitorId *
        "%27%20since%207%20days%20ago%20%20TIMESERIES"
        #         69599173-5b61-41e0-b4e6-ba69e179bc70
    else
        curlCommand = "unknown command"
    end

    curlStr = ["-H","$apiKey","$curlCommand"]

    # Todo regular expression tests for "unknown" and report failure and return empty
    if SP.debugLevel > 4
        println("curlStr=",curlStr)
    end

    curlCmd = `curl $curlStr`
    jsonString = readstring(pipeline(curlCmd,stderr=DevNull))

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

#
#  Fill in NR structure from Curl Dictionaries
#
function fillNrTimeSeries(SP::ShowParams,NR::NrParams,seriesArray::Array)

    if SP.debugLevel > 8
        println("Series ",seriesArray)
    end

    nrows = length(seriesArray)
    #colnames = convert(Vector{UTF8String}, collect(keys(seriesArray[1])))
    colnames = ["inspectedCount","endTimeSeconds","beginTimeSeconds"]
    sort!(colnames)

    ncols = length(colnames)

    df = DataFrame(Any,nrows,ncols+1)
    for i in 1:nrows
        for j in 1:ncols
            df[i, j] = seriesArray[i][colnames[j]]
        end
    end

    colnames = ["results"]
    for i in 1:nrows
        j = 4
        innerDict = seriesArray[i][colnames[1]][1]
        df[i,j] = innerDict["average"]
    end
    df = names!(df,[Symbol("beginTimeSeconds"),Symbol("endTimeSeconds"),Symbol("inspectedCount"),Symbol("averageTotalReceivedSize")])

    if SP.debugLevel > 4
        beautifyDF(df[1:3,:])
    end

    #todo store into structure
    NR.timeSeries.row = deepcopy(df)

end

function fillNrResults(SP::ShowParams,NR::NrParams,resultsArray::Array)

    if SP.debugLevel > 8
        println("Series ",resultsArray)
    end

    eventsDict = resultsArray[1]
    eventArray = eventsDict["events"]

    nrows = length(eventArray)
    #colnames = convert(Vector{UTF8String}, collect(keys(eventArray[1])))

    colnames = ["timestamp","jobId","onPageContentLoad","onPageLoad",
        "duration","durationBlocked","durationConnect","durationDNS","durationReceive","durationSend","durationSSL","durationWait",
        "requestBodySize","requestHeaderSize","responseBodySize","responseHeaderSize","responseStatus","responseCode","pageref",
        "contentType","contentCategory","verb","externalResource","host","path",
        "hierarchicalURL","URL","domain","serverIPAddress""jobId","monitorName"]

    ncols = length(colnames)

    #println("events=",colnames," nrows=",nrows," ncols=",ncols)

    df = DataFrame(Any,nrows,ncols)
    for i in 1:nrows
        for j in 1:ncols
            df[i, j] = get(eventArray[i],colnames[j],NA)
        end
    end

    df = names!(df,[Symbol("timestamp"),Symbol("jobId"),Symbol("onPageContentLoad"),Symbol("onPageLoad"),
    Symbol("duration"),Symbol("durationBlocked"),Symbol("durationConnect"),Symbol("durationDNS"),
    Symbol("durationReceive"),Symbol("durationSend"),Symbol("durationSSL"),Symbol("durationWait"),
    Symbol("requestBodySize"),Symbol("requestHeaderSize"),Symbol("responseBodySize"),Symbol("responseHeaderSize"),
    Symbol("responseStatus"),Symbol("responseCode"),Symbol("pageref"),Symbol("contentType"),
    Symbol("contentCategory"),Symbol("verb"),Symbol("externalResource"),Symbol("host"),Symbol("path"),
    Symbol("hierarchicalURL"),Symbol("URL"),Symbol("domain"),Symbol("serverIPAddress""jobId"),Symbol("monitorName")])

    sort!(df,cols=[order(:timestamp,rev=false)])

    if SP.debugLevel > 4
        quickTitle("Debug4: Fill New Relic Results")
        beautifyDF(df[1:10,:],maxRows=500)
    end

    NR.results.row = deepcopy(df)

end

function fillNrTotal(SP::ShowParams,NR::NrParams,totalDict::Dict)

    if SP.debugLevel > 8
        println("Total Dict ",totalDict)
    end

    NR.totals.inspectedCount = totalDict["inspectedCount"]
    NR.totals.endTimeSeconds = totalDict["endTimeSeconds"]
    NR.totals.beginTimeSeconds = totalDict["beginTimeSeconds"]

    # Assuming only one result for now
    for result in totalDict["results"]
        NR.totals.resultAverage = result["average"]
    end
end

function fillNrRunPerf(SP::ShowParams,NR::NrParams,perfDict::Dict)

    if SP.debugLevel > 8
        println()
        println("Perf Dict ",perfDict)
    end

    NR.runPerf.inspectedCount = perfDict["inspectedCount"]
    NR.runPerf.wallClockTime = perfDict["wallClockTime"]
end

function fillNrMetadata(SP::ShowParams,NR::NrParams,metaDict::Dict)

    if SP.debugLevel > 8
        println()
        println("Meta Dict ",metaDict)
    end

    NR.metadata.endTime = metaDict["endTime"]
    NR.metadata.beginTime = metaDict["beginTime"]

end
