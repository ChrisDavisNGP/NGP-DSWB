

function curlJsonWorkflow(TV::TimeVars,SP::ShowParams,CU::CurlParams)

    if CU.synthetic
        finalDict = syntheticCommands(TV,SP,CU)
    elseif CU.syntheticBodySize
        finalDict = syntheticCommands(TV,SP,CU)
    else
        println("NR Type not yet defined")
        return
    end

    return finalDict

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
        curlStr = ["-H","$apiKey","$curlCommand"]
    elseif CU.syntheticListOneMonitor
        apiKey = "X-Api-Key:" * CU.apiAdminKey
        curlCommand = "https://synthetics.newrelic.com/synthetics/api/v3/monitors/" * CU.syntheticMonitorId
        curlStr = ["-H","$apiKey","$curlCommand"]
    elseif CU.syntheticBodySize
        apiKey = "X-Query-Key:" * CU.apiQueryKey
        curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=select%20average(totalResponseBodySize)%20FROM%20SyntheticCheck%20WHERE%20%20monitorName%20%3D%27JTP-Gallery-Equinox-M%27%20SINCE%2030%20days%20ago%20TIMESERIES%20%20auto"
        curlStr = ["-H","$apiKey","$curlCommand"]
    elseif CU.syntheticBodySizeByRequest
        apiKey = "X-Query-Key:" * CU.apiQueryKey
        curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=SELECT%20average(responseBodySize)%20FROM%20SyntheticRequest%20WHERE%20monitorId%20%3D%20%2769599173-5b61-41e0-b4e6-ba69e179bc70%27%20since%207%20days%20ago%20%20TIMESERIES"
        curlStr = ["-H","$apiKey","$curlCommand"]
    else
        curlCommand = "unknown command"
    end

    # Todo regular expression tests for "unknown" and report failure and return empty
    if SP.debugLevel > 4
        println("curlStr=",curlStr)
    end

    curlCmd = `curl $curlStr`
    jsonString = readstring(curlCmd)

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

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

    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=" *
        "SELECT%20stddev(totalResponseBodySize)%2Caverage(totalResponseBodySize)%2Cstddev(duration)%2Caverage(duration)%20" *
        "FROM%20SyntheticCheck%20facet%20monitorName%20since%20%27" * startTimeNR * "%27%20until%20%27" * endTimeNR *
        "%27%20with%20TIMEZONE%20%27America%2FNew_York%27%20limit%20500%20COMPARE%20WITH%20" * compareWith
    curlStr = ["-H","$apiKey","$curlCommand"]

    #SELECT stddev(totalResponseBodySize),average(totalResponseBodySize),stddev(duration),average(duration) FROM SyntheticCheck facet monitorName since '2018-01-10 00:07:00' until '2018-01-10 17:00:00' with TIMEZONE 'America/New_York' limit 500
    # https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=SELECT%20
    #stddev(totalResponseBodySize)%2Caverage(totalResponseBodySize)%2Cstddev(duration)%2Caverage(duration)%20
    #FROM%20SyntheticCheck%20facet%20monitorName%20%
    #since%20%272018-01-10%2007%3A00%3A00%27%20
    #until%20%272018-01-10%2017%3A00%3A0027%20
    #with%20TIMEZONE%20%27America%2FNew_York%27%20limit%20500

    if SP.debugLevel > 4
        println("curlStr=",curlStr)
    end

    curlCmd = `curl $curlStr`
    jsonString = readstring(curlCmd)

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

    apiKey = "X-Query-Key:" * CU.apiQueryKey
    #curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=select%20average(totalResponseBodySize)%20FROM%20SyntheticCheck%20WHERE%20%20monitorName%20%3D%27JTP-Gallery-Equinox-M%27%20SINCE%2030%20days%20ago%20TIMESERIES%20%20auto"
    curlCommand = "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=SELECT%20*%20FROM%20SyntheticRequest%20SINCE%20%27" * startTimeNR *
        "%27%20UNTIL%20%27" * endTimeNR * "%27%20WHERE%20monitorName%20%3D%20%27" * monitor * "%27%20with%20timezone%20%27America%2FNew_York%27%20limit%20500"
    curlStr = ["-H","$apiKey","$curlCommand"]

# Dec 20 Small   https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=SELECT%20*%20FROM%20SyntheticRequest%20SINCE%201513793700000%20UNTIL%201513795500000%20WHERE%20monitorName%20%3D%20%27JTP-Gallery-Equinox-M%27
#SELECT * FROM SyntheticRequest SINCE 1513793700000 UNTIL 1513795500000 WHERE monitorName = 'JTP-Gallery-Equinox-M'

# Dec 20 Large   https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="SELECT%20*%20FROM%20SyntheticRequest%20SINCE%201513835100000%20UNTIL%201513836900000%20WHERE%20monitorName%20%3D%20%27JTP-Gallery-Equinox-M%27"
#SELECT * FROM SyntheticRequest SINCE 1513835100000 UNTIL 1513836900000 WHERE monitorName = 'JTP-Gallery-Equinox-M' 21 dec 01:06:34

# Jan 5 Normal  https://insights-api.newrelic.com/v1/accounts/78783/query?nrql="SELECT%20*%20FROM%20SyntheticRequest%20SINCE%201515189420000%20UNTIL%201515193020000%20WHERE%20monitorName%20%3D%20%27JTP-Gallery-Equinox-M%27"
#SELECT * FROM SyntheticRequest SINCE 1515189420000 UNTIL 1515193020000 WHERE monitorName = 'JTP-Gallery-Equinox-M'  05 Jan 18 17:36

    # curl -H "Accept: application/json" -H ""
    #

    # Todo regular expression tests for "unknown" and report failure and return empty
    if SP.debugLevel > 4
        println("curlStr=",curlStr)
    end

    curlCmd = `curl $curlStr`
    jsonString = readstring(curlCmd)

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end


function syntheticCommands(TV::TimeVars,SP::ShowParams,CU::CurlParams)

    #  List all syn monitors
    #   curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors'
    if CU.syntheticListAllMonitors
        jsonInput = curlCommands(TV,SP,CU)
        finalDict = curlSyntheticJson(SP,jsonInput)
        return finalDict
    end

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'
    if CU.syntheticListOneMonitor
        jsonInput = curlCommands(TV,SP,CU)
        finalDict = curlSyntheticJson(SP,jsonInput)
        return finalDict
    end

    # curl -H "Accept: application/json" -H "X-Query-Key: HFdC9JQE7P3Bkwk9HMl0kgVTH2j5yucx"
    # "https://insights-api.newrelic.com/v1/accounts/78783/query?nrql=SELECT%20average(responseBodySize)%20FROM%20SyntheticRequest%20WHERE%20monitorId%20%3D%20%2769599173-5b61-41e0-b4e6-ba69e179bc70%27%20since%207%20days%20ago%20%20TIMESERIES

    if CU.syntheticBodySize
        jsonInput = curlCommands(TV,SP,CU)
        finalDict = curlSyntheticJson(SP,jsonInput)
    end


end

function curlSyntheticJson(SP::ShowParams,jList::ASCIIString)

    if SP.debugLevel > 8
        println("jList=",jList)
    end

    jParsed = JSON.parse(jList)

    if SP.debugLevel > 6
        println(jParsed)
        println(typeof(jParsed))
    end

    return jParsed
end

function dailyChangeCheckWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,NR::NrParams,CU::CurlParams)

    openingTitle(TV,UP,SP)

    dailyChangeCheck(UP,SP,NR,CU)

end

function dailyChangeCheck(UP::UrlParams,SP::ShowParams,NR::NrParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Starting dailyChangeCheck")
    end

    jsonTimeString = curlSelectDurationAndSize(SP,CU,CU.oldStart,CU.oldEnd)
    timeDict = curlSyntheticJson(SP,jsonTimeString)
    monitorsDF = fillNrTotalResults(SP,NR,timeDict)

    diffDailyChange(SP,monitorsDF;diffBySize=false)
    diffDailyChange(SP,monitorsDF;diffBySize=true)

end

function timeSizeRequestsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,NR::NrParams,CU::CurlParams)

    openingTitle(TV,UP,SP)

    investigateSizeProblems(UP,SP,NR,CU)

end

function investigateSizeProblems(TV::TimeVars,UP::UrlParams,SP::ShowParams,NR::NrParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Starting investigateSizeProblems")
    end

    if SP.debugLevel > 6
        beautifyDF(NR.timeSeries.row[1:3,:])

        try
            drawDF = DataFrame()
            drawDF[:col1] = NR.timeSeries.row[:beginTimeSeconds]
            drawDF[:data1] = NR.timeSeries.row[:averageTotalReceivedSize]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Average Size"], mPulseWidget=false, chart_title="Size Chart", vizTypes=["line"])
        catch y
            println("draw Avg Recd Size exception ",y)
        end
    end

    jsonTimeString = curlSelectAllByTime(TV,SP,CU,CU.oldStart,CU.oldEnd,CU.syntheticMonitor)
    timeDict = curlSyntheticJson(SP,jsonTimeString)

    if SP.debugLevel > 8
        fillNrMetadata(SP,NR,timeDict["metadata"])
        println("Metadata: Begin=",NR.metadata.beginTime," End=",NR.metadata.endTime)

        fillNrRunPerf(SP,NR,timeDict["performanceStats"])
        println("Run Perf: Inspected=",NR.runPerf.inspectedCount," Wall Time=",NR.runPerf.wallClockTime)
    end

    fillNrResults(SP,NR,timeDict["results"])
    test1DF = dumpHostGroups(SP,NR;showGroups=false)


    jsonTimeString = curlSelectAllByTime(TV,SP,CU,CU.newStart,CU.newEnd,CU.syntheticMonitor)
    timeDict = curlSyntheticJson(SP,jsonTimeString)
    fillNrResults(SP,NR,timeDict["results"])
    test2DF = dumpHostGroups(SP,NR;showGroups=false)

    test1DFSize = deepcopy(test1DF)
    test2DFSize = deepcopy(test2DF)
    diffHostGroups(SP,test1DF,test2DF;diffBySize=false)
    diffHostGroups(SP,test1DFSize,test2DFSize;diffBySize=true)

end

function newRelicConvert(SP::ShowParams,NR::NrParams,synChkBodySizeDict::Dict)

    fillNrTotal(SP,NR,synChkBodySizeDict["total"])
    println()
    println("Total: Inspected=",NR.totals.inspectedCount,
            " Begin Sec=",NR.totals.beginTimeSeconds,
            " End Sec=",NR.totals.endTimeSeconds,
            " Result Average=",NR.totals.resultAverage
            )

    fillNrMetadata(SP,NR,synChkBodySizeDict["metadata"])
    println("Metadata: Begin=",NR.metadata.beginTime," End=",NR.metadata.endTime)

    fillNrRunPerf(SP,NR,synChkBodySizeDict["performanceStats"])
    println("Run Perf: Inspected=",NR.runPerf.inspectedCount,
             " Wall Time=",NR.runPerf.wallClockTime)

    println()

    fillNrTimeSeries(SP,NR,synChkBodySizeDict["timeSeries"])
    println("Times Series: size",size(NR.timeSeries.row,1))
    #This is an array println("TimeS Dict ",keys(synChkBodySizeDict["timeSeries"]))

end

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
        beautifyDF(df,maxRows=500)
    end

    NR.results.row = deepcopy(df)

end

function fillNrTotalResults(SP::ShowParams,NR::NrParams,totalResultsDict::Dict)

    if SP.debugLevel > 8
        println("Total Results ",totalResultsDict)
    end

    println()
    println("Starting Fill NR Total Results")

    #for results in totalResultsDict["totalResult"]
    #    println()
    #    println(results)
    #end

    #for results in totalResultsDict["unknownGroup"]
    #    println()
    #    println(results)
    #end
    monitorsDF = DataFrame(name=ASCIIString[],
        oldSizeStdDev=Float64[],oldSizeAvg=Float64[],oldDurationStdDev=Float64[],oldDurationAvg=Float64[],
        newSizeStdDev=Float64[],newSizeAvg=Float64[],newDurationStdDev=Float64[],newDurationAvg=Float64[],
        )

    periodResultsDict = totalResultsDict["current"]
    for monitorDict in periodResultsDict["facet"]
        #println()
        #println(monitorDict)
        #println(monitorName," Size Std Dev=",sizeStdDev," Average=",sizeAvg," Duration Std Dev=",durationStdDev," Duration=",durationAvg)
        monitorName = monitorDict["name"]

        newSizeStdDev = monitorDict["results"][1]["standardDeviation"]
        newSizeAvg = monitorDict["results"][2]["average"]
        newDurationStdDev = monitorDict["results"][3]["standardDeviation"]
        newDurationAvg = monitorDict["results"][4]["average"]

        push!(monitorsDF,[monitorName,
            0.0,0.0,0.0,0.0,
            newSizeStdDev,newSizeAvg,newDurationStdDev,newDurationAvg
            ])
    end

    periodResultsDict = totalResultsDict["previous"]
    for monitorDict in periodResultsDict["facets"]
        #println()
        #println(monitorDict)
        #println(monitorName," Size Std Dev=",sizeStdDev," Average=",sizeAvg," Duration Std Dev=",durationStdDev," Duration=",durationAvg)
        monitorName = monitorDict["name"]

        oldSizeStdDev = monitorDict["results"][1]["standardDeviation"]
        oldSizeAvg = monitorDict["results"][2]["average"]
        oldDurationStdDev = monitorDict["results"][3]["standardDeviation"]
        oldDurationAvg = monitorDict["results"][4]["average"]

        push!(monitorsDF,[monitorName,
            oldSizeStdDev,oldSizeAvg,oldDurationStdDev,oldDurationAvg,
            0.0,0.0,0.0,0.0
            ])
    end

    sort!(monitorsDF,cols=[order(:name,rev=false)])
#todo groupby name

    if SP.debugLevel > 4
        beautifyDF(monitorsDF,maxRows=500)
    end

    return monitorsDF
end

# Simple three fields with one tricky field
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

function dumpHostGroups(SP::ShowParams,NR::NrParams;showGroups::Bool=true)

    if SP.debugLevel > 8
        println("Starting dumpHostGroups")
    end

    hostGroupsDF = DataFrame(host=ASCIIString[],bodySize=Int64[],resources=Int64[],duration=Float64[])

    i = 0
    for host in NR.results.row[:,:host]
        i += 1
        if isna(host)
            continue
        elseif (ismatch(r".*segment.*",host))
            NR.results.row[i:i,:host] = "Segment"
        elseif (ismatch(r".*blueconic.*",host))
            NR.results.row[i:i,:host] = "Blue Conic"
        elseif (ismatch(r".*krxd.*",host))
            NR.results.row[i:i,:host] = "Krxd"
        elseif (ismatch(r".*google-analytics.*",host))
            NR.results.row[i:i,:host] = "Google Analytics"
        elseif (ismatch(r".*unrulymedia.*",host))
            NR.results.row[i:i,:host] = "Unruly Media"
        elseif (ismatch(r".*demdex.*",host))
            NR.results.row[i:i,:host] = "Demdex"
        elseif (ismatch(r".*monetate.*",host))
            NR.results.row[i:i,:host] = "Monetate"
        elseif (ismatch(r".*moatads.*",host))
            NR.results.row[i:i,:host] = "Moatads"
        elseif (ismatch(r".*addthis.*",host))
            NR.results.row[i:i,:host] = "Addthis"
        elseif (ismatch(r".*doubleverify.*",host))
            NR.results.row[i:i,:host] = "DoubleVerify"
        elseif (ismatch(r".*doubleclick.*",host))
            NR.results.row[i:i,:host] = "DoubleClick"
        elseif (ismatch(r".*googlesyndication.*",host))
            NR.results.row[i:i,:host] = "Google Syndication"
        elseif (ismatch(r".*extremereach.*",host))
            NR.results.row[i:i,:host] = "Extreme Reach"
        end

    end

    for subDF in groupby(NR.results.row,:host)

        if isna(subDF[1:1,:host][1])
            continue
        end
        push!(hostGroupsDF,[subDF[1:1,:host][1],sum(subDF[:,:responseBodySize]),size(subDF,1),sum(subDF[:,:duration])])
    end

    sort!(hostGroupsDF,cols=:bodySize,rev=true)
    if showGroups
        beautifyDF(hostGroupsDF)
    end

    return hostGroupsDF

end

function diffHostGroups(SP::ShowParams,test1DF::DataFrame,test2DF::DataFrame;diffBySize::Bool=true)

    # Assume test1DF is LHS

    if SP.debugLevel > 8
        beautifyDF(test1DF[1:3,:])
        beautifyDF(test2DF[1:3,:])
    end

    diffDF = DataFrame(host=ASCIIString[],delta=Float64[],oldSize=Int64[],newSize=Int64[],oldDuration=Float64[],newDuration=Float64[])

    t1 = 0
    for hostT1 in test1DF[:,:host]
        printed = false
        t1 += 1
        sizeT1 = test1DF[t1:t1,:bodySize][1]
        durationT1 = test1DF[t1:t1,:duration][1]
        t2 = 0
        for hostT2 in test2DF[:,:host]
            t2 += 1
            if hostT1 == hostT2
                sizeT2 = test2DF[t2:t2,:bodySize][1]
                durationT2 = test2DF[t2:t2,:duration][1]
                #println(hostT1," h1=",sizeT1," h2=",sizeT2)
                if diffBySize && sizeT2 == sizeT1
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                elseif !diffBySize && durationT2 == durationT1
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                end

                if diffBySize && sizeT2 == 0
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                elseif !diffBySize && durationT2 < 100  # 100 ms shift can be ignored
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                end

                if diffBySize && sizeT1 == 0
                    break;
                elseif !diffBySize && durationT1 == 0
                    break;
                end

                if diffBySize
                    deltaPercent = (sizeT2-sizeT1) / sizeT1 * 100.0
                    if !(deltaPercent > -5.0 && deltaPercent < 5.0)
                        #println(hostT1," delta=",deltaPercent," h1=",sizeT1," h2=",sizeT2)
                        push!(diffDF,[hostT1,deltaPercent,sizeT1,sizeT2,durationT1,durationT2])
                    end
                else
                    deltaPercent = (durationT2-durationT1) / durationT1 * 100.0
                    if !(deltaPercent > -25.0 && deltaPercent < 25.0)
                        #println(hostT1," delta=",deltaPercent," h1=",sizeT1," h2=",sizeT2)
                        push!(diffDF,[hostT1,deltaPercent,sizeT1,sizeT2,durationT1,durationT2])
                    end
                end

                printed = true
                deleterows!(test2DF,t2)
                break
            end
        end
        if !printed && sizeT1 > 999
            #println(hostT1," h1=", sizeT1)
            push!(diffDF,[hostT1,0.0,sizeT1,0,durationT1,0])
        end
    end

    t2 = 0
    for hostT2 in test2DF[:,:host]
        t2 += 1
        sizeT2 = test2DF[t2:t2,:bodySize][1]
        durationT2 = test2DF[t2:t2,:duration][1]
        if sizeT2 > 999
            #println(hostT2," h2=", sizeT2)
            push!(diffDF,[hostT2,0.0,0,sizeT2,0,durationT2])
        end
    end

    if diffBySize
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Size Change"),Symbol("Old Size"),Symbol("New Size"),Symbol("Old Duration"),Symbol("New Duration")])
    else
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Duration Change"),Symbol("Old Size"),Symbol("New Size"),Symbol("Old Duration"),Symbol("New Duration")])
    end

    beautifyDF(diffDF,defaultNumberFormat=(:precision => 0, :commas => true))

end

function diffDailyChange(SP::ShowParams,monitorsDF::DataFrame;diffBySize::Bool=true)


    if SP.debugLevel > -1
        beautifyDF(monitorsDF[1:10,:])
    end

    activeMonitorsDF = monitorsDF[Bool[x > 0 for x in test1DF[:sizeStdDev]],:]
    beautifyDF(activeMonitorsDF[1:10,:])

    return
    diffDF = DataFrame(host=ASCIIString[],delta=Float64[],oldSize=Int64[],newSize=Int64[],oldDuration=Float64[],newDuration=Float64[])

    t1 = 0
    for hostT1 in test1DF[:,:host]
        printed = false
        t1 += 1
        sizeT1 = test1DF[t1:t1,:bodySize][1]
        durationT1 = test1DF[t1:t1,:duration][1]
        t2 = 0
        for hostT2 in test2DF[:,:host]
            t2 += 1
            if hostT1 == hostT2
                sizeT2 = test2DF[t2:t2,:bodySize][1]
                durationT2 = test2DF[t2:t2,:duration][1]
                #println(hostT1," h1=",sizeT1," h2=",sizeT2)
                if diffBySize && sizeT2 == sizeT1
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                elseif !diffBySize && durationT2 == durationT1
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                end

                if diffBySize && sizeT2 == 0
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                elseif !diffBySize && durationT2 < 100  # 100 ms shift can be ignored
                    deleterows!(test2DF,t2)
                    printed = true
                    break;
                end

                if diffBySize && sizeT1 == 0
                    break;
                elseif !diffBySize && durationT1 == 0
                    break;
                end

                if diffBySize
                    deltaPercent = (sizeT2-sizeT1) / sizeT1 * 100.0
                    if !(deltaPercent > -5.0 && deltaPercent < 5.0)
                        #println(hostT1," delta=",deltaPercent," h1=",sizeT1," h2=",sizeT2)
                        push!(diffDF,[hostT1,deltaPercent,sizeT1,sizeT2,durationT1,durationT2])
                    end
                else
                    deltaPercent = (durationT2-durationT1) / durationT1 * 100.0
                    if !(deltaPercent > -25.0 && deltaPercent < 25.0)
                        #println(hostT1," delta=",deltaPercent," h1=",sizeT1," h2=",sizeT2)
                        push!(diffDF,[hostT1,deltaPercent,sizeT1,sizeT2,durationT1,durationT2])
                    end
                end

                printed = true
                deleterows!(test2DF,t2)
                break
            end
        end
        if !printed && sizeT1 > 999
            #println(hostT1," h1=", sizeT1)
            push!(diffDF,[hostT1,0.0,sizeT1,0,durationT1,0])
        end
    end

    t2 = 0
    for hostT2 in test2DF[:,:host]
        t2 += 1
        sizeT2 = test2DF[t2:t2,:bodySize][1]
        durationT2 = test2DF[t2:t2,:duration][1]
        if sizeT2 > 999
            #println(hostT2," h2=", sizeT2)
            push!(diffDF,[hostT2,0.0,0,sizeT2,0,durationT2])
        end
    end

    if diffBySize
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Size Change"),Symbol("Old Size"),Symbol("New Size"),Symbol("Old Duration"),Symbol("New Duration")])
    else
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Duration Change"),Symbol("Old Size"),Symbol("New Size"),Symbol("Old Duration"),Symbol("New Duration")])
    end

    beautifyDF(diffDF,defaultNumberFormat=(:precision => 0, :commas => true))

end
