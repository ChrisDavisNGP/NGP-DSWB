include("URL-Classification-Data.jl")

function synCheckWorkflow(TV::TimeVars,SP::ShowParams,CU::CurlParams)

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

function syntheticCommands(TV::TimeVars,SP::ShowParams,CU::CurlParams)

    #  List all syn monitors
    if CU.syntheticListAllMonitors
        jsonInput = curlCommands(TV,SP,CU)
        finalDict = curlSyntheticJson(SP,jsonInput)
        return finalDict
    end

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    if CU.syntheticListOneMonitor
        jsonInput = curlCommands(TV,SP,CU)
        finalDict = curlSyntheticJson(SP,jsonInput)
        return finalDict
    end

    # curl -H "Accept: application/json" -H "X-Query-Key: HFdC9JQE7P3Bkwk9HMl0kgVTH2j5yucx"

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

    if SP.debugLevel > 8
        println(jParsed)
        println(typeof(jParsed))
    end

    return jParsed
end

function dailyChangeCheckOnPageLoadWorkflow(oldTV::TimeVars,newTV::TimeVars,SP::ShowParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Starting dailyChangeCheckOnPageLoad")
    end

    # Get a list of Monitors

    jsonMonitorList = curlSelectActiveSyntheticMonitors(SP,CU)
    monitorListDict = curlSyntheticJson(SP,jsonMonitorList)
    monitorListDF = monitorListResults(SP,monitorListDict)

    for monitor in monitorListDF[:name]

        # To Do look for locations and divide
        jsonOnPageLoad = curlSelectByMonitorOnPageLoad(newTV,SP,CU,monitor)
        onPageLoadDict = curlSyntheticJson(SP,jsonOnPageLoad)
        onPageLoadNewDF = monitorOnPageLoad(SP,onPageLoadDict)

        if size(onPageLoadNewDF,1) == 0
            continue
        end

        jsonOnPageLoad = curlSelectByMonitorOnPageLoad(oldTV,SP,CU,monitor)
        onPageLoadDict = curlSyntheticJson(SP,jsonOnPageLoad)
        onPageLoadOldDF = monitorOnPageLoad(SP,onPageLoadDict)

        if size(onPageLoadOldDF,1) == 0
            continue
        end

        diffDailyChangeOnPageLoad(oldTV,newTV,SP,CU,onPageLoadNewDF,onPageLoadOldDF)
        #break;
    end

    return


end


function dailyChangeCheckWorkflow(SP::ShowParams,CU::CurlParams)

    dailyChangeCheck(SP,CU)

end

function dailyChangeCheck(SP::ShowParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Starting dailyChangeCheck")
    end

    jsonTimeString = curlSelectDurationAndSize(SP,CU,CU.oldStart,CU.oldEnd)
    timeDict = curlSyntheticJson(SP,jsonTimeString)
    monitorsDF = getMonitorsFromTotalResults(SP,timeDict)
    sizeMonitorsDF = deepcopy(monitorsDF)

    diffDailyChange(SP,monitorsDF;diffBySize=false)
    diffDailyChange(SP,sizeMonitorsDF;diffBySize=true)

end

function timeSizeRequestsWorkflow(TV::TimeVars,SP::ShowParams,NR::NrParams,CU::CurlParams)

    #openingTitle(TV,UP,SP)

    investigateSizeProblems(TV,SP,NR,CU)

end

function investigateSizeProblems(TV::TimeVars,SP::ShowParams,NR::NrParams,CU::CurlParams)

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
    test1DF = dumpHostGroups(SP,NR;showGroups=true)


    jsonTimeString = curlSelectAllByTime(TV,SP,CU,CU.newStart,CU.newEnd,CU.syntheticMonitor)
    timeDict = curlSyntheticJson(SP,jsonTimeString)
    fillNrResults(SP,NR,timeDict["results"])
    test2DF = dumpHostGroups(SP,NR;showGroups=true)

    test1DFSize = deepcopy(test1DF)
    test2DFSize = deepcopy(test2DF)
    diffHostGroups(SP,test1DF,test2DF;diffBySize=false)
    diffHostGroups(SP,test1DFSize,test2DFSize;diffBySize=true)

end

function syntheticStatsWorkflow(TV::TimeVars,SP::ShowParams,NR::NrParams,CU::CurlParams)

    #openingTitle(TV,UP,SP)

    investigateStats(TV,SP,NR,CU)

end

function investigateStats(TV::TimeVars,SP::ShowParams,NR::NrParams,CU::CurlParams)

    if SP.debugLevel > 8
        println("Starting investigateStats")
    end

    jsonTimeString = curlSelectAllByTimeAndUrl(TV,SP,CU,CU.oldStart,CU.oldEnd,CU.syntheticMonitor)
    timeDict = curlSyntheticJson(SP,jsonTimeString)

    fillNrResults(SP,NR,timeDict["results"])

    #df = deepcopy(NR.results.row)
    #delRows = Int64[]
    #i = 0
    #for r in eachrow(df)
    #        i += 1
    #    if !ismatch(r"https://www.nationalgeographic.com/photography/proof/2017/09/fall-equinox-gallery/$",r[:URL])
    #        push!(delRows,i)
    #    end
    #end
    #println(" row count 1 ",size(NR.results.row,1))
    #deleterows!(df,delRows)

#    println(" row count 1 ",size(df,1))

    if SP.debugLevel > 6
        beautifyDF(NR.results.row[1:3,:])
    end

    quickTimestampViz(NR,:onPageLoad,"On Page Load")
    quickTimestampViz(NR,:duration,"Duration")
    quickTimestampViz(NR,:onPageContentLoad,"On Page Content Load")
    quickTimestampViz(NR,:durationBlocked,"Duration Blocked")
    quickTimestampViz(NR,:durationConnect,"Duration Connect")
    quickTimestampViz(NR,:durationDNS,"Duration DNS")
    quickTimestampViz(NR,:durationReceive,"Duration Receive")
    quickTimestampViz(NR,:durationSend,"Duration Send")
    quickTimestampViz(NR,:durationSSL,"Duration SSL")
    quickTimestampViz(NR,:durationWait,"Duration Wait")
    quickTimestampViz(NR,:requestBodySize,"Request Body Size")
    quickTimestampViz(NR,:requestHeaderSize,"Request Header Size")
    quickTimestampViz(NR,:responseBodySize,"Response Body Size")
    quickTimestampViz(NR,:responseHeaderSize,"Response Header Size")
    quickTimestampViz(NR,:responseCode,"Response Code")

end

function quickTimestampViz(NR::NrParams,theSymbol::Symbol,Title::ASCIIString)
    try
        drawDF = DataFrame()
        drawDF[:col1] = unix2datetime(NR.results.row[:timestamp]/1000.0)
        drawDF[:data1] = NR.results.row[theSymbol]
        axis_x_min = 0

        i = 0
        for row in eachrow(drawDF)
            i += 1
            #println("row ",i, " ",typeof(row[:col1]))
            if typeof(row[:col1]) == Int64
                row[:col1] = unix2datetime(row[:col1]/1000.0)
            end
        end

        #Trim points above 3 StdDev
        dv = Array{Float64}(drawDF[:data1])
        statsDF = basicStatsFromDV(dv)
        topRangeA = statsDF[:median] + (3 * statsDF[:stddev])
        topRange = topRangeA[1]
        nrows, ncols = size(drawDF)
        for row in 1:nrows
            if drawDF[row,:data1] > topRange
                drawDF[row,:data1] = topRange
            end
        end

        quickTitle(ASCIIString(NR.results.row[1:1,:monitorName][1] * " " * Title) )

        beautifyDF(statsDF)

        c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=[Title],
                mPulseWidget=false, chart_title=" ", vizTypes=["area-spline"],
                axis_x_min=axis_x_min)
    catch y
        println("quickTimestampViz exception ",y)
    end
end

function newRelicConvert(SP::ShowParams,NR::NrParams,synChkBodySizeDict::Dict)

    fillNrTotal(SP,NR,synChkBodySizeDict["total"])

    fillNrMetadata(SP,NR,synChkBodySizeDict["metadata"])

    fillNrRunPerf(SP,NR,synChkBodySizeDict["performanceStats"])

    fillNrTimeSeries(SP,NR,synChkBodySizeDict["timeSeries"])

    #This is an array println("TimeS Dict ",keys(synChkBodySizeDict["timeSeries"]))
    if SP.debugLevel > 0
        println()
        println("Total: Inspected=",NR.totals.inspectedCount,
                " Begin Sec=",NR.totals.beginTimeSeconds,
                " End Sec=",NR.totals.endTimeSeconds,
                " Result Average=",NR.totals.resultAverage
                )
        println("Metadata: Begin=",NR.metadata.beginTime," End=",NR.metadata.endTime)

        println("Run Perf: Inspected=",NR.runPerf.inspectedCount,
                 " Wall Time=",NR.runPerf.wallClockTime)
         println("Times Series: size=",size(NR.timeSeries.row,1))
     end

end

function monitorListResults(SP::ShowParams,monitorListDict::Dict)

    if SP.debugLevel > 8
        println("Monitor List Results ",monitorListDict)
    end

    resultsArray = monitorListDict["results"]
    resultsDict = resultsArray[1]
    eventArray = resultsDict["members"]

    nrows = length(eventArray)
    #colnames = convert(Vector{UTF8String}, collect(keys(eventArray[1])))

    colnames = ["name"]

    ncols = length(colnames)

    #println("events=",colnames," nrows=",nrows," ncols=",ncols)

    df = DataFrame(Any,nrows,ncols)
    for i in 1:nrows
        df[i, 1] = eventArray[i]
    end

    df = names!(df,[Symbol("name")])

    sort!(df,cols=[order(:name,rev=false)])

    if SP.debugLevel > 4
        quickTitle("Debug4: Fill New Relic Results")
        beautifyDF(df[1:10,:],maxRows=500)
    end

    return df

end

function monitorOnPageLoad(SP::ShowParams,onPageLoadDict::Dict)

    if SP.debugLevel > 8
        println("On Page Load Results ",onPageLoadDict)
    end

    resultsArray = onPageLoadDict["results"]
    eventsDict = resultsArray[1]
    eventArray = eventsDict["events"]

    nrows = length(eventArray)
    #colnames = convert(Vector{UTF8String}, collect(keys(eventArray[1])))

    colnames = ["checkId","monitorName","timestamp","onPageLoad"]

    ncols = length(colnames)

    #println("events=",colnames," nrows=",nrows," ncols=",ncols)

    df = DataFrame(Any,nrows,ncols)
    for i in 1:nrows
        for j in 1:ncols
            df[i, j] = get(eventArray[i],colnames[j],NA)
        end
    end

    df = names!(df,[Symbol("Id"),Symbol("monitor"),Symbol("timestamp"),Symbol("OnPageLoad")])

    sort!(df,cols=[order(:timestamp,rev=false)])

    if SP.debugLevel > 4
        quickTitle("Debug4: Fill New Relic Results")
        beautifyDF(df[:,:],maxRows=500)
    end

    return df

end

function getMonitorsFromTotalResults(SP::ShowParams,totalResultsDict::Dict)

    if SP.debugLevel > 8
        println()
        println("Starting Fill NR Total Results")
        #println("Total Results ",totalResultsDict)
    end

    monitorsDF = DataFrame(name=ASCIIString[],
        oldSizeStdDev=Float64[],oldSizeAvg=Float64[],oldDurationStdDev=Float64[],oldDurationAvg=Float64[],
        newSizeStdDev=Float64[],newSizeAvg=Float64[],newDurationStdDev=Float64[],newDurationAvg=Float64[],
        )

    periodResultsDict = totalResultsDict["current"]
    for monitorDict in periodResultsDict["facets"]
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
        monitorName = monitorDict["name"]

        oldSizeStdDev = monitorDict["results"][1]["standardDeviation"]
        oldSizeAvg = monitorDict["results"][2]["average"]
        oldDurationStdDev = monitorDict["results"][3]["standardDeviation"]
        oldDurationAvg = monitorDict["results"][4]["average"]

        monitorsDF[Bool[x == monitorName for x in monitorsDF[:name]],:oldSizeStdDev] = oldSizeStdDev
        monitorsDF[Bool[x == monitorName for x in monitorsDF[:name]],:oldSizeAvg] = oldSizeAvg
        monitorsDF[Bool[x == monitorName for x in monitorsDF[:name]],:oldDurationStdDev] = oldDurationStdDev
        monitorsDF[Bool[x == monitorName for x in monitorsDF[:name]],:oldDurationAvg] = oldDurationAvg
        #println("Testing ", monitorName,"=",monitorsDF[Bool[x == monitorName for x in monitorsDF[:name]],:name])
        #push!(monitorsDF,[monitorName,            oldSizeStdDev,oldSizeAvg,oldDurationStdDev,oldDurationAvg,            0.0,0.0,0.0,0.0            ])
    end

    sort!(monitorsDF,cols=[order(:name,rev=false)])

    if SP.debugLevel > 6
        beautifyDF(monitorsDF,maxRows=500)
    end

    return monitorsDF
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
        else
            ascHost = ASCIIString(host)
            #println("LookupHost=",ascHost," typeof=",typeof(ascHost))
            #println("ascHost=",ascHost,"lookupHost=",lookupHost(ascHost))
            newHost = lookupHost(SP,ascHost)
            if newHost != "NoneInner"
                NR.results.row[i:i,:host] = newHost
            end
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

    diffDF = DataFrame(host=ASCIIString[],delta=Float64[],old=Float64[],new=Float64[])

    t1 = 0
    for hostT1 in test1DF[:,:host]
        printed = false
        t1 += 1
        sizeT1 = test1DF[t1:t1,:bodySize][1] * 1.0
        durationT1 = test1DF[t1:t1,:duration][1]
        if SP.debugLevel > 6
            println("Outer=",hostT1," s1=",sizeT1," d1=",durationT1)
        end
        t2 = 0
        for hostT2 in test2DF[:,:host]
            t2 += 1
            if hostT1 == hostT2
                sizeT2 = test2DF[t2:t2,:bodySize][1] * 1.0
                durationT2 = test2DF[t2:t2,:duration][1]
                if SP.debugLevel > 6
                    println("       Inner=",hostT1," s2=",sizeT2," d2=",durationT2)
                end
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
                elseif !diffBySize && durationT2 < 250  # 100 ms shift can be ignored
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
                        push!(diffDF,[hostT1,deltaPercent,sizeT1,sizeT2])
                    end
                else
                    deltaPercent = (durationT2-durationT1) / durationT1 * 100.0
                    if !(deltaPercent > -25.0 && deltaPercent < 25.0)
                        push!(diffDF,[hostT1,deltaPercent,durationT1,durationT2])
                    end
                end

                printed = true
                deleterows!(test2DF,t2)
                break
            end
        end

        if !printed
            if SP.debugLevel > 6
                println("Extra1=",hostT1," siz1=", sizeT1," dur1=", durationT1)
            end
            if diffBySize && sizeT1 > 10000
                push!(diffDF,[hostT1,0.0,sizeT1,0.0])
            elseif !diffBySize && durationT1 > 250
                push!(diffDF,[hostT1,0.0,durationT1,0.0])
            end
        end
    end

    if SP.debugLevel > 6
        println()
    end

    t2 = 0
    for hostT2 in test2DF[:,:host]
        t2 += 1
        sizeT2 = test2DF[t2:t2,:bodySize][1] * 1.0
        durationT2 = test2DF[t2:t2,:duration][1]
        if SP.debugLevel > 6
            println("Extra2=",hostT2," siz2=", sizeT2," dur2=", durationT2)
        end
        if diffBySize && sizeT2 > 10000
            push!(diffDF,[hostT2,0.0,0.0,sizeT2])
        elseif !diffBySize && durationT2 > 250
            push!(diffDF,[hostT2,0.0,0.0,durationT2])
        end
    end

    sort!(diffDF,cols=[order(:delta,rev=true),order(:old,rev=true),order(:new,rev=true)])

    if diffBySize
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Size Change"),Symbol("Old Size"),Symbol("New Size")])
    else
        diffDF = names!(diffDF,[Symbol("Web Host"),Symbol("% Duration Change"),Symbol("Old Duration"),Symbol("New Duration")])
    end

    beautifyDF(diffDF,defaultNumberFormat=(:precision => 0, :commas => true))

end

function diffDailyChange(SP::ShowParams,monitorsDF::DataFrame;diffBySize::Bool=true)


    if SP.debugLevel > 8
        println("Starting diffDailyChange")
        beautifyDF(monitorsDF[1:10,:])
    end

    activeMonitorsDF = DataFrame()

    if diffBySize
        activeMonitorsDF = monitorsDF[Bool[x > 0 for x in monitorsDF[:oldSizeStdDev]],:]
        activeMonitorsDF = activeMonitorsDF[Bool[x > 0 for x in activeMonitorsDF[:newSizeStdDev]],:]
    else
        activeMonitorsDF = monitorsDF[Bool[x > 0 for x in monitorsDF[:oldDurationStdDev]],:]
        activeMonitorsDF = activeMonitorsDF[Bool[x > 0 for x in activeMonitorsDF[:newDurationStdDev]],:]
    end

    if SP.debugLevel > 6
        beautifyDF(activeMonitorsDF[1:10,:])
    end

    diffDF = DataFrame(name=ASCIIString[],delta=Float64[],oldStdDev=Float64[],oldAvg=Float64[],newStdDev=Float64[],newAvg=Float64[])

    t1 = 0
    for name in activeMonitorsDF[:,:name]
        t1 += 1
        if diffBySize
            oldStdDev = activeMonitorsDF[t1:t1,:oldSizeStdDev][1]
            oldAvg    = activeMonitorsDF[t1:t1,:oldSizeAvg][1]
            newStdDev = activeMonitorsDF[t1:t1,:newSizeStdDev][1]
            newAvg    = activeMonitorsDF[t1:t1,:newSizeAvg][1]
        else
            oldStdDev = activeMonitorsDF[t1:t1,:oldDurationStdDev][1]
            oldAvg    = activeMonitorsDF[t1:t1,:oldDurationAvg][1]
            newStdDev = activeMonitorsDF[t1:t1,:newDurationStdDev][1]
            newAvg    = activeMonitorsDF[t1:t1,:newDurationAvg][1]
        end

        oldAvgRangeLower = oldAvg - (oldStdDev * CU.howManyStdDev)
        if oldAvgRangeLower < 0
            oldAvgRangeLower = 0
        end
        oldAvgRangeUpper = oldAvg + (oldStdDev * CU.howManyStdDev)

        #println("Name=",name," newAvg=",newAvg," oldAvgRangeLower=",oldAvgRangeLower," oldAvgRangeUpper=",oldAvgRangeUpper)

        if newAvg < oldAvgRangeLower || newAvg > oldAvgRangeUpper
            deltaPercent = (newAvg-oldAvg) / oldAvg * 100.0
            push!(diffDF,[name,deltaPercent,oldStdDev,oldAvg,newStdDev,newAvg])
        end
    end

    sort!(diffDF,cols=[order(:delta,rev=true),order(:oldAvg,rev=true),order(:newAvg,rev=true)])

    if diffBySize
        diffDF = names!(diffDF,[Symbol("Monitor"),Symbol("% Size Change"),Symbol("Old Size StdDev"),Symbol("Old Size"),Symbol("New Size StdDev"),Symbol("New Size")])
    else
        diffDF = names!(diffDF,[Symbol("Monitor"),Symbol("% Duration Change"),Symbol("Old Duration StdDev"),Symbol("Old Duration"),Symbol("New Duration StdDev"),Symbol("New Duration")])
    end

    beautifyDF(diffDF,defaultNumberFormat=(:precision => 0, :commas => true))

end

function diffDailyChangeOnPageLoad(oldTV::TimeVars,newTV::TimeVars,SP::ShowParams,CU::CurlParams,newDF::DataFrame,oldDF::DataFrame)


    if SP.debugLevel > 8
        println("Starting diffDailyChangeOnPageLoad")
        beautifyDF(newDF[1:min(3,end),:])
        beautifyDF(oldDF[1:min(3,end),:])
    end

    dvOld = Array{Float64}(oldDF[:OnPageLoad])
    statsOldDF = basicStatsFromDV(dvOld)
    dvNew = Array{Float64}(newDF[:OnPageLoad])
    statsNewDF = basicStatsFromDV(dvNew)

    oldLower = statsOldDF[1:1,:median][1] - (statsOldDF[1:1,:stddev][1] * CU.howManyStdDev)
    if oldLower < 0
        oldLower = 0
    end
    oldUpper = statsOldDF[1:1,:median][1] + (statsOldDF[1:1,:stddev][1] * CU.howManyStdDev)

    if statsNewDF[1:1,:median][1] > oldLower && statsNewDF[1:1,:median][1] < oldUpper
        if SP.debugLevel > 4
            println("Rejecting oldLower=$oldLower, oldUpper=$oldUpper, new value=",statsNewDF[1:1,:median][1])
        end
        return
    end
    # Figure out if it is worth printing

    quickTitle(ASCIIString(oldDF[1:1,:monitor][1]))

    quickTitle(oldTV.timeString)
    beautifyDF(statsOldDF)

    quickTitle(newTV.timeString)
    beautifyDF(statsNewDF)

    try
        data1=size(oldDF[:OnPageLoad],1)
        data2=size(newDF[:OnPageLoad],1)
        minRows = min(data1,data2)

        #drawDF = DataFrame()

        #drawDF[:col1] = oldDF[:timestamp]
        #drawDF[:data1] = oldDF[:OnPageLoad]
        #drawDF[:data2] = newDF[:OnPageLoad]

        #i = 0
        #for row in eachrow(drawDF)
        #    i += 1
            #println("row ",i, " ",typeof(row[:col1]))
        #    if typeof(row[:col1]) == Int64
        #        row[:col1] = unix2datetime(row[:col1]/1000.0)
        #    end
        #end

        drawDF = DataFrame(col1=DateTime[],data1=Float64[],data2=Float64[])

        for i=1:minRows
            testDT = unix2datetime(oldDF[i:i,:timestamp][1]/1000.0)
            dataPL1 = oldDF[i:i,:OnPageLoad]
            dataPL2 = newDF[i:i,:OnPageLoad]
            push!(drawDF,[testDT;dataPL1;dataPL2])
        end

        axis_x_min = 0
        c3 = drawC3Viz(drawDF; axisLabels=["Date Time"],dataNames=["Old","New"],
                mPulseWidget=false, chart_title= "On Page Load Chart", vizTypes=["line","line"],
                axis_x_min=axis_x_min)
    catch y
        println("diffDailyChangeOnPageLoad Old exception ",y)
    end
end
