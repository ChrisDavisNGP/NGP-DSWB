
# Display 10 rows of DataFrame
function display10Rows(df::DataFrame, nameArr::Array)
    beautifyDF(names!(df[1:min(10, end),[1:3;]],nameArr))
end

function displayManyRows(df::DataFrame, nameArr::Array, limit::Int64)
    beautifyDF(names!(df[1:min(limit, end),[1:3;]],nameArr))
end

function deviceTypeTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        fieldNames = [:user_agent_device_type]
        treeData = getTreemapData(TV.startTimeUTC, TV.endTimeUTC, fieldNames = fieldNames)
        treeData[:x1] = "Natgeo - All"
        displayTitle(chart_title = "Device Type for Page Group: $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        drawTree(treeData; titleCol = :x1, fieldNames = fieldNames)

        if (SP.devView)
            # Format beacon output
            sort!(treeData, cols=:beacons, rev=true)
            displayTitle(chart_title = "Device Type for Page Group: $(UP.pageGroup)", chart_info = ["Highest Beacon Counts and Load Time"],showTimeStamp=false)
            # Keep rows with beacon count > 500
            # treeData = treeData[treeData[:beacons].>499,:]
            displayManyRows(treeData[:,1:3], [Symbol("User Agent Family"), Symbol("Load Time"), Symbol("Beacons")],SP.treemapTableLines)
        end

    catch y
        println("deviceTypeTreemap Exception ",y)
    end
end

function pageGroupTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        treeData = getTreemapData(TV.startTimeUTC, TV.endTimeUTC, fieldNames = [:page_group])
        treeData[:x1] = "Natgeo - All"
        chartTile = "$(UP.pageGroup) Page Group"
        if UP.pageGroup == "%"
            chartTile = "All Page Groups"
        end
        displayTitle(chart_title = chartTile, chart_info = [TV.timeString],showTimeStamp=false)
        drawTree(treeData; titleCol = :x1, fieldNames = [:page_group])

        if (SP.devView)
            sort!(treeData, cols=:beacons, rev=true)
            displayTitle(chart_title = chartTile, chart_info = ["Highest Beacon Counts and Load Times"],showTimeStamp=false)
            # Keep rows with beacon count > 500
            treeData = treeData[treeData[:beacons].>499,:]
            displayManyRows(treeData[:,1:3], [Symbol("Page Group"), Symbol("Load Time"), Symbol("Beacons")],SP.treemapTableLines)
        end

        treeData = getTreemapData(TV.startTimeUTC, TV.endTimeUTC, fieldNames = [:user_agent_device_type,:page_group])

        if (UP.deviceType == "Desktop" || UP.deviceType == "%")
            subTreeData = treeData[treeData[:, :user_agent_device_type] .== "Desktop", :]
            subTreeData[:x1] = "Natgeo - Desktop"
            displayTitle(chart_title = "Page Group - Desktop", chart_info = [TV.timeString],showTimeStamp=false)
            drawTree(subTreeData; titleCol = :x1, fieldNames = [:page_group])
            if (SP.devView)
                sort!(subTreeData, cols=:beacons, rev=true)
                displayTitle(chart_title = "Desktop", chart_info = ["Highest Beacon Counts and Load Times"],showTimeStamp=false)
                # Keep rows with beacon count > 499
                subTreeData = subTreeData[subTreeData[:beacons].>499,:]
                displayManyRows(subTreeData[:,2:4], [Symbol("Page Group");Symbol("Load Time");Symbol("Page Views")],SP.treemapTableLines)
            end
        end

        if (UP.deviceType == "Mobile" || UP.deviceType == "%")
            subTreeData = treeData[treeData[:, :user_agent_device_type] .== "Mobile", :]
            subTreeData[:x1] = "Natgeo - Mobile"
            displayTitle(chart_title = "Mobile", chart_info = [TV.timeString],showTimeStamp=false)
            drawTree(subTreeData; titleCol = :x1, fieldNames = [:page_group])
            if (SP.devView)
                sort!(subTreeData, cols=:beacons, rev=true)
                displayTitle(chart_title = "Mobile", chart_info = ["Highest Beacon Counts and Load Times"],showTimeStamp=false)
                # Keep rows with beacon count > 499
                subTreeData = subTreeData[subTreeData[:beacons].>499,:]
                displayManyRows(subTreeData[:,2:4], [Symbol("Page Group");Symbol("Load Time");Symbol("Page Views")],SP.treemapTableLines)
            end
        end
    catch y
        println("pageGroupTreemap Exception ",y)
    end
end

function browserFamilyTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        treeData = getTreemapData(TV.startTimeUTC, TV.endTimeUTC, fieldNames = [:user_agent_family])
        treeData[:x1] = "Natgeo - All"
        displayTitle(chart_title = "Browser Family for Page Group: $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        drawTree(treeData; titleCol = :x1, fieldNames = [:user_agent_family])

        # Format beacon output
        sort!(treeData, cols=:beacons, rev=true)
        displayTitle(chart_title = "Browser Family for Page Group: $(UP.pageGroup)", chart_info = ["Highest Beacon Counts and Load Time"],showTimeStamp=false)

        # Keep rows with beacon count > 500
        # treeData = treeData[treeData[:beacons].>499,:]
        displayManyRows(treeData[:,[1:3;]], [Symbol("User Agent Family"), Symbol("Load Time"), Symbol("Beacons")],SP.treemapTableLines)
    catch y
        println("browserFamilyTreemap Exception ",y)
    end
end

function countryTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        treeData = getTreemapData(TV.startTimeUTC, TV.endTimeUTC, fieldNames = [:geo_cc])
        treeData[:x1] = "Natgeo - All"
        displayTitle(chart_title = "Countries for Page Group: $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        drawTree(treeData; titleCol = :x1, fieldNames = [:geo_cc])

        # Format beacon output
        sort!(treeData, cols=:beacons, rev=true)
        displayTitle(chart_title = "Countries for Page Group: $(UP.pageGroup)", chart_info = ["Highest Beacon Counts and Load Time"],showTimeStamp=false)

        # Translate region abbreviations to names
        cc = getMapCountryNames()
        countries = DataArray([])
        for x in eachrow(treeData)
            countries = vcat(countries,cc[x[:geo_cc]])
        end

        treeData[:geo_cc] = countries
        displayManyRows(treeData[:,[1:3;]], [Symbol("Countries"), Symbol("Load Time"), Symbol("Beacons")],SP.treemapTableLines)
    catch y
        println("countryTreemap Exception ",y)
    end
end

function removeNotBlocking(localDF::DataFrame)
    try
        #watch for non-deep copies such as the functions below
        i = 1
        for x in localDF[:,:urlgroup]
            if x == "Not Blocking"
                deleterows!(localDF,i)
            end
            i += 1
        end
    catch y
        println("notBlocking Exception ",y)
    end
end

function notBlocking(localDF::DataFrame)
    try
        #watch for non-deep copies such as the functions below
        #i = 1
        #for x in localDF[:,:urlgroup]
        #    if x == "Not Blocking"
        #        deleterows!(localDF,i)
        #    end
        #    i += 1
        #end
        removeNotBlocking(localDF);

        push!(localDF,["Not Blocking",999999999,0,0,0,0,0,0,0,0,0,"Not Blocking",1,"Label",0,0])
    catch y
        println("notBlocking Exception ",y)
    end
end

function bodyTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame,beaconString::ASCIIString;showPageUrl::Bool=false,showTreemap::Bool=true)
    try
        totalTime = sum(toppageurl[:,:Total])
        currentTime = sum(toppageurl[:,:beacons])
        if currentTime > 0
            titlestring = "This includes time which is overlapped but does not include the gaps."
            title2string = "Note: beacons field is used for load time"
            displayTitle(chart_title = "$(beaconString) Times (K ms) For Page", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

            notBlocking(toppageurl);
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:urlpagegroup] = "Not $(beaconString)"
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:Critical] = 0
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:beacons] = totalTime - currentTime

            treeDF = DataFrame()
            treeDF[:,:urlpagegroup] = toppageurl[:,:urlpagegroup]
            treeDF[:,:beacons] = toppageurl[:,:beacons]
            treeDF[:,:label] = toppageurl[:,:label]
            treeDF[:,:load_time] = toppageurl[:,:load_time]

            fieldNames = [:urlpagegroup]
            treeDF[:label] = "$(beaconString) Time"
            if (showTreemap)
                drawTree(treeDF; titleCol=:label, fieldNames=fieldNames,resourceColors=true)
            end
            removeNotBlocking(toppageurl)

            if (SP.devView)
                list = DataFrame()
                list[:,:urlpagegroup] = deepcopy(toppageurl[:,:urlpagegroup])
                list[:,:Total] = deepcopy(toppageurl[:,:Total])
                list[:,:beacons] = deepcopy(toppageurl[:,:beacons])
                if (showPageUrl)
                    list[:,:urlgroup] = deepcopy(toppageurl[:,:urlgroup])
                end

                totalPercentTime = sum(list[:,:beacons]) * 0.010
                sort!(list,cols=[order(:beacons,rev=true),order(:Total,rev=true)])

                totalPercentTime = list[1:1,:beacons] * 0.1
                list = list[Bool[x > totalPercentTime[1] for x in list[:beacons]],:]
                if (showPageUrl)
                    map!(x->replace(x,"%","\%"),list[:,:urlgroup])
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                    [Symbol("URL Page Group"),Symbol("Total Time"),Symbol(beaconString),Symbol("Url Without Params")]))
                else
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                    [Symbol("URL Page Group"),Symbol("Total Time"),Symbol(beaconString)]))
                end
            end
        else
            println("No $(beaconString) time.  Output nothing in report")
        end
    catch y
        println("bodyTreemap Exception ",y)
    end
end

function gapTreemapV2(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false,showTreemap::Bool=true)
    try
        #beacons on Blocking
        beaconString = "Gap"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("beacons"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl,showTreemap=showTreemap)

    catch y
        println("gapTreemapV2 Exception ",y)
    end
end

function blockingTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false,showTreemap::Bool=true)
    try
        #beacons on Blocking
        beaconString = "Blocking"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("beacons"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl,showTreemap=showTreemap)

    catch y
        println("blockingTreemap Exception ",y)
    end
end

function dnsTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on DNS
        beaconString = "DNS"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("beacons"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl)

    catch y
        println("dnsTreemap Exception ",y)
    end
end

function redirectTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on Redirect
        beaconString = "Redirect"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("beacons"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl)

    catch y
        println("redirectTreemap Exception ",y)
    end
end

function requestTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on Request
        beaconString = "Request"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("beacons"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl)

    catch y
        println("requestTreemap Exception ",y)
    end
end

function responseTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on Response
        beaconString = "Response"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("beacons"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl)

    catch y
        println("responseTreemap Exception ",y)
    end
end

function tcpTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on TCP
        beaconString = "TCP"
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("beacons"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        bodyTreemap(TV,UP,SP,toppageurl,beaconString;showPageUrl=showPageUrl)

    catch y
        println("tcpTreemap Exception ",y)
    end
end

# Critical Path Display
function criticalPathTreemapV2(TV::TimeVars,UP::UrlParams,SP::ShowParams,labelField::ASCIIString,toppageurl::DataFrame)
    try
        #beacons on Critical
        toppageurl = names!(toppageurl[:,:],
            [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("beacons"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        notBlocking(toppageurl);
        toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:urlpagegroup] = "Time Waiting And/Or Executing Browser Side Code"
        toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:Gap] = 0  # Nice shade of red for waiting time
        toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:beacons] = sum(toppageurl[:,:Gap])

        treeDF = DataFrame()
        treeDF[:,:urlpagegroup] = toppageurl[:,:urlpagegroup]
        treeDF[:,:beacons] = toppageurl[:,:beacons]                      # Sum of all critical path numbers
        treeDF[:,:label] = toppageurl[:,:label]                          # Median of all load times in MS
        treeDF[:,:load_time] = toppageurl[:,:load_time]

        #display(treeDF[1:3,:])
        displayTitle(chart_title = "$labelField",showTimeStamp=false)
        fieldNames = [:urlpagegroup]
        treeDF[:label] = "Critical Path"
        drawTree(treeDF; titleCol=:label, fieldNames=fieldNames,resourceColors=true)
        removeNotBlocking(toppageurl)

        if (SP.devView)
            currentTime = sum(toppageurl[:,:beacons])
            if currentTime > 0
                list = DataFrame()
                list[:,:urlpagegroup] = deepcopy(toppageurl[:,:urlpagegroup])
                list[:,:Total] = deepcopy(toppageurl[:,:Total])
                list[:,:Critical] = deepcopy(toppageurl[:,:beacons])
                list[:,:urlgroup] = deepcopy(toppageurl[:,:urlgroup])

                totalPercentTime = sum(list[:,:Critical]) * 0.010
                sort!(list,cols=[order(:Critical,rev=true),order(:Total,rev=true)])
                displayTitle(chart_title = "Top Times By Critical Path Time (ms)",showTimeStamp=false)
                totalPercentTime = list[1:1,:Critical] * 0.1
                #Skip percent check
                #list = list[Bool[x > totalPercentTime[1] for x in list[:Critical]],:]
                beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                    [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Critical Path"),Symbol("Url Without Params")]))
            end
        end
    catch y
        println("criticalPathTreemapV2 Exception ",y)
    end
end

function endToEndTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #beacons on Total
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("beacons"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        #totalTime = sum(toppageurl[:,:Total])
        currentTime = sum(toppageurl[:,:beacons])
        totalTime = currentTime
        if currentTime > 0
            notBlocking(toppageurl);
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:urlpagegroup] = "Not End To End Time"
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:Critical] = 0
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:beacons] = totalTime - currentTime

            treeDF = DataFrame()
            treeDF[:,:urlpagegroup] = toppageurl[:,:urlpagegroup]
            treeDF[:,:beacons] = toppageurl[:,:beacons]
            treeDF[:,:label] = toppageurl[:,:label]
            treeDF[:,:load_time] = toppageurl[:,:load_time]

            fieldNames = [:urlpagegroup]
            treeDF[:label] = "End to End Time"
            drawTree(treeDF; titleCol=:label, fieldNames=fieldNames,resourceColors=true)
            removeNotBlocking(toppageurl)

            if (SP.devView)
                list = DataFrame()
                list[:,:urlpagegroup] = deepcopy(toppageurl[:,:urlpagegroup])
                #list[:,:Total] = deepcopy(toppageurl[:,:Total])
                list[:,:beacons] = deepcopy(toppageurl[:,:beacons])
                if (showPageUrl)
                    list[:,:urlgroup] = deepcopy(toppageurl[:,:urlgroup])
                end

                totalPercentTime = sum(list[:,:beacons]) * 0.010
                sort!(list,cols=[order(:beacons,rev=true)])

                titlestring = "This includes time which is overlapped."
                title2string = "Note: beacons field is used for load time and load_time field is used fractional load time"
                displayTitle(chart_title = "Total Time (K ms) For All Pages In Sample", chart_info = [titlestring,title2string,TV.timeString],showTimeStamp=false)

                #totalPercentTime = list[1:1,:beacons] * 0.001
                totalPercentTime = 1
                list = list[Bool[x > totalPercentTime[1] for x in list[:beacons]],:]
                if (showPageUrl)
                    #beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                    #    [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Redirect"),Symbol("Url Without Params")]))
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                        [Symbol("URL Page Group"),Symbol("End to End Time"),Symbol("Url Without Params")]))
                else
                    #beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                    #    [Symbol("URL Page Group"),Symbol("Total Time"),Symbol("Redirect"),Symbol("Url Without Params")]))
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                        [Symbol("URL Page Group"),Symbol("End to End Time")]))
                end
            end
        end
    catch y
        println("endToEndTreemap Exception ",y)
    end
end

function itemCountTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,toppageurl::DataFrame;showPageUrl::Bool=false)
    try
        #Note: This one is not time based.  Do not use this one as a template for others

        #beacons on request_count
        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("beacons"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")])

        currentTime = sum(toppageurl[:,:beacons])
        if currentTime > 0
            notBlocking(toppageurl);
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:urlpagegroup] = "Not Request Count"
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:Critical] = 0
            toppageurl[toppageurl[:,:urlgroup] .== "Not Blocking",:beacons] = 0

            treeDF = DataFrame()
            treeDF[:,:urlpagegroup] = toppageurl[:,:urlpagegroup]
            treeDF[:,:beacons] = toppageurl[:,:beacons]
            treeDF[:,:label] = toppageurl[:,:label]
            treeDF[:,:load_time] = toppageurl[:,:load_time]

            #display(treeDF[1:3,:])
            fieldNames = [:urlpagegroup]
            treeDF[:label] = "Request Count"
            drawTree(treeDF; titleCol=:label, fieldNames=fieldNames,resourceColors=true)
            removeNotBlocking(toppageurl)

            if (SP.devView)
                list = DataFrame()
                list[:,:urlpagegroup] = deepcopy(toppageurl[:,:urlpagegroup])
                list[:,:request_count] = deepcopy(toppageurl[:,:beacons])
                if (showPageUrl)
                    list[:,:urlgroup] = deepcopy(toppageurl[:,:urlgroup])
                end

                totalPercentTime = sum(list[:,:request_count]) * 0.010
                sort!(list,cols=[order(:request_count,rev=true)])
                displayTitle(chart_title = "Top Counts By Request Count",showTimeStamp=false)
                totalPercentTime = list[1:1,:request_count] * 0.1
                list = list[Bool[x > totalPercentTime[1] for x in list[:request_count]],:]
                if (showPageUrl)
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                        [Symbol("URL Page Group"),Symbol("Request Count"),Symbol("Url Without Params")]))
                else
                    beautifyDF(names!(list[1:min(SP.treemapTableLines,end),:],
                        [Symbol("URL Page Group"),Symbol("Request Count")]))
                end
            end
        end
    catch y
        println("itemCountTreemap Exception ",y)
    end
end

function criticalPathFinalTreemap(TV::TimeVars,UP::UrlParams,SP::ShowParams,criticalPathDF::DataFrame)
    try

        if (SP.debugLevel > 8)
            standardChartTitle(TV,UP,SP,"Debug8: Critical Path DF")
            beautifyDF(criticalPathDF)
        end

        #beacons on Total
        cpDF = names!(criticalPathDF[:,:],
        [Symbol("urlgroup"),Symbol("average"),Symbol("maximum"),Symbol("counter"),Symbol("label")])

        totalAverage = sum(cpDF[:,:average])
        
        treeDF = DataFrame()
        treeDF[:,:urlgroup] = cpDF[:,:urlgroup]
        treeDF[:,:beacons] = cpDF[:,:average]
        treeDF[:,:label] = cpDF[:,:label]
        #treeDF[:,:load_time] = cpDF[:,:counter]/100.0
        treeDF[:,:load_time] = (cpDF[:,:average]/totalAverage) * 2 # Color on percents assuming most will be under 50%

        fieldNames = [:urlgroup]
        treeDF[:label] = "Critical Path Summary"
        drawTree(treeDF; titleCol=:label, fieldNames=fieldNames,resourceColors=true)
        #drawTreev2(treeDF,treeDF[:label],10)

        if (SP.devView)
            list = DataFrame()
            list[:,:urlgroup] = deepcopy(cpDF[:,:urlgroup])
            list[:,:beacons] = deepcopy(cpDF[:,:average])
            list[:,:maximum] = deepcopy(cpDF[:,:maximum])
            list[:,:counter] = deepcopy(cpDF[:,:counter])

            totalPercentTime = sum(list[:,:beacons]) * 0.010
            sort!(list,cols=[order(:beacons,rev=true)])

            standardChartTitle(TV,UP,SP,"Average Time (ms) For All Pages In Sample")

            totalPercentTime = 1
            list = list[Bool[x > totalPercentTime[1] for x in list[:beacons]],:]
            beautifyDF(names!(list[:,:],
                [Symbol("URL Page Group");Symbol("Average Time (ms)");Symbol("Maximum");Symbol("Occurances")]))
        end

    catch y
        println("criticalPathFinalTreemap Exception ",y)
    end
end

function urlPageTreemapsAllBody(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    #studySession::ASCIIString
    #studyTime::Int64

    localTableDF = defaultBeaconsToDF(TV,UP,SP)
    println("$table count is ",size(localTableDF))

    localTableRtDF = treemapsLocalTableRtToDF(TV,UP,SP)
    println("$tableRt count is ",size(localTableRtDF))

    setRangeUPT(TV,UP,SP,localTableDF)

    showAvailableSessions(UP,SP,localTableDF,localTableRtDF)

    # Individual pages uses the numbers above make the best tree maps

    # Test 1
    studySession = gStudySession
    studyTime =  gStudyTime

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

    if size(toppageurl,1) == 0
        println("No data found")
        return
    end

    findTopPageViewUPT(TV,UP,SP,toppageurl)

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

    if (SP.reportLevel > 11)
        if (studyTime > 0)
            topdetailurl = query("""\
            select CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
            CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END as load_time,
            CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END as beacons,
            1 as request_count
            FROM $(tableRt) where session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
            """);
        elseif (studySession != "None")
            topdetailurl = query("""\
            select CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
            avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as load_time,
            avg(CASE WHEN (response_last_byte = 0) THEN (0) ELSE (response_last_byte-start_time) END) as beacons,
            count(*) as request_count
            FROM $(localTableRt) where session_id = '$(studySession)'
            group by urlgroup
            """);
        else
            topdetailurl = query("""\
            select CASE WHEN (position('?' in url) > 0) then trim('/' from (substring(url for position('?' in substring(url from 9)) +7))) else trim('/' from url) end as urlgroup,
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

end
