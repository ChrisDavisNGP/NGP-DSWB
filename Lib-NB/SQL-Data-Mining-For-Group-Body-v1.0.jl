type LocalVars
    linesOutput::Int64
end

function displayGroup(TV::TimeVars,UP::UrlParams,LV::LocalVars)
    try
        currentPageGroupDF = groupSamplesTableDF(UP.beaconTable,UP.pageGroup)
        #println("$pageGroup Beacons: ",size(currentPageGroupDF)[1])

        finalPrintDF = DataFrame(count=Int64[],url=ASCIIString[],params_u=ASCIIString[])

        for subDF in groupby(currentPageGroupDF,[:url,:params_u])
            currentGroup = subDF[1:1,:url]
            currentParams = subDF[1:1,:params_u]
            #println(size(subDF,1),"  ",currentGroup[1],"  ",currentParams[1])
            push!(finalPrintDF,[size(subDF,1);subDF[1:1,:url];subDF[1:1,:params_u]])
        end

        displayTitle(chart_title = "Top Beacons Counts (limit $(LV.linesOutput)) For $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        sort!(finalPrintDF, cols=(order(:count, rev=true)))
        beautifyDF(finalPrintDF[1:min(LV.linesOutput,end),:])        
    catch y
        println("displayGroup Exception ",y)
    end
end    