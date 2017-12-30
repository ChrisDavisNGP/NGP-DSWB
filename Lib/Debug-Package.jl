function tvUpSpDumpDebug(TV::TimeVars,UP::UrlParams,SP::ShowParams,msg::ASCIIString)

    if (SP.debugLevel > 8)
        println("\nDebug8: Starting $msg")
        println("Time MS UTC: $(TV.startTimeMsUTC),$(TV.endTimeMsUTC)")
        println("urlRegEx $(UP.urlRegEx)")
        println("dev=$(UP.deviceType), os=$(UP.agentOs), page grp=$(UP.pageGroup)")
        println("time Range: $(UP.timeLowerMs),$(UP.timeUpperMs)")
    end
end

function tableDumpDFDebug(TV::TimeVars,UP::UrlParams,SP::ShowParams,dumpDF::DataFrame)

    if (SP.debugLevel > 8)
        standardChartTitle(TV,UP,SP,"Debug8: Dump All Columns")
        beautifyDF(dumpDF[1:min(3,end),:])
    end
end
