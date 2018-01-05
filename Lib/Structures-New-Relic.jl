
type NrMetadata
    #routerGuid"
    #"messages"
    #"rawSince"
    endTime::ASCIIString
    #"eventType"
    #"rawUntil"
    #"guid"
    #"rawCompareWith"
    #"openEnded"
    #"timeSeries"
    #"bucketSizeMillis"
    #"beginTimeMillis"
    beginTime::ASCIIString
    #"eventTypes"
    #"endTimeMillis
end

type NrRunPerf
    #"ioTime"
    inspectedCount::Int64
    #"cacheMisses"
    #"slowLaneFileProcessingTime"
    #"subqueryWeightUpdates"
    wallClockTime::Int64
    #"partialCacheHits"
    #"cacheSkipped"
    #"ioBytes"
    #"sumFileProcessingTimePercentile"
    #"processCount"
    #"rawBytes"
    #"fileReadCount"
    #"sumSubqueryWeightStartFileProcessingTime"
    #"decompressionCachePutTime"
    #"maxInspectedCount"
    #"decompressionTime"
    #"runningQueriesTotal"
    #"decompressionOutputBytes"
    #"minInspectedCount"
    #"decompressedBytes"
    #"mergeTime"
    #"omittedCount"
    #"fileProcessingTime"
    #"responseBodyBytes"
    #"decompressionCacheGetTime"
    #"slowLaneWaitTime"
    #"decompressionCount"
    #"matchCount"
    #"sumSubqueryWeight"
    #"slowLaneFiles"
    #"decompressionCacheEnabledCount"
    #"ignoredFiles"
    #"fullCacheHits"
end

type NrTotal
    inspectedCount::Int64
    endTimeSeconds::Int64
    beginTimeSeconds::Int64
    resultAverage::Float64
end


type NrParams
    totals::NrTotal
    metadata::NrMetadata
    runPerf::NrRunPerf
    totalsAvail::Bool
end

function NrParamsInit()

    totals = NrTotal(0,0,0,0.0)
    metadata = NrMetadata("","")
    runPerf = NrRunPerf(0,0)

    NR = NrParams(totals,metadata,runPerf,false)

    return NR
end
