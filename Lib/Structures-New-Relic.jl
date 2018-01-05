
type NrMetadata
    #routerGuid"
    #"messages"
    #"rawSince"
    #"endTime"
    #"eventType"
    #"rawUntil"
    #"guid"
    #"rawCompareWith"
    #"openEnded"
    #"timeSeries"
    #"bucketSizeMillis"
    #"beginTimeMillis"
    #"beginTime"
    #"eventTypes"
    #"endTimeMillis
end

type NrRunPerf
    #"ioTime"
    #"inspectedCount"
    #"cacheMisses"
    #"slowLaneFileProcessingTime"
    #"subqueryWeightUpdates"
    #"wallClockTime"
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
    totalsAvail::Bool
end

function NrParamsInit()

    totals = NrTotal(0,0,0,0.0)
    NR = NrParams(totals,false)

    return NR
end
