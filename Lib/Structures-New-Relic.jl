type NrTotal
    inspectedCount::Int64
    endTimeSeconds::Int64
    #results
    beginTimeSeconds::Int64
end


type NrParams
    totals::NrTotal
    totalsAvail::bool
end

function NrParamsInit()

    NR = NrParams()

    return UP
end
