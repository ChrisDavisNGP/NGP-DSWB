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
