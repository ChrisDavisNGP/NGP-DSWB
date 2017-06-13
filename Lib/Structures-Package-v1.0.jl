type UrlParams
    beaconTable::ASCIIString
    btView::ASCIIString
    resourceTable::ASCIIString
    rtView::ASCIIString
    pageGroup::ASCIIString
    urlRegEx::ASCIIString
    urlFull::ASCIIString
    timeLowerMs::Float64
    timeUpperMs::Float64
    limitRows::Int64
    samplesMin::Int64
    sizeMin::Int64
    orderBy::ASCIIString
    usePageLoad::Bool
    deviceType::ASCIIString
end

function UrlParamsInit()
    # Set blank structure and fill later as needed
    UP = UrlParams("","","","","","","",0.0,0.0,0,0,0,"",true,"")
    return UP
end

type ShowParams
    desktop::Bool
    mobile::Bool
    devView::Bool
    criticalPathOnly::Bool
    debug::Bool
    debugLevel::Int64
end

function ShowParamsInit()
    SP = ShowParams(false,true,false,true,false,0)
    return SP
end

type SoastaGraphs
    customer::ASCIIString
end

function SoastaGraphsInit()
    SG = SoastaGraphs("")
    return SG
end

type TimeVars
    startTime::DateTime
    endTime::DateTime
    startTimeMs::Int64
    endTimeMs::Int64
    startTimeUTC::DateTime
    endTimeUTC::DateTime
    startTimeMsUTC::Int64
    endTimeMsUTC::Int64
    datePart::Symbol
    timeString::ASCIIString
    timeStringUTC::ASCIIString
end

function TimeVarsInit()
    dt = DateTime(2000,1,1,1,1)
    TV = TimeVars(dt,dt,0,0,dt,dt,0,0,:hour,"a","b")
    return TV
end