type ShowParams
    desktop::Bool
    mobile::Bool
    devView::Bool
    criticalPathOnly::Bool
    debug::Bool
    debugLevel::Int64
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

type UrlParams
    beaconTable::ASCIIString
    resourceTable::ASCIIString
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
