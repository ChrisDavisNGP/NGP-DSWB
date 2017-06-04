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

type ShowParams
    desktop::Bool
    mobile::Bool
    devView::Bool
    criticalPathOnly::Bool
    debug::Bool
    debugLevel::Int64
end
