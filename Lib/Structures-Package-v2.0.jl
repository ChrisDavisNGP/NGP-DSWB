type UrlParams
    beaconTable::ASCIIString
    btView::ASCIIString
    resourceTable::ASCIIString
    rtView::ASCIIString
    pageGroup::ASCIIString
    urlRegEx::ASCIIString
    urlFull::ASCIIString
    resRegEx::ASCIIString
    timeLowerMs::Int64
    timeUpperMs::Int64
    limitRows::Int64
    samplesMin::Int64
    sizeMin::Int64
    orderBy::ASCIIString
    usePageLoad::Bool
    deviceType::ASCIIString
    agentOs::ASCIIString
end

function UrlParamsInit()
    # Set blank structure and fill later as needed
    UP = UrlParams(table,"",tableRt,"","%","%","","%",1000,600000,0,0,0,"",true,"%","%")
    return UP
end

function UrlParamsPrint(UP::UrlParams)
    println("Tables: bt=",UP.beaconTable," btView=",UP.btView," rt=",UP.resourceTable," rtView=",UP.rtView);
    println("pageGroup=",UP.pageGroup," urlRegEx=",UP.urlRegEx," urlFull=",UP.urlFull," resRegEx=",UP.resRegEx);
    println("timeLowerMs=",UP.timeLowerMs," timeUpperMs=",UP.timeUpperMs," limitRows=",UP.limitRows);
    println("samplesMin=",UP.samplesMin," sizeMin=",UP.sizeMin," orderBy=",UP.orderBy," usePageLoad=",UP.usePageLoad);
    println(" deviceType=",UP.deviceType," agentOS=",UP.agentOs);
end

type ShowParams
    desktop::Bool
    mobile::Bool
    devView::Bool
    criticalPathOnly::Bool
    debug::Bool
    debugLevel::Int64        #debugLevel = 10 # 1 for min output, 5 medium output, 10 all output
    reportLevel::Int64       #reportLevel = 10 # 1 for min output, 5 medium output, 10 all output
    showLines::Int64
    treemapTableLines::Int64
    scrubUrlChars::Int64
end

function ShowParamsInit()
    SP = ShowParams(false,true,false,true,false,0,1,10,20,150)
    return SP
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
