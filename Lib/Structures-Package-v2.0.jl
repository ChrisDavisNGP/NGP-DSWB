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

# Look for known bad values.  Case sensitive values
function UrlParamsValidate(UP::UrlParams)

    if (UP.orderBy != "time" or UP.orderBy != "size")
      println("Warning: orderBy unknown value ",UP.orderBy," [time|size] known values. Continuing")
    end

    # Todo: build a list and list validate routine to validate against
    if (UP.deviceType != "Desktop" or UP.deviceType != "Mobile" or UP.deviceType != "Tablet" or UP.deviceType != "Other" or UP.deviceType != "(No Value)" or UP.deviceType != "%")
      println("Warning: deviceType unknown value ",UP.deviceType," [Desktop|Mobile|Tablet|Other|(No Value)|%] common values. Continuing")
    end

    # Todo: build a list and list validate routine to validate against
    if (UP.agentOs != "iOS" or UP.agentOs != "Android OS" or UP.agentOs != "Mac OS X" or UP.agentOs != "Windows" or UP.agentOs != "%")
      println("Warning: agentOs unusual value ",UP.agentOs," [Android OS|iOS|Mac OS X|Winodws|%] are common values. Continuing")
    end

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

function ShowParamsValidate(SP::ShowParams)

    if (!SP.debug and SP.debugLevel > 0)
      println("Warning: debug flag off but debugLevel greater than 0. Continuing")
    end

    if (SP.debug)
      if (SP.debugLevel < 0 or SP.debugLevel > 10)
        println("Warning: debugLevel value ",SP.debugLevel," outside 0 to 10. Continuing")
      end
    end      

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
