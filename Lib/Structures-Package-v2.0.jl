# Gather the notebook name for later
display("text/html", """<script charset="utf-8">IPython.notebook.kernel.execute('notebook_name = "'+IPython.notebook.notebook_name+'" ');</script>""")

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

function UrlParamsInit(nb::ASCIIString)
    # Set blank structure and fill later as needed
    #nb = getNotebookName()

    btView = "$(table)_$(nb)_pview"
    rtView = "$(tableRt)_$(nb)_pview"
    UP = UrlParams(table,btView,tableRt,rtView,"%","%","","%",1000,600000,0,0,0,"time",true,"%","%")
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

    if (UP.orderBy != "time" && UP.orderBy != "size")
      println("Warning: orderBy unknown value ",UP.orderBy,", [time|size] known values. Continuing")
    end

    # Todo: build a list and list validate routine to validate against
    if (UP.deviceType != "Desktop" && UP.deviceType != "Mobile" && UP.deviceType != "Tablet" && UP.deviceType != "Other" && UP.deviceType != "(No Value)" && UP.deviceType != "%")
      println("Warning: deviceType unknown value ",UP.deviceType,", [Desktop|Mobile|Tablet|Other|(No Value)|%] common values. Continuing")
    end

    # Todo: build a list and list validate routine to validate against
    if (UP.agentOs != "iOS" && UP.agentOs != "Android OS" && UP.agentOs != "Mac OS X" && UP.agentOs != "Windows" && UP.agentOs != "%")
      println("Warning: agentOs unusual value ",UP.agentOs,", [Android OS|iOS|Mac OS X|Winodws|%] are common values. Continuing")
    end

    if (
        UP.pageGroup != "%" &&
        UP.pageGroup != "News Article" &&
        UP.pageGroup != "Channel" &&
        UP.pageGroup != "Kids" &&
        UP.pageGroup != "Travel AEM" &&
        UP.pageGroup != "Photography AEM" &&
        UP.pageGroup != "Magazine AEM" &&
        UP.pageGroup != "Video" &&
        UP.pageGroup != "Your Shot" &&
        UP.pageGroup != "Animal AEM" &&
        UP.pageGroup != "Nat Geo Homepage" &&
        UP.pageGroup != "Environment AEM"
        )
        println("Warning: pageGroup is an unusual value ",UP.pageGroup,", See mPulse All Page Group filter for all values.")
        println("         Common names [News Article|Channel|Kids|Travel AEM|Photography AEM|Nat Get Homepage|Your Shot]")
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
    scrubUrlSections::Int64
end

function ShowParamsInit()
    SP = ShowParams(false,true,false,true,false,0,1,10,20,150,75)
    return SP
end

function ShowParamsValidate(SP::ShowParams)

    SP.debug = false
    if (SP.debugLevel != 0)
      SP.debug = true
    end

    if (SP.debug)
      if (SP.debugLevel < 0 || SP.debugLevel > 10)
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
