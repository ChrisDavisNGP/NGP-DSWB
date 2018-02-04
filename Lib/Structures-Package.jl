# Gather the notebook name for later

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

    limitRows::Int64         # Trying to retire
    limitQueryRows::Int64
    limitPageViews::Int64

    samplesMin::Int64
    sizeMin::Int64
    orderBy::ASCIIString
    usePageLoad::Bool
    deviceType::ASCIIString
    agentOs::ASCIIString
    useJson::Bool
    jsonFilename::ASCIIString
    whatIf::DataArray
end

function UrlParamsInit(nb::ASCIIString)
    # Set blank structure and fill later as needed
    #nb = getNotebookName()

    btView = "$(table)_$(nb)_pview"
    rtView = "$(tableRt)_$(nb)_pview"
    UP = UrlParams(
        table,btView,tableRt,rtView,
        "%","%","","%",
        2000,60000,
        250,250,250,
        10,10000,"time",true,"%","%",false,"",["whatIf"])

    if isdefined(:UpPageGroup)
        UP.pageGroup = UpPageGroup
    end

    if isdefined(:UpUrlRegEx)
        UP.urlRegEx = UpUrlRegEx
    end

    if isdefined(:UpUrlFull)
        UP.urlFull = UpUrlFull
    end

    if isdefined(:UpResRegEx)
        UP.resRegEx = UpResRegEx
    end

    if isdefined(:UpTimeLowerMs)
        UP.timeLowerMs = UpTimeLowerMs
    end

    if isdefined(:UpTimeUpperMs)
        UP.timeUpperMs = UpTimeUpperMs
    end

    if isdefined(:UpLimitRows)
        UP.limitRows = UpLimitRows
        UP.limitQueryRows = UP.limitRows
        UP.limitPageViews = UP.limitRows
    end

    if isdefined(:UpLimitQueryRows)
        UP.limitQueryRows = UpLimitQueryRows
    end

    if isdefined(:UpLimitPageViews)
        UP.limitPageViews = UpLimitPageViews
    end

    if isdefined(:UpSamplesMin)
        UP.samplesMin = UpSamplesMin
    end

    if isdefined(:UpSizeMin)
        UP.sizeMin = UpSizeMin
    end

    if isdefined(:UpOrderBy)
        UP.orderBy = UpOrderBy
    end

    if isdefined(:UpUsePageLoad)
        UP.usePageLoad = UpUsePageLoad
    end

    if isdefined(:UpDeviceType)
        UP.deviceType = UpDeviceType
    end

    if isdefined(:UpAgentOs)
        UP.agentOs = UpAgentOs
    end

    if isdefined(:UpUseJson)
        UP.useJson = UpUseJson
    end

    if isdefined(:UpJsonFilename)
        UP.jsonFilename = UpJsonFilename
    end

    if isdefined(:UpWhatIf)
        UP.whatIf = UpWhatIf
    end

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
        UP.pageGroup != "Nat Geo Site" &&
        UP.pageGroup != "No Page Group" &&
        UP.pageGroup != "Unknown" &&
        UP.pageGroup != "Adventure AEM" &&
        UP.pageGroup != "Dev-QA-Stage-UAT" &&
        UP.pageGroup != "Environment AEM"
        )
        println("Warning: pageGroup is an unusual value \"",UP.pageGroup,"\", See mPulse All Page Group filter for all values.")
        println("         Common names [News Article|Channel|Kids|Travel AEM|Photography AEM|Nat Get Homepage|Your Shot]")
    end

end

type ShowParams
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
    SP = ShowParams(false,true,false,0,2,25,20,150,75)

    if isdefined(:SpDevView)
        SP.devView = SpDevView
    end

    if isdefined(:SpCriticalPathOnly)
        SP.criticalPathOnly = SpCriticalPathOnly
    end

    if isdefined(:SpDebugLevel)
        SP.debugLevel = SpDebugLevel
    end

    if isdefined(:SpReportLevel)
        SP.reportLevel = SpReportLevel
    end

    if isdefined(:SpShowLines)
        SP.showLines = SpShowLines
    end

    if isdefined(:SpTreemapTableLines)
        SP.treemapTableLines = SpTreemapTableLines
    end

    if isdefined(:SpScrubUrlChars)
        SP.scrubUrlChars = SpScrubUrlChars
    end

    if isdefined(:SpScrubUrlSections)
        SP.scrubUrlSections = SpScrubUrlSections
    end

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
    startTimeStr::ASCIIString
    endTimeStr::ASCIIString
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
    TV = TimeVars(dt,dt,0,0,"","",dt,dt,0,0,:hour,"a","b")
    return TV
end

type CurlParams

    #NR Synthetic series
    synthetic::Bool
    syntheticListAllMonitors::Bool
    syntheticListOneMonitor::Bool
    syntheticBodySize::Bool
    syntheticBodySizeByRequest::Bool
    syntheticMonitorId::ASCIIString
    syntheticMonitor::ASCIIString

    #Compare Points
    oldStart::ASCIIString
    oldEnd::ASCIIString
    newStart::ASCIIString
    newEnd::ASCIIString

    #NR Keys
    apiAdminKey::ASCIIString
    apiQueryKey::ASCIIString
    account::ASCIIString

    #Misc
    howManyStdDev::Int64
    urlRegEx::ASCIIString


    #NR accounts
    #masterAccountId 78783
    #globalSitesAccountId 775840

    #Json values
    jsonFilename::ASCIIString
end

function CurlParamsInit(nb::ASCIIString)

    # Chris' Admin Key in NR

    CU = CurlParams(
        false, false, false, false, false, "no id", "no name",
        "0","0","0","0",
        "b2abadd58593d10bb39329981e8b702d","HFdC9JQE7P3Bkwk9HMl0kgVTH2j5yucx","78783",
        1,"",
        "$nb.json"
    )

    if isdefined(:CuJsonFileName)
        CU.jsonFilename = CuJsonFileName
    end

    if isdefined(:CuSyntheticListAllMonitors)
        CU.syntheticListAllMonitors = CuSyntheticListAllMonitors
    end

    # Typically we should just put in the Monitor ID
    if isdefined(:CuSyntheticListOneMonitor) || isdefined(:CuSyntheticMonitorId)
        CU.syntheticListOneMonitor = true
    end

    if isdefined(:CuSyntheticMonitorId)
        CU.syntheticMonitorId = CuSyntheticMonitorId
    end

    if isdefined(:CuSyntheticMonitor)
        CU.syntheticMonitor = CuSyntheticMonitor
        CU.syntheticMonitor = replace(CU.syntheticMonitor," ","%20")
    end

    if isdefined(:CuOldStart)
        CU.oldStart = CuOldStart
        CU.oldStart = replace(CU.oldStart," ","%20")
        CU.oldStart = replace(CU.oldStart,":","%3A")
    end

    if isdefined(:CuOldEnd)
        CU.oldEnd = CuOldEnd
        CU.oldEnd = replace(CU.oldEnd," ","%20")
        CU.oldEnd = replace(CU.oldEnd,":","%3A")
    end

    if isdefined(:CuNewStart)
        CU.newStart = CuNewStart
        CU.newStart = replace(CU.newStart," ","%20")
        CU.newStart = replace(CU.newStart,":","%3A")
    end

    if isdefined(:CuNewEnd)
        CU.newEnd = CuNewEnd
        CU.newEnd = replace(CU.newEnd," ","%20")
        CU.newEnd = replace(CU.newEnd,":","%3A")
    end

    if isdefined(:CuUrlRegEx)
        CU.urlRegEx = CuUrlRegEx
        CU.urlRegEx = replace(CU.urlRegEx,":","%3A")
        CU.urlRegEx = replace(CU.urlRegEx,"/","%2F")
        CU.urlRegEx = replace(CU.urlRegEx," ","%20")
    end

    if isdefined(:CuSyntheticBodySize)
        CU.syntheticBodySize = CuSyntheticBodySize
    end

    if isdefined(:CuSyntheticBodySizeByRequest)
        CU.syntheticBodySizeByRequest = CuSyntheticBodySizeByRequest
    end

    if isdefined(:CuSynthetic)
        CU.synthetic = true
    end

    if isdefined(:CuHowManyStdDev)
        CU.howManyStdDev = CuHowManyStdDev
    end

    if isdefined(:CuAccount)
        CU.account = CuAccount

        if CU.account == "775840"
            CU.apiQueryKey = "DuGn43YjpU5aZyZ8gWcS5mlz33fBc1Fm"
        end
    end

    # Hard coded keys for now but anyone can add the code for apiAdminKey et al

    return CU

end

function CurlParamsValidate(CU::CurlParams)


    if CU.synthetic && CU.syntheticMonitorId == "no id"
        println("Warning: current code needs a monitor ID to work")
    end

end
