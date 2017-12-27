
# #Individual pages uses the numbers above make the best tree maps
# #Test 1
# studySession = "060212ca-9fdb-4b55-9aa9-b2ff9f6c5032-odv5lh"
# studyTime =  1474476831224;

# Better version using TimeVars structure to pass the time around
function waterFallFinder(TV::TimeVars,UP::UrlParams,SP::ShowParams,studySession::ASCIIString,studyTime::Int64)
    try
        bt = UP.beaconTable

        waterfall = query("""\
            select
                page_group,geo_cc,geo_rg, user_agent_os, user_agent_osversion, user_agent_device_type, user_agent_family, user_agent_major
            FROM $bt
            where
               "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
            order by "timestamp" asc
            LIMIT $(SP.showLines)
        """)

        if size(waterfall,1) == 0
            if SP.debugLevel > 0
                println("Waterfall not found.  Make sure your time range matches you single session if you are doing manual traces")
            end
            return
        end

        whenUTC = msToDateTime(studyTime)
        whenUTCz = ZonedDateTime(whenUTC,TimeZone("UTC"))
        when = astimezone(whenUTCz,TimeZone("America/New_York"))
        #whenString = "$(when) Local Time"
        whenString = string(monthname(Dates.month(when))," ",Dates.day(when),", ",Dates.year(when)," ",
        Dates.hour(when),":",Dates.minute(when)," Local Time")

        displayTitle(chart_title = "mPulse Waterfall Finder", chart_info = [whenString,
            "Use the time range and columns below to find your waterfall graph in mPulse.","Session ID is $(studySession)"],showTimeStamp=false)

        waterfall = names!(waterfall[:,:],
        [Symbol("Page Group"),Symbol("Country"),Symbol("Region"),Symbol("OS Family"),Symbol("OS Version"),Symbol("Device Type"),
            Symbol("Browser Family"),Symbol("Browser Version")]);

        display(waterfall)
        #println("studyTime ",studyTime," local ",whenString)

    catch y
        println("waterFallFinder Exception ",y)
    end
end

function getNotebookName()
    #whos(r"^n.*")
    #?setenv
    #display(ENV["PATH"])
    #display(ENV)
    #display(notebook_name)
    #display("text/html", """<script charset="utf-8">IPython.notebook.kernel.execute('notebook_name = "'+IPython.notebook.notebook_name+'" ');</script>""")
    if (notebook_name == "")
        println("problems with notebook_name javascript.  See top of Structures.jl.")
        notebook_name = "default_name"
    end
    nb = replace(notebook_name,"\.ipynb","")
    nb = replace(nb,"-","_")
    #display(nb)
    return nb
end

function openingTitle(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    chartTitle = "Report Paramaters:"
    chartTitle *= " Page Group=$(UP.pageGroup), DeviceType=$(UP.deviceType), Browser OS=$(UP.agentOs)"
    chartInfo  = "Other Settings: limitRows=$(UP.limitRows),time range ms=($(UP.timeLowerMs),$(UP.timeUpperMs))"
    chartInfoOptional = ""
    if UP.urlRegEx != "%" || UP.urlFull != "%" || UP.resRegEx != "%"
        chartInfoOptional = "urlRegEx=$(UP.urlRegEx)\nurlFull=$(UP.urlFull)\nresRegEx=$(UP.resRegEx)"
    end

    displayTitle(chart_title = chartTitle, chart_info = [TV.timeString;chartInfo;chartInfoOptional], showTimeStamp=false)

end

function standardChartTitle(TV::TimeVars,UP::UrlParams,SP::ShowParams,openingString::ASCIIString)
    chartTitle = openingString
    chartTitle *= " ($(UP.pageGroup),$(UP.deviceType),$(UP.agentOs))"

    if (SP.debugLevel > 4)
        chartInfo  = "Other Settings: limitRows=$(UP.limitRows),time range ms=($(UP.timeLowerMs),$(UP.timeUpperMs))"
        chartInfo2 = "urlRegEx=$(UP.urlRegEx)"
        chartInfo3 = "urlFull=$(UP.urlFull)"
        displayTitle(chart_title = chartTitle, chart_info = [TV.timeString;chartInfo;chartInfo2;chartInfo3], showTimeStamp=false)
    else
        displayTitle(chart_title = chartTitle, chart_info = [TV.timeString], showTimeStamp=false)
    end
end
