
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

        whenUTC = msToDateTime(studyTime)
        whenUTCz = ZonedDateTime(whenUTC,TimeZone("UTC"))
        when = astimezone(whenUTCz,TimeZone("America/New_York"))
        #whenString = "$(when) Local Time"
        whenString = string(monthname(Dates.month(when))," ",Dates.day(when),", ",Dates.year(when)," ",
        Dates.hour(when),":",Dates.minute(when)," Local Time")

        displayTitle(chart_title = "mPulse Waterfall Finder", chart_info = [whenString,
            "Use the time range and columns below to find your waterfall graph in mPulse.","Session ID is $(studySession)"],showTimeStamp=false)

        waterfall = names!(waterfall[:,:],
        [symbol("Page Group"),symbol("Country"),symbol("Region"),symbol("OS Family"),symbol("OS Version"),symbol("Device Type"),
            symbol("Browser Family"),symbol("Browser Version")]);

        display(waterfall)
        #println("studyTime ",studyTime," local ",whenString)

    catch y
        println("waterFallFinder Exception ",y)
    end
end
