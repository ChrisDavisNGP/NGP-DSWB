
# showAvailableSessions(localTableDF,localTableRtDF,rangeLower,rangeUpper)
# #Individual pages uses the numbers above make the best tree maps
# #Test 1
# studySession = "060212ca-9fdb-4b55-9aa9-b2ff9f6c5032-odv5lh"
# studyTime =  1474476831224;

# Better version using TimeVars structure to pass the time around
function waterFallFinder(table::ASCIIString,studySession::ASCIIString,studyTime::Int64,tv::TimeVars;limit::Int64=30)
    try
        waterfall = query("""\
            select
                page_group,geo_cc,geo_rg, user_agent_os, user_agent_osversion, user_agent_device_type, user_agent_family, user_agent_major
                FROM $table
                where "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
                order by "timestamp" asc
                LIMIT $(limit)
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
        println("showAvailSessions Exception ",y)
    end
end

# Assume TV variable is set
function waterFallFinder(table::ASCIIString,studySession::ASCIIString,studyTime::Int64;limit::Int64=30)
    try
    waterfall = query("""\
        select
            page_group,geo_cc,geo_rg, user_agent_os, user_agent_osversion, user_agent_device_type, user_agent_family, user_agent_major
            FROM $table
            where "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
            order by "timestamp" asc
            LIMIT $(limit)
    """)

    println("Obsolete: Old WaterFallFinder.  See utilities-package.")
    when = msToDateTime(studyTime)

    displayTitle(chart_title = "mPulse Waterfall Finder Assistant", chart_info = [when,"NOTE: Remember to substract 4 hours EDT (5 when EST) from the time as time is in UTC",
        "Use the time range and columns to find your waterfall graph in mPulse.","Session ID is $(studySession)"],showTimeStamp=false)
    display(waterfall)

    catch y
        println("showAvailSessions Exception ",y)
    end
end

function waterFallFinder(table::ASCIIString,studySession::ASCIIString,studyTime::Int64,startTimeMs::Int64,endTimeMs::Int64;limit::Int64=30)
    try
    waterfall = query("""\
        select
            page_group,geo_cc,geo_rg, user_agent_os, user_agent_osversion, user_agent_device_type, user_agent_family, user_agent_major
            FROM $table
            where "timestamp" between $startTimeMs and $endTimeMs and session_id = '$(studySession)' and "timestamp" = '$(studyTime)'
            order by "timestamp" asc
            LIMIT $(limit)
    """)

    when = msToDateTime(studyTime)
        displayTitle(chart_title = "mPulse Waterfall Finder Assistant", chart_info = [when,"NOTE: Remember to substract 4 hours EDT (5 when EST) from the time as time is in UTC",
        "Use the time range and columns to find your waterfall graph in mPulse.","Session ID is $(studySession)"],showTimeStamp=false)
    display(waterfall)

    catch y
        println("showAvailSessions Exception ",y)
    end
end
