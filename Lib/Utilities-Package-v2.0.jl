function bestDatePart(startTime::DateTime, endTime::DateTime, datePart::Symbol)

    try
        datePart = :minute
        dt = endTime-startTime
        deltaTime = convert(Int64,dt)

        if (deltaTime < (6*60*60*1000)+1)
            datePart = :minute
        elseif (deltaTime < (7*24*60*60*1000)+1)
            datePart = :hour
        else
            datePart = :day
        end
    catch y
        println("bestDatepart Exception ",y)
    end
    return datePart
end


#Common numbers that go negitive
#removeNegitiveTime(toppageurl,:Total)
#removeNegitiveTime(toppageurl,:Redirect)
#removeNegitiveTime(toppageurl,:Blocking)
#removeNegitiveTime(toppageurl,:DNS)
#removeNegitiveTime(toppageurl,:TCP)
#removeNegitiveTime(toppageurl,:Request)
#removeNegitiveTime(toppageurl,:Response)


function removeNegitiveTime(inDF::DataFrame,field::Symbol)
# Negitive blocking known

#display(toppageurl[Bool[x < 0 for x in negDf[:blocking]],:])
    i = 0
    for x in inDF[field]
        i += 1
        if x < 0
            #println("row ",i, " field ",field," value ",inDF[i,field])
            inDF[i,field] = 0
        end
    end
end
;

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

dt = DateTime(2000,1,1,1,1)
tv = TimeVars(dt,dt,0,0,dt,dt,0,0,:hour,"a","b")

function timeVariables(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=true
    )
    try
        tv.startTime = DateTime(Y1,M1,D1,H1,MM1)
        tv.endTime = DateTime(Y2,M2,D2,H2,MM2)
        tv.startTimeMs = datetimeToMs(tv.startTime)
        tv.endTimeMs = datetimeToMs(tv.endTime)

        tv.startTimeUTC = datetimeToUTC(tv.startTime, TimeZone("America/New_York"))
        tv.endTimeUTC = datetimeToUTC(tv.endTime, TimeZone("America/New_York"))
        tv.startTimeMsUTC = datetimeToMs(tv.startTimeUTC)
        tv.endTimeMsUTC = datetimeToMs(tv.endTimeUTC)

        tv.datePart = :hour
        tv.datePart = bestDatePart(tv.startTimeUTC,tv.endTimeUTC,tv.datePart)

        tv.timeString = "$(padDateTime(tv.startTime)) to $(padDateTime(tv.endTime)) Local Time"
        tv.timeStringUTC = "$(padDateTime(tv.startTimeUTC)) to $(padDateTime(tv.endTimeUTC)) UTC Time"

        if (showTime)
            println(tv.timeString);
            println(tv.timeStringUTC);
        end

        return;
    catch y
        println("timeVariables Exception ",y)
    end
end

function weeklyTimeVariables(;days::Int64=7)
    try
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2])
        startTime = DateTime(endTime - Day(days))

        timeVariables(
            Dates.year(startTime),
            Dates.month(startTime),
            Dates.day(startTime),
            Dates.hour(startTime),
            Dates.minute(startTime),
            Dates.year(endTime),
            Dates.month(endTime),
            Dates.day(endTime),
            Dates.hour(endTime),
            Dates.minute(endTime)
        );
    catch y
        println("weeklyTimeVariables Exception ",y)
    end
end

# Take 10 hours from yesterday by default
function yesterdayTimeVariables(;startHour::Int64=7,endHour::Int64=17)
    try
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2] - Hour(24-endHour))
        startTime = DateTime(endTime - Hour(endHour-startHour))

        timeVariables(
            Dates.year(startTime),
            Dates.month(startTime),
            Dates.day(startTime),
            Dates.hour(startTime),
            Dates.minute(startTime),
            Dates.year(endTime),
            Dates.month(endTime),
            Dates.day(endTime),
            Dates.hour(endTime),
            Dates.minute(endTime)
        );
    catch y
        println("yesterdayTimeVariables Exception ",y)
    end
end

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
