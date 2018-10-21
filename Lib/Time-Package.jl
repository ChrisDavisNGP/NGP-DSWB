# Packages in Lib Directory; structures first
function bestDatePart(start_time::DateTime, endTime::DateTime)

    try
        datePart = :minute
        dt = endTime-start_time
        deltaTime = convert(Int64,dt)
        minuteLimit = (10*60*60*1000)+1
        hourLimit = (7*24*60*60*1000)+1
        #println("starttime=",start_time," endtime=",endTime," deltaTime =",deltaTime," minuteLimit=",minuteLimit," hourLimit=",hourLimit)

        if (deltaTime < minuteLimit)
            datePart = :minute
        elseif (deltaTime < hourLimit)
            datePart = :hour
        else
            datePart = :day
        end

        return datePart

    catch y
        println("bestDatepart Exception ",y)
    end
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



function timeVariables(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=false
    )
    try
        tempTime = TimeVarsInit()

        tempTime.start_time = DateTime(Y1,M1,D1,H1,MM1)
        tempTime.endTime = DateTime(Y2,M2,D2,H2,MM2)
        tempTime.startTimeMs = datetimeToMs(tempTime.start_time)
        tempTime.endTimeMs = datetimeToMs(tempTime.endTime)
        tempTime.startTimeStr = Dates.format(tempTime.start_time,"yyyy-mm-dd HH:MM:SS")
        tempTime.endTimeStr = Dates.format(tempTime.endTime,"yyyy-mm-dd HH:MM:SS")

        tempTime.startTimeUTC = datetimeToUTC(tempTime.start_time, TimeZone("America/New_York"))
        tempTime.endTimeUTC = datetimeToUTC(tempTime.endTime, TimeZone("America/New_York"))
        tempTime.startTimeMsUTC = datetimeToMs(tempTime.startTimeUTC)
        tempTime.endTimeMsUTC = datetimeToMs(tempTime.endTimeUTC)

        tempTime.datePart = :hour
        tempTime.datePart = bestDatePart(tempTime.startTimeUTC,tempTime.endTimeUTC)

        tempTime.timeString = "$(padDateTime(tempTime.start_time)) to $(padDateTime(tempTime.endTime)) Local Time"
        tempTime.timeStringUTC = "$(padDateTime(tempTime.startTimeUTC)) to $(padDateTime(tempTime.endTimeUTC)) UTC Time"

        if (showTime)
            println(tempTime.timeString);
            println(tempTime.timeStringUTC);
        end

        return tempTime;
    catch y
        println("TV = timeVariables Exception ",y)
    end
end

function anyTimeVar(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=false
    )
    try
        localTimeVar = TimeVarsInit()
        localTimeVar.start_time = DateTime(Y1,M1,D1,H1,MM1)
        localTimeVar.endTime = DateTime(Y2,M2,D2,H2,MM2)
        localTimeVar.startTimeMs = datetimeToMs(localTimeVar.start_time)
        localTimeVar.endTimeMs = datetimeToMs(localTimeVar.endTime)
        localTimeVar.startTimeStr = "$(padDateTime(localTimeVar.start_time))"
        localTimeVar.endTimeStr = "$(padDateTime(localTimeVar.endTime))"

        localTimeVar.startTimeUTC = datetimeToUTC(localTimeVar.start_time, TimeZone("America/New_York"))
        localTimeVar.endTimeUTC = datetimeToUTC(localTimeVar.endTime, TimeZone("America/New_York"))
        localTimeVar.startTimeMsUTC = datetimeToMs(localTimeVar.startTimeUTC)
        localTimeVar.endTimeMsUTC = datetimeToMs(localTimeVar.endTimeUTC)

        localTimeVar.datePart = :hour
        localTimeVar.datePart = bestDatePart(localTimeVar.startTimeUTC,localTimeVar.endTimeUTC)

        localTimeVar.timeString = "$(padDateTime(localTimeVar.start_time)) to $(padDateTime(localTimeVar.endTime)) Local Time"
        localTimeVar.timeStringUTC = "$(padDateTime(localTimeVar.startTimeUTC)) to $(padDateTime(localTimeVar.endTimeUTC)) UTC Time"

        if (showTime)
            println(localTimeVar.timeString);
            println(localTimeVar.timeStringUTC);
        end

        return localTimeVar
    catch y
        println("anyTimeVar Exception ",y)
    end
end

function weeklyTimeVariables(;days::Int64=7,debugTime::Int64=0)
    try
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2])
        startTime = DateTime(endTime - Day(days) + Minute(1))

        if (debugTime > 0)
            println("weeklyTimeVariables endTime: ", endTime)
            println("startTime: ",startTime)
        end

        localtv =
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

        return localtv

    catch y
        println("TV = weeklyTimeVariables Exception ",y)
    end
end

function prevWorkWeekTimeVariables()
    try
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2])

        # Calc previous week M-F ignore holidays
        while Dates.dayname(endTime) != "Friday"
            endTime = DateTime(endTime - Day(1))
        end

        start_time = DateTime(endTime - Day(5) + Minute(1))

        localtv =
        timeVariables(
            Dates.year(start_time),
            Dates.month(start_time),
            Dates.day(start_time),
            Dates.hour(start_time),
            Dates.minute(start_time),
            Dates.year(endTime),
            Dates.month(endTime),
            Dates.day(endTime),
            Dates.hour(endTime),
            Dates.minute(endTime)
        );

        return localtv

    catch y
        println("TV = prevWorkWeekTimeVariables Exception ",y)
    end
end

function yesterdayTimeVariables(;startHour::Int64=0,endHour::Int64=24,hours=0)
    try
        gmtNow = Dates.now()
        lclNow = now(TimeZone("America/New_York"))
        start_time = lclNow - Dates.Day(1) # comes back UTC
        endTime = DateTime(Dates.year(start_time), Dates.month(start_time), Dates.day(start_time), 23, 59)
        start_time = DateTime(endTime - Hour(24) + Minute(1))

        #println("    clock: ", gmtNow, " local: ", lclNow)
        #println("StartTime: ",start_time)
        #println("  endTime: ",endTime)
        #println("adj start: ",start_time)

        if (hours > 0)
            startHour = 13
            endHour = startHour + hours
            if (endHour > 23)
                endHour = 23
            end
            endTime = DateTime(endTime - Hour(24-endHour))
            start_time = DateTime(start_time + Hour(startHour))
            #println("hr  start: ",start_time)
            #println("hr    end: ",endTime)
        end

        localtv =
        timeVariables(
            Dates.year(start_time),
            Dates.month(start_time),
            Dates.day(start_time),
            Dates.hour(start_time),
            Dates.minute(start_time),
            Dates.year(endTime),
            Dates.month(endTime),
            Dates.day(endTime),
            Dates.hour(endTime),
            Dates.minute(endTime)
        );

        return localtv

    catch y
        println("yesterdayTimeVariables Exception ",y)
    end
end

function todayTimeVariables()
    try
        # If you want today between 7 am and 5 pm use this function
        # Not much use before 7 am and best used after 5 pm like the daily Synthetic runs
        # Otherwise use exact time input like TvExactRange

        gmtNow = Dates.now()
        lclNow = now(TimeZone("America/New_York"))
        start_time = lclNow # comes back UTC
        endTime = DateTime(Dates.year(start_time), Dates.month(start_time), Dates.day(start_time), 23, 59)
        start_time = DateTime(endTime - Hour(24) + Minute(1))

        #println("    clock: ",gmtNow, " local: ",lclNow)
        #println("start_time: ",start_time)
        #println("  endTime: ",endTime)
        #println(" adjStart: ",start_time)

        localtv =
        timeVariables(
            Dates.year(start_time),
            Dates.month(start_time),
            Dates.day(start_time),
            Dates.hour(start_time),
            Dates.minute(start_time),
            Dates.year(endTime),
            Dates.month(endTime),
            Dates.day(endTime),
            Dates.hour(endTime),
            Dates.minute(endTime)
        );

        return localtv

    catch y
        println("todayTimeVariables Exception ",y)
    end
end

function pickTime(;debugTime::Int64=0)

    # Greater than 10 hours is the usual work day
    if isdefined(:TvHours) && TvHours < 10
        localtv = yesterdayTimeVariables(hours=TvHours)
        return localtv
    end

    if isdefined(:TvTodayWorkDay)
        localtv = todayTimeVariables()
        return localtv
    end

    if isdefined(:TvDays) && TvDays < 8
        if (debugTime > 0)
            println(" Calling weeklyTimeVariables TvDays: ",TvDays)
        end
        localtv = weeklyTimeVariables(days=TvDays,debugTime)
        return localtv
    end

    if isdefined(:TvWorkWeek)
        localtv = prevWorkWeekTimeVariables()
        return localtv
    end

    if isdefined(:TvExactRange)

        #println("Debug: TvExactRange=",TvExactRange)
        timeValues = split(TvExactRange,",")

        if size(timeValues,1) != 2
            println("Bad TvExactRange, need two date strings (m/d/yyyy hh:mm) separated by comma")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeStartParts = split(timeValues[1])
        #println("Debug: timeStartParts=",timeStartParts)

        if size(timeStartParts,1) != 2
            println("Bad Date time in first position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeStartPart1 = split(timeStartParts[1],"/")
        if size(timeStartPart1,1) != 3
            println("Bad Date time in first position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeStartPart2 = split(timeStartParts[2],":")
        if size(timeStartPart2,1) != 2
            println("Bad Date time in first position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeEndParts = split(timeValues[2])
        #println("Debug: timeEndParts",timeEndParts)

        if size(timeEndParts,1) != 2
            println("Bad Date time in second position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeEndPart1 = split(timeEndParts[1],"/")
        if size(timeEndPart1,1) != 3
            println("Bad Date time in first position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        timeEndPart2 = split(timeEndParts[2],":")
        if size(timeEndPart2,1) != 2
            println("Bad Date time in first position, need (m/d/yyyy hh:mm) using 24 hour clock")
            println("TvExactRange = \"3/7/2018 20:30,3/7/2018 21:00\" for example")
            return
        end

        if timeStartPart2[1] == "00" timeStartPart2[1] = "0" end
        if timeStartPart2[2] == "00" timeStartPart2[2] = "0" end
        if timeEndPart2[1] == "00" timeEndPart2[1] = "0" end
        if timeEndPart2[2] == "00" timeEndPart2[2] = "0" end

        #println("Debug: TV = timeVariables(",timeStartPart1[3],",",timeStartPart1[2],",",timeStartPart1[1],",",timeStartPart2[1],",",timeStartPart2[2],",",
        #timeEndPart1[3],",",timeEndPart1[2],",",timeEndPart1[1],",",timeEndPart2[1],",",timeEndPart2[2],")"
        #)

        localtv = timeVariables(
            parse(Int64,timeStartPart1[3]),
            parse(Int64,timeStartPart1[1]),
            parse(Int64,timeStartPart1[2]),
            parse(Int64,timeStartPart2[1]),
            parse(Int64,timeStartPart2[2]),
            parse(Int64,timeEndPart1[3]),
            parse(Int64,timeEndPart1[1]),
            parse(Int64,timeEndPart1[2]),
            parse(Int64,timeEndPart2[1]),
            parse(Int64,timeEndPart2[2])
            )

        return localtv

    end

    # Check if the default was forced
    if isdefined(:TvYesterdayWorkDay)
        localtv = yesterdayTimeVariables()
        return localtv
    end

    localtv = yesterdayTimeVariables()
    return localtv
end


# Kernal 5.0
#
#using Dates
#const DATEEPOCH = Dates.value(Date(0))
#const DATETIMEEPOCH = Dates.value(DateTime(0))
#println("DE=",DATEEPOCH, " DTE=", DATETIMEEPOCH)
#
#epochms2datetime(i) = DateTime(Dates.UTM(DATETIMEEPOCH + Int64(i)))
#
#i = 1516410204146
#println(typeof(i))
#epochms2datetime(1516410204)
#Dates.epochms2datetime(i)
