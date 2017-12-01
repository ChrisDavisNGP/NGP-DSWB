function bestDatePart(startTime::DateTime, endTime::DateTime, datePart::Symbol)

    try
        datePart = :minute
        dt = endTime-startTime
        deltaTime = convert(Int64,dt)
        minuteLimit = (6*60*60*1000)+1
        hourLimit = (7*24*60*60*1000)+1
        #println("starttime=",startTime," endtime=",endTime," deltaTime =",deltaTime," minuteLimit=",minuteLimit," hourLimit=",hourLimit)

        if (deltaTime < minuteLimit)
            datePart = :minute
        elseif (deltaTime < hourLimit)
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

tv = TimeVarsInit()

function timeVariables(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=true
    )
    try
        timeInit.startTime = DateTime(Y1,M1,D1,H1,MM1)
        timeInit.endTime = DateTime(Y2,M2,D2,H2,MM2)
        timeInit.startTimeMs = datetimeToMs(timeInit.startTime)
        timeInit.endTimeMs = datetimeToMs(timeInit.endTime)

        timeInit.startTimeUTC = datetimeToUTC(timeInit.startTime, TimeZone("America/New_York"))
        timeInit.endTimeUTC = datetimeToUTC(timeInit.endTime, TimeZone("America/New_York"))
        timeInit.startTimeMsUTC = datetimeToMs(timeInit.startTimeUTC)
        timeInit.endTimeMsUTC = datetimeToMs(timeInit.endTimeUTC)

        timeInit.datePart = :hour
        timeInit.datePart = bestDatePart(timeInit.startTimeUTC,timeInit.endTimeUTC,timeInit.datePart)

        timeInit.timeString = "$(padDateTime(timeInit.startTime)) to $(padDateTime(timeInit.endTime)) Local Time"
        timeInit.timeStringUTC = "$(padDateTime(timeInit.startTimeUTC)) to $(padDateTime(timeInit.endTimeUTC)) UTC Time"

        if (showTime)
            println(timeInit.timeString);
            println(timeInit.timeStringUTC);
        end

        return timeInit;
    catch y
        println("TV = timeVariables Exception ",y)
    end
end

function anyTimeVar(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=true
    )
    try
        anyTimeVar = TimeVarsInit()
        anyTimeVar.startTime = DateTime(Y1,M1,D1,H1,MM1)
        anyTimeVar.endTime = DateTime(Y2,M2,D2,H2,MM2)
        anyTimeVar.startTimeMs = datetimeToMs(anyTimeVar.startTime)
        anyTimeVar.endTimeMs = datetimeToMs(anyTimeVar.endTime)

        anyTimeVar.startTimeUTC = datetimeToUTC(anyTimeVar.startTime, TimeZone("America/New_York"))
        anyTimeVar.endTimeUTC = datetimeToUTC(anyTimeVar.endTime, TimeZone("America/New_York"))
        anyTimeVar.startTimeMsUTC = datetimeToMs(anyTimeVar.startTimeUTC)
        anyTimeVar.endTimeMsUTC = datetimeToMs(anyTimeVar.endTimeUTC)

        anyTimeVar.datePart = :hour
        anyTimeVar.datePart = bestDatePart(anyTimeVar.startTimeUTC,anyTimeVar.endTimeUTC,anyTimeVar.datePart)

        localTV.timeString = "$(padDateTime(anyTimeVar.startTime)) to $(padDateTime(anyTimeVar.endTime)) Local Time"
        localTV.timeStringUTC = "$(padDateTime(anyTimeVar.startTimeUTC)) to $(padDateTime(anyTimeVar.endTimeUTC)) UTC Time"

        if (showTime)
            println(localTV.timeString);
            println(localTV.timeStringUTC);
        end

        return anyTimeVar
    catch y
        println("anyTimeVar Exception ",y)
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
        println("TV = weeklyTimeVariables Exception ",y)
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
