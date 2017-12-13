# Packages in Lib Directory; structures first
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



function timeVariables(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=true
    )
    try
        tempTime = TimeVarsInit()

        tempTime.startTime = DateTime(Y1,M1,D1,H1,MM1)
        tempTime.endTime = DateTime(Y2,M2,D2,H2,MM2)
        tempTime.startTimeMs = datetimeToMs(tempTime.startTime)
        tempTime.endTimeMs = datetimeToMs(tempTime.endTime)

        tempTime.startTimeUTC = datetimeToUTC(tempTime.startTime, TimeZone("America/New_York"))
        tempTime.endTimeUTC = datetimeToUTC(tempTime.endTime, TimeZone("America/New_York"))
        tempTime.startTimeMsUTC = datetimeToMs(tempTime.startTimeUTC)
        tempTime.endTimeMsUTC = datetimeToMs(tempTime.endTimeUTC)

        tempTime.datePart = :hour
        tempTime.datePart = bestDatePart(tempTime.startTimeUTC,tempTime.endTimeUTC,tempTime.datePart)

        tempTime.timeString = "$(padDateTime(tempTime.startTime)) to $(padDateTime(tempTime.endTime)) Local Time"
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
    showTime::Bool=true
    )
    try
        localTimeVar = TimeVarsInit()
        localTimeVar.startTime = DateTime(Y1,M1,D1,H1,MM1)
        localTimeVar.endTime = DateTime(Y2,M2,D2,H2,MM2)
        localTimeVar.startTimeMs = datetimeToMs(localTimeVar.startTime)
        localTimeVar.endTimeMs = datetimeToMs(localTimeVar.endTime)

        localTimeVar.startTimeUTC = datetimeToUTC(localTimeVar.startTime, TimeZone("America/New_York"))
        localTimeVar.endTimeUTC = datetimeToUTC(localTimeVar.endTime, TimeZone("America/New_York"))
        localTimeVar.startTimeMsUTC = datetimeToMs(localTimeVar.startTimeUTC)
        localTimeVar.endTimeMsUTC = datetimeToMs(localTimeVar.endTimeUTC)

        localTimeVar.datePart = :hour
        localTimeVar.datePart = bestDatePart(localTimeVar.startTimeUTC,localTimeVar.endTimeUTC,localTimeVar.datePart)

        localTimeVar.timeString = "$(padDateTime(localTimeVar.startTime)) to $(padDateTime(localTimeVar.endTime)) Local Time"
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

function weeklyTimeVariables(;days::Int64=7)
    try
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2])
        startTime = DateTime(endTime - Day(days) + Minute(1))

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

# Take 10 hours from yesterday by default
function yesterdayTimeVariables(;startHour::Int64=7,endHour::Int64=17,hours=0)
    try
        if (hours > 0)
            startHour = 13
            endHour = startHour + hours
            if (endHour > 23)
                endHour = 23
            end
        end
        firstAndLast = getBeaconsFirstAndLast()
        endTime = DateTime(firstAndLast[1,2] - Hour(24-endHour))
        startTime = DateTime(endTime - Hour(endHour-startHour) + Second(1))

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
        println("yesterdayTimeVariables Exception ",y)
    end
end
