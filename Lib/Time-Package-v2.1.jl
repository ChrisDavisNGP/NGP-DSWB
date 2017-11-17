# Packages in Lib Directory; structures first
include("Structures-Package-v2.0.jl")
;

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

function anyTimeVar(
    Y1::Int64,M1::Int64,D1::Int64,H1::Int64,MM1::Int64,
    Y2::Int64,M2::Int64,D2::Int64,H2::Int64,MM2::Int64;
    showTime::Bool=true
    )
    try
        localtv = TimeVarsInit()
        localtv.startTime = DateTime(Y1,M1,D1,H1,MM1)
        localtv.endTime = DateTime(Y2,M2,D2,H2,MM2)
        localtv.startTimeMs = datetimeToMs(localtv.startTime)
        localtv.endTimeMs = datetimeToMs(localtv.endTime)

        localtv.startTimeUTC = datetimeToUTC(localtv.startTime, TimeZone("America/New_York"))
        localtv.endTimeUTC = datetimeToUTC(localtv.endTime, TimeZone("America/New_York"))
        localtv.startTimeMsUTC = datetimeToMs(localtv.startTimeUTC)
        localtv.endTimeMsUTC = datetimeToMs(localtv.endTimeUTC)

        localtv.datePart = :hour
        localtv.datePart = bestDatePart(localtv.startTimeUTC,localtv.endTimeUTC,localtv.datePart)

        localtv.timeString = "$(padDateTime(localtv.startTime)) to $(padDateTime(localtv.endTime)) Local Time"
        localtv.timeStringUTC = "$(padDateTime(localtv.startTimeUTC)) to $(padDateTime(localtv.endTimeUTC)) UTC Time"

        if (showTime)
            println(localtv.timeString);
            println(localtv.timeStringUTC);
        end

        return localtv
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
