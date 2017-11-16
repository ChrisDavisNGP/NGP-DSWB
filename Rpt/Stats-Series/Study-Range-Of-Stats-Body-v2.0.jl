function rawStatsSROS(TV::TimeVars,UP::UrlParams)

    localStatsDF = DataFrame()
    statsDF = DataFrame()
    medianThreshold = Int64
    try
        localStatsDF = statsTableDF(UP.btView,UP.pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC);    
        statsDF = basicStats(localStatsDF, UP.pageGroup, TV.startTimeMsUTC, TV.endTimeMsUTC)
        medianThreshold = statsDF[1:1,:median][1]
    
        displayTitle(chart_title = "Raw Data Stats Including Those above 600 seconds for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(statsDF[:,:])
    catch y
        println("setupStats Exception ",y)
    end
end

function createAllStatsDF(TV::TimeVars,UP::UrlParams)

    year1 = Dates.year(TV.startTimeUTC)
    month1 = Dates.month(TV.startTimeUTC)
    day1 = Dates.day(TV.startTimeUTC)
    hour1 = Dates.hour(TV.startTimeUTC)
    #minute1 = Dates.minute(TV.startTimeUTC)
    #study whole hours only
    minute1 = 0

    year2 = Dates.year(TV.endTimeUTC)
    month2 = Dates.month(TV.endTimeUTC)
    day2 = Dates.day(TV.endTimeUTC)
    hour2 = Dates.hour(TV.endTimeUTC)
    #minute2 = Dates.minute(TV.endTimeUTC)
    minute2 = 59

    AllStatsDF = DataFrame()

    # todo Month and Year

    i = 0
    for (day = day1:day2)
        #println("Day ",day)
        startHour = hour1
        endHour = hour2
        if (day == day1 && day != day2)
            endHour = 23 
        end

        if (day != day1 && day != day2)
            startHour = 0
            endHour = 23
        end

        if (day != day1 && day == day2)
            startHour = 0
        end

        for (hour = startHour:endHour)
            #println("Year ",year1," Month ",month1," Day ",day," Hour ",hour," M1 ",minute1," M2 ",minute2)
            i += 1
            startTime = DateTime(year1,month1,day,hour,minute1)
            endTime   = DateTime(year2,month2,day,hour,minute2)

            startTimeMs = datetimeToMs(startTime)
            endTimeMs = datetimeToMs(endTime)

            localStatsDF = query("""\
                select 
                    timers_t_done 
                from $(UP.beaconTable)
                where 
                    page_group = '$(UP.pageGroup)' and 
                    "timestamp" between $startTimeMs and $endTimeMs and
                    timers_t_done >= 1 and timers_t_done < 600000 
            """)
            statsDF = runningStats(year1,month1,day,hour,localStatsDF)
            #display(statsDF)
            if (i == 1)
                AllStatsDF = deepcopy(statsDF)
            else
                append!(AllStatsDF,statsDF)
            end
        end
    end

    beautifyDF(AllStatsDF)
    return AllStatsDF
end

function drawC3VizConverter(UP::UrlParams,AllStatsDF::DataFrame;graphType::Int64=1)
    
    if (graphType == 1)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:mean]
            drawDF[:data2] = AllStatsDF[:median]
            drawDF[:data3] = AllStatsDF[:stddev]

            c3 = drawC3Viz(drawDF; axisLabels=["Mean","Median", "Standard Dev"],dataNames=["Mean", 
                "Median", "Standard Deviation"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line","line","line"])
        catch y
            println("drawC3VizConverter exception ",y)
        end 
        return
    end

    if (graphType == 2)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:q25]
            drawDF[:data2] = AllStatsDF[:median]
            drawDF[:data3] = AllStatsDF[:q75]

        c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],
            dataNames=["Q25","Q50","Q75"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line","line"])
        catch y
            println("drawQ25Q75 exception ",y)
        end    
        return
    end
    
    if (graphType == 3)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:kurtosis]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Kurtosis"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawKurt exception ",y)
        end    
        return
    end

    if (graphType == 4)

        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data2] = AllStatsDF[:skewness]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Skewness"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawSkew exception ",y)
        end    
    end

    if (graphType == 5)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:entropy]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Entropy"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawEntropy exception ",y)
        end    
    end
    
    if (graphType == 6)
        try
            drawDF = DataFrame()
            drawDF[:col1] = AllStatsDF[:datetime]
            drawDF[:data1] = AllStatsDF[:modes]

            c3 = drawC3Viz(drawDF; axisLabels=["Seconds"],dataNames=["Modes"], mPulseWidget=false, chart_title="$(UP.pageGroup) Page Group", vizTypes=["line"])
        catch y
            println("drawModes exception ",y)
        end    
    end
end
