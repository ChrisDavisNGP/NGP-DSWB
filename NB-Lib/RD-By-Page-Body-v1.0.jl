function resourceMatched(tableRt::ASCIIString;linesOut::Int64=25)
    
    try
        joinTables = query("""\
        select 
        count(*)
        from $tableRt
        where 
            "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and 
             url ilike '$resourceUrl'
        """);

        displayTitle(chart_title = "Matches For Url Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end 

function resourceScreen(tableRt::ASCIIString;linesOut::Int64=25)
    
    try
        joinTables = query("""\
        select 
        count(*),
        initiator_type,
        height,
        width,
        x,
        y,
        url       
        from $tableRt
        where 
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        group by initiator_type,height,width,x,y,url
        order by count(*) desc
        limit $(linesOut)
        """);

        displayTitle(chart_title = "Screen Details For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end 

function resourceSize(tableRt::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)
    
    try
        joinTables = query("""\
        select 
        count(*),
        encoded_size,
        transferred_size,
        decoded_size,
        url       
        from $tableRt
        where 
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and
        encoded_size > $(minEncoded)
        group by encoded_size,transferred_size,decoded_size,url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Size Details For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
        
        dv1 = joinTables[:encoded_size]
        statsDF1 = limitedStatsFromDV(dv1)
        showLimitedStats(statsDF1,"Encoded Size Stats (Page Views are Groups In Above Table)")
        dv2 = joinTables[:transferred_size]
        statsDF2 = limitedStatsFromDV(dv2)
        showLimitedStats(statsDF2,"Transferred Size Stats")
        
    catch y
        println("bigTable5 Exception ",y)
    end
end 

function resourceSummary(tableRt::ASCIIString;linesOut::Int64=25)
    
    try
        joinTables = query("""\
        select 
        count(*),url
        from $tableRt
        where 
            "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC) and 
             url ilike '$resourceUrl'
        group by 
        url
        order by count(*) desc
        """);

        displayTitle(chart_title = "Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end 

function resourceSummaryAllFields(tableRt::ASCIIString;linesOut::Int64=25)
    
    try
        joinTables = query("""\
        select 
        *
        from $tableRt
        where 
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        limit $(linesOut)
        """);

        displayTitle(chart_title = "Raw Resource Url Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end 

function resourceSummaryDomainUrl(tableRt::ASCIIString;linesOut::Int64=25)
    
    try
        joinTables = query("""\
        select 
        count(*),
        url,params_u
        from $tableRt
        where 
          url ilike '$resourceUrl' and
          "timestamp" between $(tv.startTimeMsUTC) and $(tv.endTimeMsUTC)
        group by 
            url, params_u
        order by count(*) desc
        limit 25
        """);

        displayTitle(chart_title = "Domain Url For Resource Pattern $(resourceUrl)", chart_info = [tv.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end 
