function firstAndLast(TV::TimeVars,UP::UrlParams,pageGroup::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
    wpfLimitedTable(UP.btView,UP.beaconTable,pageGroup,TV.startTimeMsUTC,TV.endTimeMsUTC)
    setTable(UP.btView)
    topUrlTableForWPF(UP.btView,pageGroup,TV.timeString;rowLimit=rowLimit, beaconsLimit=beaconsLimit, paginate=paginate)
    q = query(""" drop view if exists $(UP.btView);""")
end

function topUrlTableForWPF(ltName::ASCIIString, pageGroup::ASCIIString,timeString::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2, paginate::Bool=false)
    try
       
        topurl = query("""\

        select count(*),
        CASE 
        when  (position('?' in params_u) > 0) then trim('/' from (substring(params_u for position('?' in substring(params_u from 9)) +7)))
        else trim('/' from params_u)
        end urlgroup
        FROM $(ltName)
        where 
        beacon_type = 'page view'
        group by urlgroup
        order by count(*) desc
        limit $(rowLimit)
        """);

        #println(nrow(topurl))
        #beautifyDF(topurl)
        
        if (nrow(topurl) == 0)
            displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) URLs for $(pageGroup) - No Page Views", showTimeStamp=false)
            return
        else
            displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) URLs for $(pageGroup)", chart_info = ["Note: If you see AEM URL's in this list tell Chris Davis",timeString],showTimeStamp=false)
        end
        
        #scrubUrlToPrint(topurl)
        #println(nrow(topurl))
        
        
        
        newDF = topurl[Bool[x > beaconsLimit for x in topurl[:count]],:]
        printDF = names!(newDF[:,:],[symbol("Views"),symbol("Url - $(pageGroup)")])
        
        #beautifyDF(printDF)

        if (paginate)
            paginatePrintDf(printDF)
        else
            beautifyDF(printDF[:,:])
        end

    catch y
        println("topUrlTable Exception ",y)
    end

end

function paginatePrintDf(printDF::DataFrame)
    try
        currentLine = 1
        linesOut = 25
        linesToPrint = size(printDF,1)
        
        while currentLine < linesToPrint
            beautifyDF(printDF[currentLine:min(currentLine+linesOut-1,end),:])
            currentLine += linesOut
        end

    catch y
        println("paginatePrintDf Exception ",y)
    end

end

function cleanupTableFTWP(TV::TimeVars,UP::UrlParams)
    
    CleanupTable = query("""\
        select 
            page_group,
            count(*) as "Page Views"
        FROM $(UP.beaconTable)
        where 
            beacon_type = 'page view' 
            and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            and page_group in ('Adventure WPF','Animals WPF','Environment WPF','Games WPF','Images WPF',
                                'Movies WPF','Ocean WPF','Photography WPF','Science WPF','Travel WPF')
        GROUP BY page_group
        Order by count(*) desc
    """)

    beautifyDF(CleanupTable[1:min(10,end),:])
end

