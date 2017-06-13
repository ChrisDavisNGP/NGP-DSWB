function firstAndLast(localTable::ASCIIString,pageGroup::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2)
    limitedTable(localTable,table,pageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
    setTable(localTable)
    topUrlTableForWPF(localTable,pageGroup,tv.timeString;rowLimit=rowLimit, beaconsLimit=beaconsLimit)
    q = query(""" drop view if exists $localTable;""")
end

function topUrlTableForWPF(ltName::ASCIIString, pageGroup::ASCIIString,timeString::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2)
    try
        displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) WPF URLs for $(pageGroup)", chart_info = ["Note: If you see AEM URL's in this list tell Chris Davis",timeString],showTimeStamp=false)

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

        scrubUrlToPrint(topurl)
        newDF = topurl[Bool[x > beaconsLimit for x in topurl[:count]],:]
        beautifyDF(names!(newDF[:,:],[symbol("Views"),symbol("Url - With Grouping After Parameters Dropped")]))

    catch y
        println("topUrlTable Exception ",y)
    end

end

function cleanupTableFTWP(TV::TimeVars,UP::UrlParams)
    
    CleanupTable = query("""\
        select 
            page_group,
            count(*) as "Page Views",
            count(*)/7 as "Per Day"
        FROM $(UP.beaconTable)
        where 
            beacon_type = 'page view' 
            and "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            and page_group in ('Adventure WPF','Animals WPF','Environment WPF','Games WPF','Images WPF','Magazine NGM',
                                'Movies WPF','Ocean WPF','Photography WPF','Science WPF','Travel WPF')
        GROUP BY page_group
        Order by count(*) desc
    """)

    beautifyDF(CleanupTable[1:min(10,end),:])
end

