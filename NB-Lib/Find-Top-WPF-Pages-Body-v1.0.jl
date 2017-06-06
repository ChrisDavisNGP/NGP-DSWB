function firstAndLast(localTable::ASCIIString,pageGroup::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2)
    limitedTable(localTable,table,pageGroup,tv.startTimeMsUTC,tv.endTimeMsUTC)
    setTable(localTable)
    topUrlTableForWPF(localTable,pageGroup,tv.timeString;rowLimit=rowLimit, beaconsLimit=beaconsLimit)
    q = query(""" drop view if exists $localTable;""")
end

function topUrlTableForWPF(ltName::ASCIIString, pageGroup::ASCIIString,timeString::ASCIIString; rowLimit::Int64=20, beaconsLimit::Int64=2)
    try
        displayTitle(chart_title = "Top $(rowLimit) (min $(beaconsLimit)) WPF URLs for $(pageGroup)", chart_info = ["Note: If you see AEM URL's in this list tell Chris Davis or Derek Fetsch",timeString],showTimeStamp=false)

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

