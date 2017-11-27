function customReferralsTable(TV::TimeVars,UP::UrlParams)
    try
        localTable = UP.btView

        fb = query("""\
        select 'Facebook' refgrp, count(*)
        FROM $localTable
        where http_referrer ilike '%facebook%' or params_r ilike '%facebook%' or tp_ga_utm_source ilike '%facebook%'
        """)

        #display(fb)


        gb = query("""\
        select 'Google' refgrp, count(*)
        FROM $localTable
        where http_referrer ilike '%google%' or params_r ilike '%google%' or tp_ga_utm_source ilike '%google%'
        """)

        #display(gb)

        red = query("""\
        select 'Reddit' refgrp, count(*)
        FROM $localTable
        where http_referrer ilike '%reddit%' or params_r ilike '%reddit%' or tp_ga_utm_source ilike '%reddit%'
        """)

        #display(red)

        gas = query("""\
        select tp_ga_utm_source, count(*)
        FROM $localTable
        where http_referrer is not null and http_referrer != 'null'
        group by tp_ga_utm_source
        order by count(*) desc
        """)

        #display(gas)

        gam = query("""\
        select tp_ga_utm_medium, count(*)
        FROM $localTable
        where http_referrer is not null and http_referrer != 'null'
        group by tp_ga_utm_medium
        order by count(*) desc
        """)

        #display(gam)

        gac = query("""\
        select tp_ga_utm_campaign, count(*)
        FROM $localTable
        where http_referrer is not null and http_referrer != 'null'
        group by tp_ga_utm_campaign
        order by count(*) desc
        """)

        displayTitle(chart_title = "Custom Analytics Top Referrers for $(UP.pageGroup)", chart_info = [TV.timeString],showTimeStamp=false)

        dftest2 = DataFrame(RefGroup=["","",""],Cnt=1:3)
        dftest2[1:1,:Cnt] = fb[1:1,:count]
        dftest2[2:2,:Cnt] = gb[1:1,:count]
        dftest2[3:3,:Cnt] = red[1:1,:count]
        dftest2[1:1,:RefGroup] = fb[1:1,:refgrp]
        dftest2[2:2,:RefGroup] = gb[1:1,:refgrp]
        dftest2[3:3,:RefGroup] = red[1:1,:refgrp]

        sort!(dftest2, cols=:Cnt, rev=true)
        beautifyDF(names!(dftest2[1:end,[1:2;]],[symbol("Referral Group"),symbol("Page Views")]))

        beautifyDF(names!(gas[1:min(10,end),[1:2;]],[symbol("Google Analytics Field: Source"),symbol("Page Views")]))
        beautifyDF(names!(gam[1:min(10,end),[1:2;]],[symbol("Google Analytics Field: Medium"),symbol("Page Views")]))
        beautifyDF(names!(gac[1:min(10,end),[1:2;]],[symbol("Google Analytics Field: Campaign"),symbol("Page Views")]))

    catch y
        println("customReferralsTable Exception ",y)
    end
end


function customReferralsTable0ld(localTable::ASCIIString, productPageGroup::ASCIIString)

    try
        displayTitle(chart_title = "Google Analytics Fields - Top Referrers for $(productPageGroup)", chart_info = [TV.timeString],showTimeStamp=false)

        fb = query("""\
            select 'Facebook' AS " ", count(*)
            FROM $localTable
            where http_referrer ilike '%facebook%' or params_r ilike '%facebook%' or tp_ga_utm_source ilike '%facebook%'
        """)

        display(fb)

        fb = query("""\
            select 'Google' AS " ", count(*)
            FROM $localTable
            where http_referrer ilike '%google%' or params_r ilike '%google%' or tp_ga_utm_source ilike '%google%'
        """)

        display(fb)

        gas = query("""\
            select tp_ga_utm_source, count(*)
            FROM $localTable
            where http_referrer is not null and http_referrer != 'null'
            group by tp_ga_utm_source
            order by count(*) desc
        """)

        display(gas)

        gam = query("""\
            select tp_ga_utm_medium, count(*)
            FROM $localTable
            where http_referrer is not null and http_referrer != 'null'
            group by tp_ga_utm_medium
            order by count(*) desc
        """)

        display(gam)

        gac = query("""\
            select tp_ga_utm_campaign, count(*)
            FROM $localTable
            where http_referrer is not null and http_referrer != 'null'
            group by tp_ga_utm_campaign
            order by count(*) desc
        """)

        display(gac)
    catch y
        println("customReferralsTable Exception ",y)
    end
end

function standardReferrals(localTable::ASCIIString, productPageGroup::ASCIIString, startTime::DateTime, endTime::DateTime, timeString::ASCIIString; limit::Int64=10)

    try
        topr = getTopReferrers(startTime, endTime, n=limit)
        limit = (min(limit,size(topr)[1]))
        chartTopN(startTime, endTime, n=limit; variable=:referrers;)
        displayTitle(chart_title = "Top Referrers for $(productPageGroup)", chart_info = [timeString],showTimeStamp=false)
        beautifyDF(topr)
    catch y
        println("standardReferrals Exception ",y)
    end
end
