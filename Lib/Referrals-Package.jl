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
        beautifyDF(names!(dftest2[1:end,[1:2;]],[Symbol("Referral Group"),Symbol("Page Views")]))

        beautifyDF(names!(gas[1:min(10,end),[1:2;]],[Symbol("Google Analytics Field: Source"),Symbol("Page Views")]))
        beautifyDF(names!(gam[1:min(10,end),[1:2;]],[Symbol("Google Analytics Field: Medium"),Symbol("Page Views")]))
        beautifyDF(names!(gac[1:min(10,end),[1:2;]],[Symbol("Google Analytics Field: Campaign"),Symbol("Page Views")]))

    catch y
        println("customReferralsTable Exception ",y)
    end
end

function standardReferrals(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        if SP.debugLevel > 8
            println("Starting standardReferrals")
        end
                
        toprDF = getTopReferrers(TV.startTimeUTC, TV.endTimeUTC, n=UP.limitRows)
        limit = (min(UP.limitRows,size(toprDF)[1]))
        chartTopN(TV.startTimeUTC, TV.endTimeUTC, n=limit; variable=:referrers;)
        displayTitle(chart_title = "Top Referrers for $(productPageGroup)", chart_info = [TV.timeString],showTimeStamp=false)
        beautifyDF(toprDF)
    catch y
        println("standardReferrals Exception ",y)
    end
end
