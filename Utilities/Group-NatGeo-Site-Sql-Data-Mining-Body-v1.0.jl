type LocalVars
    linesOutput::Int64
end


function test2GNGSSDM(UP::UrlParams,LV::LocalVars)

    try
        CleanupTable = query("""\
            select
                count(*), URL, params_u
            FROM $(UP.btView)
            where
                beacon_type = 'page view'
            GROUP BY url,params_u
            Order by count(*) desc
    """)

        beautifyDF(CleanupTable[1:min(LV.linesOutput,end),:])

    catch y
        println("test2GNGSSDM Exception ",y)
    end
end

function test3GNGSSDM(UP::UrlParams,LV::LocalVars)

    try
        CleanupTable = query("""\
            select
                count(*) as "Page Views",
                params_u as "URL Landing In Nat Geo Site Default Group"
            FROM $(UP.btView)
            where
                beacon_type = 'page view' and
                params_u <> 'http://www.nationalgeographic.com/' and
                params_u like 'http://www.nationalgeographic.com/?%'
            GROUP BY params_u
            Order by count(*) desc
        """)

        beautifyDF(CleanupTable[1:min(LV.linesOutput,end),:])

    catch y
        println("test3GNGSSDM Exception ",y)
    end
end
