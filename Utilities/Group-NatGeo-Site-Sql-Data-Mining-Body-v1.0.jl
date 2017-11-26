type LocalVars
    linesOutput::Int64
end

function defaultTableGNGSSDM(TV::TimeVars,UP::UrlParams)
    
    try
        localTable = UP.btView
        table = UP.beaconTable

        query("""\
            create or replace view $localTable as (
                select * 
                    from $table 
                    where 
                        page_group ilike '$(UP.pageGroup)' and 
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and 
                        beacon_type = 'page view' and 
                        url ilike '$(UP.urlRegEx)'
        )""")

        cnt = query("""SELECT count(*) FROM $localTable""")
        println("$localTable count is ",cnt[1,1])
    catch y
        println("defaultTableGNGSSDM Exception ",y)
    end
end

function test1GNGSSDM(UP::UrlParams,LV::LocalVars)
    
    try

        test1Table = query("""\
            select URL, count(*)
                FROM $(UP.btView)
                GROUP BY url
                Order by count(*) desc
        """)
        
        beautifyDF(test1Table[1:min(LV.linesOutput,end),:])
    catch y
        println("test1GNGSSDM Exception ",y)
    end
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

function testUserAgentGNGSSDM(UP::UrlParams,LV::LocalVars)
    
    try
        CleanupTable = query("""\
            select 
                count(*),user_agent_raw
            FROM $(UP.btView)
            where 
                beacon_type = 'page view'
            group by user_agent_raw
            order by count(*) desc
        limit 1000
    """)

        beautifyDF(CleanupTable[1:min(LV.linesOutput,end),:])
        
    catch y
        println("test2GNGSSDM Exception ",y)
    end
end


