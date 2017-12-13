function pageGroupDetailsCreateView(TV::TimeVars,UP::UrlParams,localMobileTable::ASCIIString,localDesktopTable::ASCIIString)
      try

        query("""\
            drop view if exists $(UP.btView)
        """)

        query("""\
            create or replace view $(UP.btView) as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs)
            )
        """)

        query("""\
            create or replace view $localMobileTable as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            user_agent_device_type = 'Mobile'
            )
        """)

        query("""\
            create or replace view $localDesktopTable as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            "timestamp" between $(TV.startTimeMs) and $(TV.endTimeMs) and
            user_agent_device_type = 'Desktop'
            )
        """);

    catch y
        println("pageGroupDetailTables Exception ",y)
    end
end

function defaultBeaconCreateView(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        bt = UP.beaconTable
        btv = UP.btView
        timeLowerMs = UP.timeLowerMs > 0 ? UP.timeLowerMs : 1000
        timeUpperMs = UP.timeUpperMs > 0 ? UP.timeUpperMs : 600000
        if (SP.debugLevel > 0)
            println("Low=",timeLowerMs," High=", timeUpperMs)
        end

        query("""\
            create or replace view $btv as (
                select * FROM $bt
                    where
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group ilike '$(UP.pageGroup)' and
                        params_u ilike '$(UP.urlRegEx)' and
                        user_agent_device_type ilike '$(UP.deviceType)' and
                        user_agent_os ilike '$(UP.agentOs)' and
                        timers_t_done >= $timeLowerMs and timers_t_done < $timeUpperMs
            )
        """)
        if (SP.debugLevel > 0)
            cnt = query("""SELECT count(*) FROM $btv""")
            println("$btv count is ",cnt[1,1])
        end
    catch y
        println("defaultBeaconCreateView Exception ",y)
    end
end

function defaultResourceView(TV::TimeVars,UP::UrlParams)

    try
        rtv = UP.rtView
        rt = UP.resourceTable
        btv = UP.btView

        query("""create or replace view $rtv as (
            select $rt.*
            from $btv join $rt
                on $rt.session_id = $btv.session_id
            where
                $rt."timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                $btv.session_id IS NOT NULL
            order by $rt.session_id, $rt."timestamp", $rt.start_time
        )""")

        # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
        # where beacon_type = 'page view'
        localTableRtDF = query("""SELECT * FROM $rtv""")
        #Hide output from final report
        println("$rtv count is ",size(localTableRtDF))
    catch y
        println("setupLocalTable Exception ",y)
    end
end
