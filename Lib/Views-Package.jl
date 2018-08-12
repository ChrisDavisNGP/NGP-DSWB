function defaultBeaconCreateView(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        bt = UP.beaconTable
        btv = UP.btView
        if (SP.debugLevel > 0)
            println("page group=$(UP.pageGroup), devType=$(UP.deviceType), os=$(UP.agentOs)")
            println("paramsu=",UP.urlRegEx)
            println("Low=",UP.timeLowerMs," High=", UP.timeUpperMs)
        end

        select("""\
            create or replace view $btv as (
                select * FROM $bt
                    where
                        timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group ilike '$(UP.pageGroup)' and
                        paramsu ilike '$(UP.urlRegEx)' and
                        devicetypename ilike '$(UP.deviceType)' and
                        operatingsystemname ilike '$(UP.agentOs)' and
                        pageloadtime >= $(UP.timeLowerMs) and pageloadtime < $(UP.timeUpperMs)
            )
        """)
        if (SP.debugLevel > 0)
            cnt = select("""SELECT count(*) FROM $btv""")
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

        select("""\
            create or replace view $rtv as (
                select $rt.*
                from $btv join $rt on $rt.sessionId = $btv.sessionId
                where
                    $rt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                    $btv.sessionId IS NOT NULL
                order by $rt.sessionId, $rt.timestamp, $rt.start_time
            )
            """)

        # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
        # where beacon_type = 'page view'
        localTableRtDF = select("""SELECT * FROM $rtv""")
        #Hide output from final report
        println("$rtv count is ",size(localTableRtDF))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function pageGroupDetailsCreateView(TV::TimeVars,UP::UrlParams,SP::ShowParams,localMobileTable::ASCIIString,localDesktopTable::ASCIIString)

    if SP.debugLevel > 8
        println("Starting pageGroupDetailsCreateView")
    end

      try

        select("""drop view if exists $(UP.btView)""")

        select("""\
            create or replace view $(UP.btView) as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            paramsu ilike '$(UP.urlRegEx)' and
            operatingsystemname ilike '$(UP.agentOs)' and
            devicetypename ilike '$(UP.deviceType)' and
            timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC)
            )
        """)

        # todo if select count into var for size zero check and dbg output

        select("""\
            create or replace view $localMobileTable as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            paramsu ilike '$(UP.urlRegEx)' and
            operatingsystemname ilike '$(UP.agentOs)' and
            devicetypename = 'Mobile'
            )
        """)

        select("""\
            create or replace view $localDesktopTable as
            (select * FROM $(UP.beaconTable)
            where page_group ilike '$(UP.pageGroup)' and
            timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            paramsu ilike '$(UP.urlRegEx)' and
            operatingsystemname ilike '$(UP.agentOs)' and
            devicetypename = 'Desktop'
            )
        """);

    catch y
        println("pageGroupDetailsCreateView Exception ",y)
    end
end
