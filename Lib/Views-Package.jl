function obsoleteDefaultBeaconCreateView(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    try
        bt = UP.beaconTable
        btv = UP.bt View
        if (SP.debugLevel > 0)
            println("page group=$(UP.pageGroup), devType=$(UP.deviceType), os=$(UP.agentOs)")
            println("paramsu=",UP.urlRegEx)
            println("Low=",UP.timeLowerMs," High=", UP.timeUpperMs)
        end

# another way for beacons
        beaconFilter = SQLFilter[
            ilike("pagegroupname",UP.pageGroup),
            ilike("paramsu",UP.urlRegEx),
            ilike("devicetypename",UP.deviceType),
            ilike("operatingsystemname",UP.agentOs)
            ]

            $bt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            $bt.pagegroupname ilike '$(UP.pageGroup)' and
            $bt.paramsu ilike '$(UP.urlRegEx)' and
            $bt.devicetypename ilike '$(UP.deviceType)' and
            $bt.operatingsystemname ilike '$(UP.agentOs)' and
            $bt.pageloadtime >= $(UP.timeLowerMs) and $bt.pageloadtime < $(UP.timeUpperMs)


        select("""\
            create or replace view $b tv as (
                select * FROM $bt
                    where
                        timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        pagegroupname ilike '$(UP.pageGroup)' and
                        paramsu ilike '$(UP.urlRegEx)' and
                        devicetypename ilike '$(UP.deviceType)' and
                        operatingsystemname ilike '$(UP.agentOs)' and
                        pageloadtime >= $(UP.timeLowerMs) and pageloadtime < $(UP.timeUpperMs)
            )
        """)
    catch y
        println("obsoleteDefaultBeaconCreateView Exception ",y)
    end
end

function obsoleteDefaultResourceView(TV::TimeVars,UP::UrlParams)

    try

        rt = UP.resourceTable
        bt = UP.beaconTable

        select $rt.*
        from
            $bt join $rt on $rt.sessionid = $bt.sessionid
        where
            $rt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            $bt.sessionid IS NOT NULL and
            $bt.pagegroupname ilike '$(UP.pageGroup)' and
            $bt.paramsu ilike '$(UP.urlRegEx)' and
            $bt.devicetypename ilike '$(UP.deviceType)' and
            $bt.operatingsystemname ilike '$(UP.agentOs)' and
            $bt.pageloadtime >= $(UP.timeLowerMs) and $bt.pageloadtime < $(UP.timeUpperMs)
        order by $rt.sessionid, $rt.timestamp, $rt.start_time

        select("""\
            create or replace view $rt v as (
                select $rt.*
                from $bt v join $rt on $rt.sessionid = $bt v.sessionid
                where
                    $rt.timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                    $bt v.sessionid IS NOT NULL
                order by $rt.sessionid, $rt.timestamp, $rt.start_time
            )
            """)

        # Some routines use the unload events, some do not.  First count is all beacons such as page view and unload
        # where beacontypename = 'page view'
        localTableRtDF = select("""SELECT * FROM $rt v""")
        #Hide output from final report
        println("$rt v count is ",size(localTableRtDF))
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function obsoletePageGroupDetailsCreateView(TV::TimeVars,UP::UrlParams,SP::ShowParams,localMobileTable::ASCIIString,localDesktopTable::ASCIIString)

    if SP.debugLevel > 8
        println("Starting pageGroupDetailsCreateView")
    end

      try

        select("""\
            create or replace view $(UP.bt View) as
            (select * FROM $(UP.beaconTable)
            where pagegroupname ilike '$(UP.pageGroup)' and
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
            where pagegroupname ilike '$(UP.pageGroup)' and
            timestamp between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
            paramsu ilike '$(UP.urlRegEx)' and
            operatingsystemname ilike '$(UP.agentOs)' and
            devicetypename = 'Mobile'
            )
        """)

        select("""\
            create or replace view $localDesktopTable as
            (select * FROM $(UP.beaconTable)
            where pagegroupname ilike '$(UP.pageGroup)' and
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
