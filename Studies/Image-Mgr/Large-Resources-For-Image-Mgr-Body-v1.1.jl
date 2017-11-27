type LocalVars
    fileType::ASCIIString
end

function defaultLocalTableALR(TV::TimeVars,UP::UrlParams)
    try
        table = UP.beaconTable
        localTable = UP.btView

        query("""\
            create or replace view $localTable as (
                select * from $table
                    where
                        "timestamp" between $(TV.startTimeMsUTC) and $(TV.endTimeMsUTC) and
                        page_group ilike '$(UP.pageGroup)' and
                        params_u ilike '$(UP.urlRegEx)' and
                        user_agent_device_type ilike '$(UP.deviceType)'
            )
        """)
        cnt = query("""SELECT count(*) FROM $localTable""")
        println("$localTable count is ",cnt[1,1])
    catch y
        println("setupLocalTable Exception ",y)
    end
end

function resourceSummaryLRFIMB(TV::TimeVars,UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
            select
                avg($tableRt.encoded_size) as encoded,
                avg($tableRt.transferred_size) as transferred,
                avg($tableRt.decoded_size) as decoded,
                $localTable.user_agent_os,
                $localTable.user_agent_family,
                count(*)
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where $tableRt.encoded_size > $(UP.sizeMin) and
                $tableRt.url ilike '$(fileType)'
            group by
                $localTable.user_agent_family,
                $localTable.user_agent_os
            order by encoded desc, transferred desc, decoded desc
        """);

        displayTitle(chart_title = "Pages Details (Min $(UP.sizeMin) KB), File Type $(fileType)", chart_info = [TV.timeString], showTimeStamp=false)
        #scrubUrlToPrint(joinTables,limit=150)
        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceSummaryLRFIMB Exception ",y)
    end
end

function resourceSizes2Old(TV::TimeVars,UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*),
            CASE WHEN (position('?' in $localTable.params_u) > 0) then trim('/' from (substring($localTable.params_u for position('?' in substring($localTable.params_u from 9)) +7))) else trim('/' from $localTable.params_u) end as urlgroup,
            $tableRt.url
        from $localTable join $tableRt
            on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(UP.sizeMin) and
            ($tableRt.url ilike '$(LV.fileType)' or $tableRt.url ilike '$(LV.fileType)?%')
        group by $localTable.params_u,$tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(SP.showLines,end),:])
    catch y
        println("resourceSizes2 Exception ",y)
    end
end

function resourceSizes12(UP::UrlParams,fileType::ASCIIString;linesOut::Int64=25,minEncoded::Int64=1000)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            $localTable.user_agent_os,
            $localTable.user_agent_family,
            $tableRt.url,
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*)
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(minEncoded) and
        $tableRt.url not like '%/interactive-assets/%' and $tableRt.url ilike '$(fileType)'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os,
            $tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(linesOut,end),:])
    catch y
        println("bigTable5 Exception ",y)
    end
end

function lookForLeftOversALR(UP::UrlParams,linesOutput::Int64)

    joinTables = DataFrame()

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            $localTable.user_agent_os,
            $localTable.user_agent_family,
            $localTable.user_agent_device_type,
            $tableRt.url,
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*)
        from $localTable join $tableRt
        on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > 1 and
        $tableRt.url not ilike '%/interactive-assets/%' and
        $tableRt.url not ilike '%png' and
        $tableRt.url not ilike '%svg' and
        $tableRt.url not ilike '%jpg' and
        $tableRt.url not ilike '%mp3' and
        $tableRt.url not ilike '%mp4' and
        $tableRt.url not ilike '%gif' and
        $tableRt.url not ilike '%wav' and
        $tableRt.url not ilike '%jog' and
        $tableRt.url not ilike '%js' and
        $tableRt.url not ilike '%.js?%' and
        $tableRt.url not ilike '%css' and
        $tableRt.url not ilike '%ttf' and
        $tableRt.url not ilike '%woff%'
        group by
            $localTable.user_agent_family,
            $localTable.user_agent_os,
            $localTable.user_agent_device_type,
            $tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        beautifyDF(joinTables[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversALR Exception ",y)
    end
    #display(joinTables)
end

function lookForLeftOversDetailsALR(UP::UrlParams,linesOutput::Int64)

    joinTables = DataFrame()

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
            select
                $tableRt.url,
                avg($tableRt.encoded_size) as encoded,
                avg($tableRt.transferred_size) as transferred,
                avg($tableRt.decoded_size) as decoded,
                $localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
                $localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
                $localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd,
                $tableRt.initiator_type,$tableRt.height,$tableRt.width,$tableRt.x,$tableRt.y,
                count(*)
            from $localTable join $tableRt
                on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
            where $tableRt.encoded_size > 1 and $tableRt.url not like '%/interactive-assets/%'
            group by $tableRt.url,
                $localTable.compression_types,$localTable.domain,$localTable.geo_netspeed,$localTable.mobile_connection_type,$localTable.params_scr_bpp,$localTable.params_scr_dpx,$localTable.params_scr_mtp,$localTable.params_scr_orn,params_scr_xy,
                $localTable.user_agent_family,$localTable.user_agent_major,$localTable.user_agent_minor,$localTable.user_agent_mobile,$localTable.user_agent_model,$localTable.user_agent_os,$localTable.user_agent_osversion,$localTable.user_agent_raw,
                $localTable.user_agent_manufacturer,$localTable.user_agent_device_type,$localTable.user_agent_isp,$localTable.geo_isp,$localTable.params_ua_plt,$localTable.params_ua_vnd,
                $tableRt.initiator_type,$tableRt.height,$tableRt.width,$tableRt.x,$tableRt.y
            order by encoded desc
        """);

        beautifyDF(joinTables[1:min(linesOutput,end),:])
    catch y
        println("lookForLeftOversDetailsALR Exception ",y)
    end
end

function resourceImages(TV::TimeVars,UP::UrlParams,SP::ShowParams,fileType::ASCIIString)

    try
        localTable = UP.btView
        tableRt = UP.resourceTable

        joinTables = query("""\
        select
            avg($tableRt.encoded_size) as encoded,
            avg($tableRt.transferred_size) as transferred,
            avg($tableRt.decoded_size) as decoded,
            count(*),
            $tableRt.url
        from $localTable join $tableRt
            on $localTable.session_id = $tableRt.session_id and $localTable."timestamp" = $tableRt."timestamp"
        where $tableRt.encoded_size > $(UP.sizeMin) and
            ($tableRt.url ilike '$(fileType)' or $tableRt.url ilike '$(fileType)?%') and
            $tableRt.url ilike 'http://www.nationalgeographic.com%'
        group by $tableRt.url
        order by encoded desc, transferred desc, decoded desc
        """);

        if (SP.debugLevel > 4)
            beautifyDF(joinTables[1:min(SP.showLines,end),:])
        end

        return joinTables
    catch y
        println("resourceImage Exception ",y)
    end
end

function idImageMgrPolicy(SP::ShowParams,imageDf::DataFrame)
    try

        urlPatterns = knownPatterns()
        for (url in imageDf[:,:url])
            found = false
            for key in keys(urlPatterns)
                if (ismatch(key,url))
                    value = get(urlPatterns,key,"None")
                    #println("Found ",value)
                    found = true
                    urlPatterns[key] = [value[1],value[2],value[3],value[4],value[5]+1,url]
                    break
                end
            end
            if (!found)
                #println("Missed ",url)
                regExpStart = search(url,".com")[2]+3
                regExpStr = url[regExpStart:end]
                regExpEnd = min(60,length(regExpStr))
                regExpStr = regExpStr[1:regExpEnd]

                #println("(r\".*",regExpStr)
                println("Not Found: (r\".*",regExpStr,"\",[\"\",\"\",\"",regExpStr,"\",\"",regExpStr,"\",0,\"\"]),")
#(r".*/adventure/features/everest/first-woman-to-climb-everest-jun",["/adventure/features/everest/first-woman-to-climb-everest-jun"","",",0,""]),
            end
        end

        println("\n\n\nHere are other patterns matched")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0)
                println("$(value[5])\tKnown Keys: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end


    catch y
        println("idImageMgrPolicy Exception ",y)
    end
end


function knownPatterns()
    try

        urlPatterns = Dict([

            (r".*/content/dam/.*",["Image Mgr(1)","Default",".*/content/dam/.*","Content Dam",0,""]),
            (r".*/interactive-assets/.*",["","",".*/interactive-assets/.*","Interactive Assets",0,""]),

#            (r".*/content/dam/travel/.*",["Content Dam Travel",0,""]),
#            (r".*/content/dam/photography/.*",["Photography",0,""]),
#            (r".*/content/dam/adventure/.*",["Content Dam Adventure",0,""]),
#            (r".*/content/dam/archaeologyandhistory/.*",["Content Dam Archaeologyandhistory",0,""]),
#            (r".*/content/dam/magazine/.*",["Content Dam Magazine",0,""]),
#            (r".*/content/dam/environment/.*",["Content Dam Environment",0,""]),
#            (r".*/content/dam/news/.*",["content Dam News",0,""]),
#            (r".*/content/dam/science/.*",["Content Dam Science",0,""]),
#            (r".*/content/dam/contributors/.*",["Content Dam Contributors",0,""]),
#            (r".*/content/dam/natgeo/video/.*",["Content Dam Video",0,""]),
#            (r".*/content/dam/parks/.*",["Content Dam Parks",0,""]),
#            (r".*/content/dam/animals/.*",["Content Dam Animals",0,""]),
#            (r".*/content/dam/ngdotcom/.*",["Content Dam Ngdotcom",0,""]),
#            (r".*/content/dam/peopleandculture/.*",["Content Dam People and Culture",0,""]),
#            (r".*/content/dam/books/.*",["Content Dam Books",0,""]),

            (r".*/adventure/features/.*",["","",".*/adventure/features/.*","Adventure Features",0,""]),
            (r".*/contributors/r/melody-rowell/.*",["","",".*/contributors/r/melody-rowell/.*","Contributors Melody Rowell",0,""]),
            (r".*/countryman/assets/.*",["","","/countryman/assets/.*","Countryman",0,""]),
            (r".*/foodfeatures/.*",["","",".*/foodfeatures/.*","Food Features",0,""]),
            (r".*/new-york-city-skyline-tallest.*",["","",".*/new-york-city-skyline-tallest.*","New York Skyline",0,""]),
            (r".*/cosmic-dawn/.*",["","",".*/cosmic-dawn/.*","Cosmic Dawn",0,""]),
            (r".*/taking-back-detroit/.*",["","",".*/taking-back-detroit/.*","Taking Back Detroit",0,""]),
            (r".*/americannile/.*",["","",".*/americannile/.*","American Nile",0,""]),
            (r".*/healing-soldiers/.*",["","",".*/healing-soldiers/.*","Healing Soldiers",0,""]),
            (r".*/environment/global-warming/.*",["","",".*/environment/global-warming/.*","Global Warming",0,""]),
            (r".*/magazines/pdf/.*",["","",".*/magazines/pdf/.*","Magazines Pdf",0,""]),
            (r".*/worldlegacyawards/.*",["","",".*/worldlegacyawards/.*","World Legacy Awards",0,""]),
            (r".*/news-features/son-doong-cave/.*",["","",".*/news-features/son-doong-cave/.*","News Features Son-doong-cave",0,""]),
            (r".*/trajan-column/.*",["","",".*/trajan-column/.*","Trajan Column",0,""]),
            (r".*/magazines/l/multisubs/images/.*",["","",".*/magazines/l/multisubs/images/.*","Magazines Multisubs",0,""]),
            (r".*/astrobiology/.*",["","",".*/astrobiology/.*","Astrobiology",0,""]),
            (r".*/alwaysexploring/.*",["","",".*/alwaysexploring/.*","Always Exploring",0,""]),
            (r".*/annual-report-.*",["","",".*/annual-report-.*","Annual Report",0,""]),
            (r".*/china-caves/.*",["","",".*/china-caves/.*","China Caves",0,""]),
            (r".*/clean-water-access-.*",["","",".*/clean-water-access-.*","Clean Water Access",0,""]),
            (r".*/climate-change/.*",["","",".*/climate-change/.*","Climate Change",0,""]),
            (r".*/discoverjapancontest/.*",["","",".*/discoverjapancontest/.*","Discover Japan Contest",0,""]),
            (r".*/gecpartnershowcase/.*",["","",".*/gecpartnershowcase/.*","Gec Partner Showcase",0,""]),
            (r".*/giftguide/.*",["","",".*/giftguide/.*","Gift Guide",0,""]),
            (r".*/hubble-timeline/.*",["","",".*/hubble-timeline/.*","Hubble Timeline",0,""]),
            (r".*/hurricane-katrina-new-orleans.*",["","",".*/hurricane-katrina-new-orleans.*","Hurricane Katrina",0,""]),
            (r".*/impact-report-.*",["","",".*/impact-report-.*","Impact Report",0,""]),
            (r".*/journeytojordan/.*",["","",".*/journeytojordan/.*","Journey To Jordan",0,""]),
            (r".*/love-collection-.*",["","",".*/love-collection-.*","Love Collection",0,""]),
            (r".*/loveswitzerland/.*",["","",".*/loveswitzerland/.*","Love Switzerland",0,""]),
            (r".*/magazine/201.*",["","",".*/magazine/201.*","Magazine 20xx",0,""]),
            (r".*/memorablemoments/.*",["","",".*/memorablemoments/.*","Memorable Moments",0,""]),
            (r".*/mindsuckers/.*",["","",".*/mindsuckers/.*","Mindsuckers",0,""]),
            (r".*/myaway/.*",["","",".*/myaway/.*","Myaway",0,""]),
            (r".*/people-and-culture/.*",["","",".*/people-and-culture/.*","People And Culture",0,""]),
            (r".*/promo/ngtseminars/.*",["","",".*/promo/ngtseminars/.*","Promo Ngtseminars",0,""]),
            (r".*/staralliance20/.*",["","",".*/staralliance20/.*","Star Alliance",0,""]),
            (r".*/sunrise-to-sunset/.*",["","",".*/sunrise-to-sunset/.*","Sunrise To Sunset",0,""]),
            (r".*/tracking-ivory/.*",["","",".*/tracking-ivory/.*","Tracking Ivory",0,""]),
            (r".*/travelmarketplace/.*",["","",".*/travelmarketplace/.*","Travel Marketplace",0,""]),
            (r".*/usofadventure/.*",["","",".*/usofadventure/.*","Us Of Adventure",0,""]),
            (r".*/voteyourpark/.*",["","",".*/voteyourpark/.*","Vote Your Park",0,""]),
            (r".*/west-snow-fail/.*",["","",".*/west-snow-fail/.*","West Snow Fail",0,""]),
            (r".*/year-in-review-.*",["","",".*/year-in-review-.*","Year In Review",0,""]),


            (r".*/visitpandora/.*",["Sponsor","None",".*/visitpandora/.*","Visit Pandora",0,""]),
            (r".*/microsoft/.*",["Sponsor","None",".*/microsoft/.*","Microsoft",0,""]),
            (r".*/stellaartois/.*",["Sponsor","None",".*/stellaartois/.*","Stella Artois",0,""]),
            (r".*/subaru/.*",["Sponsor","None",".*/subaru/.*","Subaru",0,""]),
            (r".*/visitcalifornia/.*",["Sponsor","None",".*/visitcalifornia/.*","Visit California",0,""]),
            (r".*/cisco/.*",["Sponsor","None",".*/cisco/.*","Cisco",0,""]),


            (r".*/unchartedwaters/.*",["Sponsor","None",".*/unchartedwaters/.*","Unchartedwaters",0,""])
            ]);

        return urlPatterns

    catch y
        println("knownPatterns Exception ",y)
    end
end
