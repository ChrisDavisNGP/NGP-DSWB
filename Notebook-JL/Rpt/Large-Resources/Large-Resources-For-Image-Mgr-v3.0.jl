using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

include("../../../Lib/Include-Package-v2.1.jl")

#TV = timeVariables(2017,6,17,10,0,2017,6,17,10,59);
TV = weeklyTimeVariables(days=7);

UP = UrlParamsInit(scriptName)
UP.pageGroup = "%"   #productPageGroup
UP.urlRegEx = "%"   #localUrl
UP.deviceType = "Mobile"
UP.sizeMin = 200000
UP.timeLowerMs = 10       # 10 ms not 1 sec
UP.timeUpperMs = 9000000  # 9 million not 600k only care about size

SP = ShowParamsInit()
SP.debugLevel = 0
SP.showLines = 10

defaultBeaconCreateView(TV,UP,SP)

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

        println("\n\n\nHere are the patterns matched for large files:\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Known")
                println("$(value[5])\tKnown: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Ignoring")
                println("$(value[5])\tIgnore: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "Sponsor")
                println("$(value[5])\tSponsor: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")

        for key in keys(urlPatterns)
            value = get(urlPatterns,key,"None")
            if (value[5] > 0 && value[2] == "")
                println("$(value[5])\tPending: \"",value[1],"\"",",\"",value[2],"\",\"",value[3],"\",\"",value[4],"\",",value[5],",\"",value[6],"\"")
            end
        end

        println("\n\n")


    catch y
        println("idImageMgrPolicy Exception ",y)
    end
end


function knownPatterns()
    try

        urlPatterns = Dict([

            (r".*/content/dam/.*",["Image Mgr","Known",".*/content/dam/.*","Content Dam",0,""]),
            (r".*/interactive-assets/.*",["Interactive Assets","Ignoring",".*/interactive-assets/.*","Interactive Assets",0,""]),

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


            (r".*/visitpandora/.*",["Sponsor","Sponsor",".*/visitpandora/.*","Visit Pandora",0,""]),
            (r".*/microsoft/.*",["Sponsor","Sponsor",".*/microsoft/.*","Microsoft",0,""]),
            (r".*/stellaartois/.*",["Sponsor","Sponsor",".*/stellaartois/.*","Stella Artois",0,""]),
            (r".*/subaru/.*",["Sponsor","Sponsor",".*/subaru/.*","Subaru",0,""]),
            (r".*/visitcalifornia/.*",["Sponsor","Sponsor",".*/visitcalifornia/.*","Visit California",0,""]),
            (r".*/cisco/.*",["Sponsor","Sponsor",".*/cisco/.*","Cisco",0,""]),


            (r".*/unchartedwaters/.*",["Sponsor","Sponsor",".*/unchartedwaters/.*","Unchartedwaters",0,""])
            ]);

        return urlPatterns

    catch y
        println("knownPatterns Exception ",y)
    end
end

fileType = "%jpg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%png"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%jpeg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%gif"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%imviewer"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%svg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

fileType = "%jpeg"
imagesDf = resourceImages(TV,UP,SP,fileType)
if (size(imagesDf)[1] > 0)
    idImageMgrPolicy(SP,imagesDf)
end

q = query(""" drop view if exists $(UP.btView);""")
;
