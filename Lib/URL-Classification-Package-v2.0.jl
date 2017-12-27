include("URL-Classification-Data-v2.0.jl")

function classifyUrl(SP::ShowParams,toppageurl::DataFrame)
    try
        i = 0
        todo = 0
        for url in toppageurl[:,:urlgroup]
            i += 1
            #@show url
            #if (SP.debugLevel > 8)
            #    println("url ",url)
            #end

            #if (ismatch(r"data:application.*",url))
            if (!ismatch(r"^http.*",url))
                if (SP.reportLevel > 4)
                    println("Problem data: ",url)
                end
                toppageurl[i:i,:urlpagegroup] = "Not HTTP String"
                continue
            end

            uri = ""
            newUrlPageGroup = "Not Done"
            if (url == "Not Blocking")
                continue
            end

            #remove bad characters
            url = replace(url,"!","")

            try
                uri = URI(url)
            catch
                if (SP.reportLevel > 4)
                    println("Problem url:",url)
                end
                toppageurl[i:i,:urlpagegroup] = "Bad URL String"
                continue
            end

            findHost = lookupHost(uri.host)
            if (findHost != "NoneInner")
                newUrlPageGroup = findHost
                toppageurl[i:i,:urlpagegroup] = findHost
            elseif (haskey(WellKnownPath,uri.path))
                newUrlPageGroup = get(WellKnownPath,uri.path,"None2")
                toppageurl[i:i,:urlpagegroup] = newUrlPageGroup
            else
                newuristring = "None"
                newuristring = PartialKnownHost(uri.host)
                if (newuristring == "To Classify")
                    newuristring = PartialKnownPath(uri.path)

                    if (newuristring == "To Classify")
                        if ((ismatch(r".*cloudfront.net",uri.host)) && (ismatch(r".*jpg",uri.path)))
                            newuristring = "AWS Cloud JPG File"
                        elseif ((ismatch(r".*cloudfront.net",uri.host)) && (ismatch(r".*png",uri.path)))
                            newuristring = "AWS Cloud PNG File"
                        end
                    end

                    if (newuristring == "To Classify")
                        todo += 1
                        #@show todo uri.host  uri.path
                        if (ismatch(r"^.*",uri.host))
                            #println("Host ", uri.host, " Path ",uri.path)
                            println("        (\"", uri.host,"\",\"",uri.host,"\"),")
                        end
                    end
                end
                if (newuristring == "None" && SP.reportLevel > 0)
                    println("Host ", uri.host, " Path ",uri.path, " *** None ***")
                        #println(uri.host)
                end

                toppageurl[i:i,:urlpagegroup] = newuristring
                newUrlPageGroup = newuristring
            end

            #println("newUrlPageGroup ",newUrlPageGroup," uri.path ",uri.path)
            returnValue = SubClassify(newUrlPageGroup,uri.path)
            toppageurl[i:i,:urlpagegroup] = returnValue
            #println("final Group ",toppageurl[i:i,:urlpagegroup])
        end
     catch y
        println("classifyUrl Exception ",y)
        println("exc: url=",url," uri=",uri)
    end

end

function SubClassify(urlPageGroup::ASCIIString,uriPath::ASCIIString)

    try
        newUrlPageGroup = urlPageGroup
        #println("Checking Broad Group",urlPageGroup)
        # subclassify a few groups
        if (urlPageGroup == "NGP Yourshot")
            #println("Classify Path ",uriPath)
            newUrlPageGroup = YourshotClassification(uriPath)
        end

        return newUrlPageGroup
     catch y
        println("subClassify Exception ",y)
    end

end

function gapAndCriticalPath(toppageurl::DataFrame)
    try
        # Start OF Gap & Critical Path Calculation

        toppageurl = names!(toppageurl[:,:],
        [Symbol("urlpagegroup"),Symbol("Start"),Symbol("Total"),Symbol("Redirect"),Symbol("Blocking"),Symbol("DNS"),
            Symbol("TCP"),Symbol("Request"),Symbol("Response"),Symbol("Gap"),Symbol("Critical"),Symbol("urlgroup"),
            Symbol("request_count"),Symbol("label"),Symbol("load_time"),Symbol("beacon_time")]);

        sort!(toppageurl,cols=[order(:Start),order(:Total,rev=true)]);

        toppageurl[:Gap] = 0
        toppageurl[:Critical] = 0

        #todo check the size to make sure at least 3 rows of data

        prevStartTime = toppageurl[1,:Start]
        prevTotalTime = toppageurl[1,:Total]
        i = 0
        toppageurl[:Critical] = toppageurl[1,:Total]

        for url in toppageurl[1:end,:urlgroup]
            i += 1
            toppageurl[i,:Gap] = 0
            toppageurl[i,:Critical] = 0

            newStartTime = toppageurl[i,:Start]
            newTotalTime = toppageurl[i,:Total]
            #println("Url ",url)

            #Sorted by start time ascending and largest total time decending
            #Anyone with same time has the previous is nested inside the current one and has no time

            if (newStartTime == prevStartTime && i > 1)
                #println("Matched line $i start time $newStartTime")
                toppageurl[i,:Gap] = 0
                toppageurl[i,:Critical] = 0
                continue
            end

            # did we have a gap?
            gapTime = newStartTime - prevStartTime - prevTotalTime

            # Negitive gap means we start inside someone else
            if (gapTime < 0)
                #nested request or overlapping but no gap already done toppageurl[i,:gap] = 0

                prevMarker = prevStartTime + prevTotalTime
                newMarker = newStartTime + newTotalTime

                #println("\tprevMarker=$prevMarker, newMarker=$newMarker")
                if (i == 1)
                  prevTotalTime = prevMarker
                  toppageurl[i,:Critical] = prevMarker
                  prevStartTime = 0
                  continue
                end

                if (prevMarker >= newMarker)
                    # Still nested inside a larger request already donetoppageurl[i,:critical] = 0
                    continue
                else
                    # Figure how much of new time is beyond end of old time
                    #println("nst=",newStartTime,",ntt=",newTotalTime,",nm=",newMarker,",pst=",prevStartTime,",ptt=",prevTotalTime,",pm=",prevMarker)
                    # When done we will pick up at the end of this newer overlapped request
                    prevTotalTime = newMarker - prevMarker
                    #println("ptt=",prevTotalTime)

                    # it is critical path but only the part which did not overlap with the previous request
                    toppageurl[i,:Critical] = newMarker - prevMarker
                    prevStartTime = prevMarker
                end

            else
                #println("gap time ",gapTime,",",newStartTime,",",newTotalTime,",",prevStartTime,",",prevTotalTime)
                toppageurl[i,:Gap] = gapTime
                # All of its time is critical path since this is start of a new range
                toppageurl[i,:Critical] = newTotalTime
                prevTotalTime = newTotalTime
                prevStartTime = newStartTime
            end
            # move on
            runningTime = sum(toppageurl[:,:Gap]) + sum(toppageurl[:,:Critical])
            #println("rt", runningTime, " at ",prevStartTime)

        end

        # Do not fix last record.  It is the "Not Blocking" Row.  Zero it out
        #i += 1
        #toppageurl[i,:Gap] = 0
        #toppageurl[i,:Critical] = 0

        return toppageurl

     catch y
        println("gapAndCritcalPath Exception ",y)
    end
end

function classifyUrlGroup(SP::ShowParams,summaryDF::DataFrame)
    try
        i = 0
        todo = 0
        #println("Starting classifyUrlGroup")
        #beautifyDF(summaryDF)

        for urlGroupItem in summaryDF[:,:urlgroup]
            i += 1
            findSummary = "Other"
            findSummary = lookupUrlGroup(urlGroupItem)
            summaryDF[i:i,:urlgroup] = findSummary
        end

     catch y
        println("classifyUrlGroup Exception ",y)
    end

end


function wellKnownPathDictionary(debug::Bool)
  if debug == true
    # There is no debug Path so far
    wellKnownPathDictionary()
  else
    wellKnownPathDictionary()
  end
end

function wellKnownHostEncyclopedia(debug::Bool)
    if debug == true
      wellKnownHostEncyclopediaDebug()
    else
      wellKnownHostEncyclopedia()
    end

end

function wellKnownHostEncyclopedia()

    VolumeA = VolumeAList()
    VolumeB = VolumeBList()
    VolumeC = VolumeCList()
    VolumeD = VolumeDList()
    VolumeE = VolumeEList()
    VolumeF = VolumeFList()
    VolumeG = VolumeGList()
    VolumeH = VolumeHList()
    VolumeI = VolumeIList()
    VolumeJ = VolumeJList()
    VolumeK = VolumeKList()
    VolumeL = VolumeLList()
    VolumeM = VolumeMList()
    VolumeN = VolumeNList()
    VolumeO = VolumeOList()
    VolumeP = VolumePList()
    VolumeQ = VolumeQList()
    VolumeR = VolumeRList()
    VolumeS = VolumeSList()
    VolumeT = VolumeTList()
    VolumeU = VolumeUList()
    VolumeV = VolumeVList()
    VolumeW = VolumeWList()
    VolumeX = VolumeXList()
    VolumeY = VolumeYList()
    VolumeZ = VolumeZList()

    VolumeOther = VolumeOtherList()

    WellKnownHostDirectory = Dict([
    ("A",VolumeA),
    ("B",VolumeB),
    ("C",VolumeC),
    ("D",VolumeD),
    ("E",VolumeE),
    ("F",VolumeF),
    ("G",VolumeG),
    ("H",VolumeH),
    ("I",VolumeI),
    ("J",VolumeJ),
    ("K",VolumeK),
    ("L",VolumeL),
    ("M",VolumeM),
    ("N",VolumeN),
    ("O",VolumeO),
    ("P",VolumeP),
    ("Q",VolumeQ),
    ("R",VolumeR),
    ("S",VolumeS),
    ("T",VolumeT),
    ("U",VolumeU),
    ("V",VolumeV),
    ("W",VolumeW),
    ("X",VolumeX),
    ("Y",VolumeY),
    ("Z",VolumeZ),
    ("Other",VolumeOther)
    ])

    return WellKnownHostDirectory
end

# Debug skips all the extra volumes
function wellKnownHostEncyclopediaDebug()

    VolumeA = VolumeAList()
    VolumeOther = VolumeOtherList()

    WellKnownHostDirectory = Dict([
    ("A",VolumeA),
    ("Other",VolumeOther)
    ])

    return WellKnownHostDirectory
end

function lookupHost(host::ASCIIString)

    hs = uppercase(host[1])
    hostStart = string(hs)
    #println("[",host,"] and [",hostStart,"]")

    try
        if (haskey(WellKnownHostDirectory,hostStart))
            #println("Fetch Volume ",hostStart)
            Volume = get(WellKnownHostDirectory,hostStart,"NoVolume")
        else
            Volume = get(WellKnownHostDirectory,"Other","NoVolume")
        end

        newUrlPageGroup = "NoneInner"
        if (haskey(Volume,host))
            #println("Fetch Host ",host)
            newUrlPageGroup = get(Volume,host,"NoneInner")
        end

        #println("New Group ",newUrlPageGroup)
        #println("")

        return newUrlPageGroup
    catch y
        println("lookupHost Exception",y)
        return "NoneInner"
    end

end

function lookupUrlGroup(urlGroup::ASCIIString)

    try
        #println("lookupUrlGroup ",urlGroup)
        if (haskey(WellKnownUrlGroup,urlGroup))
            return get(WellKnownUrlGroup,urlGroup,"Other")
        else
            println("\(\"$urlGroup\",\"\"),")
            return "Other"
        end

    catch y
        println("lookupUrlGroup Exception",y)
        return "NoneInner"
    end

end
