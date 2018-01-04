function curlJsonWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams)

    if CU.synthetic
        finalDict = syntheticCommands(TV,UP,SP,CU)
    else
        println("NR Type not yet defined")
        return
    end

    return finalDict

end

function curlCommands(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams)

    if CU.apiAdminKey != "no id"
        Key = CU.apiAdminKey
    else
        Key = "unknown"
    end

    if CU.syntheticListAllMonitors
        curlCommand = "https://synthetics.newrelic.com/synthetics/api/v3/monitors"
    elseif CU.syntheticListOneMonitor
        curlCommand = "https://synthetics.newrelic.com/synthetics/api/v3/monitors/" * CU.syntheticCurrentMonitorId
    else
        curlCommand = "unknown command"
    end

    # Todo regular expression tests for "unknown" and report failure and return empty

    curlStr1 = ["-v","-H","X-Api-Key:$Key","$curlCommand"]
    curlCmd = `curl $curlStr1`
    jsonString = readstring(curlCmd)

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

    return jsonString

end

function syntheticCommands(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams)

    #  List all syn monitors
    #   curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors'
    if CU.syntheticListAllMonitors
        jsonInput = curlCommands(TV,UP,SP,CU)
        finalDict = curlSyntheticListAllMonitorJson(TV,UP,SP,CU,jsonInput)
        return finalDict
    end

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'
    if CU.syntheticListOneMonitor
        jsonInput = curlCommands(TV,UP,SP,CU)
        finalDict = curlSyntheticListOneMonitorJson(TV,UP,SP,CU,jsonInput)
        return finalDict
    end

end

function curlSyntheticListOneMonitorJson(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams,jList::ASCIIString)

    if SP.debugLevel > 0
        println("jList=",jList)
    end

    jParsed = JSON.parse(jList)

    println(jParsed)
    println(typeof(jParsed))
    println(get(jParsed,"frequency","not found"))

    return jParsed
end


function curlSyntheticListAllMonitorJson(TV::TimeVars,UP::UrlParams,SP::ShowParams,CU::CurlParams,jList::ASCIIString)

    #urlListDF = newPagesList(UP,SP)
    #listToUseDV = urlListDF[:urlgroup] * "%"
    #finalListToUseDV = cleanupTopUrlTable(listToUseDV)

end


function timeSizeRequestsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams,newRelicDict::Dict)
    openingTitle(TV,UP,SP)

end
