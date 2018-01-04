function curlJsonWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    if isdefined(:gNRSynthetic)
        finalDF = syntheticCommands(TV,UP,SP)
    else
        println("NR Type not yet defined")
        return
    end

  #urlListDF = newPagesList(UP,SP)
  #listToUseDV = urlListDF[:urlgroup] * "%"
  #finalListToUseDV = cleanupTopUrlTable(listToUseDV)

  if (SP.debugLevel > 8)
      beautifyDF(finalDF[1:min(10,end),:])
  end

  return finalDF

end

function curlCommands(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    # todo add Curl structure
    curlSynthetic = true
    curlSyntheticListAllMonitors = true
    curlSyntheticListOneMonitor = true
    curlSyntheticCurrentMonitorId = "69599173-5b61-41e0-b4e6-ba69e179bc70"
    curlApiAdminKey = "-H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'"
    curlJsonFilename = UP.jsonFilename

    curlStr = "curl -v "
    if curlSynthetic
        curlStr = curlStr * curlApiAdminKey * " "
    else
        curlStr = curlStr * "-H 'X-Api-Key:unknown'" * " "
    end

    if curlSyntheticListAllMonitors
        curlStr = curlStr * "'https://synthetics.newrelic.com/synthetics/api/v3/monitors'"
    elseif curlSyntheticListOneMonitor
        curlStr = curlStr * "'https://synthetics.newrelic.com/synthetics/api/v3/monitors/" *
            "$curlSyntheticCurrentMonitorId'" * " "
    else
        curlStr = curlStr * "'unknown Command'"
    end

    # Todo regular expression tests for "unknown" and report failure and return empty

    run(curlStr |> "$curlJsonFilename")

    #  List all syn monitors
    #   curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors'

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d'
    #'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'

end

function syntheticCommands(TV::TimeVars,UP::UrlParams,SP::ShowParams)

    #  List all syn monitors
    #   curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors'
    curlSyntheticListAllMonitors = true
    if curlSyntheticListAllMonitors
        curlCommands(TV,UP,SP)
        finalDF = curlSyntheticListOneMonitorJson(TV,UP,SP)
        return finalDF
    end

    # Picked syn monitor "JTP-Gallery-Equinox-M"
    #  curl -v  -H 'X-Api-Key:b2abadd58593d10bb39329981e8b702d' 'https://synthetics.newrelic.com/synthetics/api/v3/monitors/69599173-5b61-41e0-b4e6-ba69e179bc70'
    curlSyntheticListOneMonitor = true
    if curlSyntheticListOneMonitor
        curlCommands(TV,UP,SP)
        finalDF = curlSyntheticListOneMonitorJson(TV,UP,SP)
        return finalDF
    end
    
end


function timeSizeRequestsWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    openingTitle(TV,UP,SP)

end
