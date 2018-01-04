function curlJsonWorkflow(TV::TimeVars,UP::UrlParams,SP::ShowParams)

  #openingTitle(TV,UP,SP)

  urlListDF = newPagesList(UP,SP)
  listToUseDV = urlListDF[:urlgroup] * "%"
  finalListToUseDV = cleanupTopUrlTable(listToUseDV)

  if (SP.debugLevel > 8)
      beautifyDF(urlListDF[1:min(10,end),:])
  end

  # the data should now be organized to use further

end
