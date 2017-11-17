function firstAndLastBeaconReport(TV::TimeVars,UP::UrlParams)
    limitedTable(TV,UP)
    setTable(UP.btView)
    firstAndLast = getBeaconsFirstAndLast()
end
