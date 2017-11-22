function customRefPGD(TV::TimeVars,UP::UrlParams)
    customReferralsTable(UP.btView,UP.pageGroup)
end

function stdRefPGD(TV::TimeVars,UP::UrlParams)
    standardReferrals(UP.btView,UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC,TV.timeString; limit=UP.limitRows)
end

function treemapsPGD(TV::TimeVars,UP::UrlParams)
    deviceTypeTreemap(UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC,TV.timeString)
    browserFamilyTreemap(UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC,TV.timeString)
    countryTreemap(UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC,TV.timeString)
end
