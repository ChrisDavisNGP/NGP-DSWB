function customRefPGD(TV::TimeVars,UP::UrlParams)
    customReferralsTable(TV,UP)
end

function stdRefPGD(TV::TimeVars,UP::UrlParams)
    standardReferrals(UP.btView,UP.pageGroup,TV.startTimeUTC,TV.endTimeUTC,TV.timeString; limit=UP.limitRows)
end

function treemapsPGD(TV::TimeVars,UP::UrlParams)
    deviceTypeTreemap(TV,UP)
    browserFamilyTreemap(TV,UP)
    countryTreemap(TV,UP)
end
