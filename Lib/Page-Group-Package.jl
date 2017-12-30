function customRefPGD(TV::TimeVars,UP::UrlParams)
    customReferralsTable(TV,UP)
end

function treemapsPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    deviceTypeTreemap(TV,UP,SP)
    browserFamilyTreemap(TV,UP,SP)
    countryTreemap(TV,UP,SP)
end
