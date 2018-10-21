function customRefPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    customReferralsTable(TV,UP,SP)
end

function treemapsPGD(TV::TimeVars,UP::UrlParams,SP::ShowParams)
    deviceTypeTreemap(TV,UP,SP)
    browserFamilyTreemap(TV,UP,SP)
    countryTreemap(TV,UP,SP)
end
