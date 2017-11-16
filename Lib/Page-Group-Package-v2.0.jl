function customRefPGD()
    customReferralsTable(localTable,productPageGroup)
end

function stdRefPGD()
    standardReferrals(localTable,productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString; limit=15)
end

function treemapsPGD()
    deviceTypeTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
    browserFamilyTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
    countryTreemap(productPageGroup,tv.startTimeUTC,tv.endTimeUTC,tv.timeString)
end

