## Tables and Data Source setup

using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser
using JSON

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,5,9,16,0,2017,5,9,16,59)
TV = prevWorkWeekTimeVariables()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

theList = "
{
    \"data\": [
        {\"attributes\": {\"publication_datetime\": \"2017-09-09T13:46:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/luxor-egypt-necropolis-discovery/\"}, \"id\": \"News:1bc85a25-ffaa-4563-827a-13d2813d87c9\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:1bc85a25-ffaa-4563-827a-13d2813d87c9/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-09T04:01:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/dog-brain-feelings-mri-gregory-berns/\"}, \"id\": \"News:2f184b2b-02d8-42e4-bc8f-74d73e2e555c\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:2f184b2b-02d8-42e4-bc8f-74d73e2e555c/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-09T04:01:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/go--baby--these-animal-babies-grow-up-without-any-help-from-pare/\"}, \"id\": \"News:4615c229-ad95-482f-b6f7-f34d778ebf56\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:4615c229-ad95-482f-b6f7-f34d778ebf56/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-09T04:00:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/wild-big-cats-lions-tigers-hunting-video-spd/\"}, \"id\": \"News:43a5768e-8dff-4a97-9e05-ab1f2e968e36\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:43a5768e-8dff-4a97-9e05-ab1f2e968e36/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T23:04:45.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/black-market-pet-trade-ecuador-frogs-spd/\"}, \"id\": \"News:0e0d3041-435c-4b65-abb0-7dfd9b9ec204\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:0e0d3041-435c-4b65-abb0-7dfd9b9ec204/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T22:36:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/irma-heat-map-ocean-warming-spd/\"}, \"id\": \"News:6a64d29c-8224-4e71-9dcd-13419b3ac4a3\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:6a64d29c-8224-4e71-9dcd-13419b3ac4a3/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T20:37:02.000000Z\", \"uri\": \"http://www.nationalgeographic.com/science/space/equinoxes/\"}, \"id\": \"Science:8a81c0d8-abf7-47ff-8d58-1c9b8aa7faf1\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Science:8a81c0d8-abf7-47ff-8d58-1c9b8aa7faf1/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T20:10:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/adventure/lists/trails/epic-trails-around-world/\"}, \"id\": \"Adventure:47671232-645c-4a6a-9ab4-d8cae40d2782\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Adventure:47671232-645c-4a6a-9ab4-d8cae40d2782/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T16:56:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/photography/proof/2017/09/mexico-earthquake-tsunami-disaster-spd/\"}, \"id\": \"Photography:21378388-046b-467a-8aaa-35d2b5051216\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Photography:21378388-046b-467a-8aaa-35d2b5051216/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T16:52:13.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/cassini-saturn-pictures-nasa-grand-finale-space-science/\"}, \"id\": \"News:001ea20b-fdff-4696-aec1-e702a20088a1\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:001ea20b-fdff-4696-aec1-e702a20088a1/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T16:43:16.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/irma-most-intense-hurricane-florida-keys-1935-history/\"}, \"id\": \"News:b6e77a11-5421-4788-9452-185646784c07\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:b6e77a11-5421-4788-9452-185646784c07/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T16:03:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/travel/destinations/asia/things-to-do-bethlehem-west-bank-wall-palestine-tourism/\"}, \"id\": \"Travel:b2bc6f28-1f8e-4520-9629-df2b386b5141\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Travel:b2bc6f28-1f8e-4520-9629-df2b386b5141/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-08T13:39:36.000000Z\", \"uri\": \"http://www.nationalgeographic.com/adventure/lists/best-places-schools-learn-adventure-sport/\"}, \"id\": \"Adventure:2e8ed93e-91bf-4187-9011-14ac8224858b\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Adventure:2e8ed93e-91bf-4187-9011-14ac8224858b/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T22:09:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/cassini-saturn-nasa-grand-finale-space-science/\"}, \"id\": \"News:ef5f760c-e0f3-490a-99b8-207cab34eda2\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:ef5f760c-e0f3-490a-99b8-207cab34eda2/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T21:17:53.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/hurricane-hunter-irma-flight-video-spd/\"}, \"id\": \"News:d98ca5be-24bc-4031-8b82-b20fd495a131\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:d98ca5be-24bc-4031-8b82-b20fd495a131/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T18:04:22.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/sea-angels-mating-arctic-video-spd/\"}, \"id\": \"News:d95dcc44-d6b3-4c18-bbac-c91bb47654ee\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:d95dcc44-d6b3-4c18-bbac-c91bb47654ee/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T15:33:03.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/wildlife-watch-exotic-pets-popular-china/\"}, \"id\": \"News:7d97f43c-3a13-468b-81d7-8db39d802a62\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:7d97f43c-3a13-468b-81d7-8db39d802a62/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T11:00:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/news/2013/01/watch-rare-footage-of-endangered-cubs-in-the-wild-/\"}, \"id\": \"News:d0c35135-229c-4014-9bd6-453baf331569\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:d0c35135-229c-4014-9bd6-453baf331569/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T04:01:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/magazine/2017/09/explore-health-brazilian-pepper-tree/\"}, \"id\": \"NGM:d30c77e0-7b94-4605-be40-7053acb5ede5\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/NGM:d30c77e0-7b94-4605-be40-7053acb5ede5/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T04:01:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/hurricane-irma-harvey-pets-video/\"}, \"id\": \"News:64405d1f-433b-4c43-a8bb-3dd7152c261d\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:64405d1f-433b-4c43-a8bb-3dd7152c261d/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-07T04:01:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/magazine/2017/09/basic-instincts-zebrafish-transparent-biomedical-research/\"}, \"id\": \"NGM:f9d6ea48-2dfe-4c1e-8eca-1f49bc22bc88\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/NGM:f9d6ea48-2dfe-4c1e-8eca-1f49bc22bc88/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T23:11:15.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/sun-solar-flare-strongest-auroras-space-science/\"}, \"id\": \"News:9bf9f3bb-576d-47b0-83e4-97fd8c48f9dd\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:9bf9f3bb-576d-47b0-83e4-97fd8c48f9dd/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T21:28:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/photography/proof/2017/09/poconos-catskills-abandoned-resorts-photos-spd/\"}, \"id\": \"Photography:f11bda95-ad3b-4768-89df-1bcc33628331\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Photography:f11bda95-ad3b-4768-89df-1bcc33628331/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T21:16:27.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/hurricane-irma-harvey-season-climate-change-weather/\"}, \"id\": \"News:7ba74f2c-abf6-4f14-921a-b455d7970fac\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:7ba74f2c-abf6-4f14-921a-b455d7970fac/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T18:40:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/north-korea-pyongyang-life-culture-tourism-spd/\"}, \"id\": \"News:d5e6330b-3444-4d98-bec6-8c440630f52e\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:d5e6330b-3444-4d98-bec6-8c440630f52e/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T17:56:05.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/canada-lynx-calls-maine-video-spd/\"}, \"id\": \"News:da5e086a-8232-4654-87bf-d3189b486db4\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:da5e086a-8232-4654-87bf-d3189b486db4/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T17:52:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/voyager-40-years-nasa-interstellar-space-science/\"}, \"id\": \"News:31043fd0-cf0b-4516-bd75-999ed719e1d4\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:31043fd0-cf0b-4516-bd75-999ed719e1d4/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T14:18:34.000000Z\", \"uri\": \"http://www.nationalgeographic.com/travel/destinations/europe/denmark/smart-cities-aarhus-denmark/\"}, \"id\": \"Travel:f3f9d4a1-d35b-4a4e-8299-a30da498962e\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Travel:f3f9d4a1-d35b-4a4e-8299-a30da498962e/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-06T14:16:26.000000Z\", \"uri\": \"http://www.nationalgeographic.com/travel/destinations/europe/finland/smart-cities-helsinki-finland/\"}, \"id\": \"Travel:177322ff-0f6b-4036-b0ba-814f08483a95\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Travel:177322ff-0f6b-4036-b0ba-814f08483a95/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T23:01:15.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/african-wild-dogs-vote-by-sneezing/\"}, \"id\": \"News:625aa0da-004f-4d27-83e4-8294d7d9e59a\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:625aa0da-004f-4d27-83e4-8294d7d9e59a/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T21:34:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/photography/proof/2017/09/hurricane-harvey-texas-houston-portraits/\"}, \"id\": \"Photography:c8391d83-be96-4f83-ba94-9105f460c9e1\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Photography:c8391d83-be96-4f83-ba94-9105f460c9e1/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T19:28:21.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/how-category-5-hurricanes-form-conditions-weather/\"}, \"id\": \"News:8de277ec-c80e-468b-8b86-2f128e436da4\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:8de277ec-c80e-468b-8b86-2f128e436da4/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T16:49:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/sponsor-content-can-computer-models-turn-the-tide-against-flood-damage/\"}, \"id\": \"News:04598b87-eaa9-4726-9c49-adab22b11bd8\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:04598b87-eaa9-4726-9c49-adab22b11bd8/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T16:05:55.000000Z\", \"uri\": \"http://www.nationalgeographic.com/travel/destinations/europe/italy/milan-things-to-do/\"}, \"id\": \"Travel:4b684b45-26a4-426c-ad47-2dff31094fb3\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Travel:4b684b45-26a4-426c-ad47-2dff31094fb3/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T16:02:44.000000Z\", \"uri\": \"http://www.nationalgeographic.com/photography/proof/2017/09/california-wildfires-drought/\"}, \"id\": \"Photography:71eba7d0-7c90-4548-8eb5-a4540c3dc3e2\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/Photography:71eba7d0-7c90-4548-8eb5-a4540c3dc3e2/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T15:22:19.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/09/mars-national-geographic-channel-tv-show-tour-360-video/\"}, \"id\": \"News:a02abefb-8d90-4a8c-b9d6-ede0bad7bfc0\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:a02abefb-8d90-4a8c-b9d6-ede0bad7bfc0/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T14:24:57.000000Z\", \"uri\": \"http://www.nationalgeographic.com/archaeology-and-history/magazine/2017/09-10/saigo-takamori-the-last-samurai/\"}, \"id\": \"History:2fbcc589-9cf1-44de-9090-9e83e2557e3d\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/History:2fbcc589-9cf1-44de-9090-9e83e2557e3d/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T12:56:00.000000Z\", \"uri\": \"http://www.nationalgeographic.com/magazine/2017/09/proof-border-wall-united-states-mexico/\"}, \"id\": \"NGM:5e14bda4-b577-44b2-9c94-112a5ede1f9b\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/NGM:5e14bda4-b577-44b2-9c94-112a5ede1f9b/?format=jsonapi\"}, \"type\": \"article:story\"},
        {\"attributes\": {\"publication_datetime\": \"2017-09-05T12:52:00.000000Z\", \"uri\": \"http://news.nationalgeographic.com/2017/08/sponsor-content-earmarked-for-cars-and-cups/\"}, \"id\": \"News:b57ca9af-a4e9-4591-8c44-a83c169457d2\", \"links\": {\"self\": \"https://relay.nationalgeographic.com/proxy/distribution/api/v1/item/News:b57ca9af-a4e9-4591-8c44-a83c169457d2/?format=jsonapi\"}, \"type\": \"article:story\"}],\"meta\": {
        \"pages\": 1,
        \"feeds\": 39
    },
    \"links\": {\"first\": \"https://relay.nationalgeographic.com/proxy/distribution/feed/v1/jsonapi/?fields=uri%2Cpublication_datetime\u0026publication_datetime=7d\u0026content_type=article%3Astory\u0026page=1\", \"last\": \"https://relay.nationalgeographic.com/proxy/distribution/feed/v1/jsonapi/?fields=uri%2Cpublication_datetime\u0026publication_datetime=7d\u0026content_type=article%3Astory\u0026page=1\", \"self\": \"https://relay.nationalgeographic.com/proxy/distribution/feed/v1/jsonapi/?fields=uri%2Cpublication_datetime\u0026publication_datetime=7d\u0026content_type=article%3Astory\"},
    \"jsonapi\": {
        \"version\": \"1.0\"
    }
}
";

if (UP.useJson)
    urlListDF = newPagesList()
    if (SP.debugLevel > 4)
        beautifyDF(urlListDF[1:min(3,end),:])
    end

    topUrlList = urlListDF[:urlgroup]
    topUrls = cleanupTopUrlTable(topUrlList)
    if (SP.debugLevel > 0)
        display(topUrls)
    end
end
;

# Time
if (UP.useJson)
    finalUrlTableOutput(TV,UP,SP,topUrls)
end
