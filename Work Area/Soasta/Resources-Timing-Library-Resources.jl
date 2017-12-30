using RT;

dsn = "vfdsn";
beacons = "anonymous_beacons";
resources = "$(beacons)_rt";

sessions = "$(beacons)_sessions";
resourceLT = "$(resources)_lt";

setRedshiftEndpoint(dsn);
setTable(beacons);
setTable(resources; tableType = RESOURCE_TABLE);
setTable(sessions; tableType = SESSIONS_TABLE);

getBeaconsFirstAndLast()

startTime = getBeaconsFirstAndLast()[symbol("First Beacon")][1];
endTime = getBeaconsFirstAndLast()[symbol("Last Beacon")][1];
resourceType = "img";
resourceType2 = "script";

domain = getDomains(startTime, endTime)[1]

# Resource Analysis
---
Analysis using the `RT` library for the resources used on `www.site.com`.

# For official documentation, try using: ?chartResourceStats
# The value passed by domain indicates what is considered a first party.  All non-matching resources will be considered 3rd party.
# The dimension argument indicate how you want the resources to be grouped.  Possible arguments: RESOURCE_TYPE (default; img, css, etc.), PG (page group), OS (Windows, Mac OS X, etc.),
#  Browser (Chrome, IE, Firefox, etc.), DEVICE_TYPE (Desktop, Mobile, etc.), COUNTRY_CODE (United States, Germany, UK, etc.), and beacon columns are also usable.
chartResourceStats(domain, startTime, endTime; dimension = RESOURCE_TYPE);

# Resource Load Times by Resource Type
---

# For official documentation, try using: ?chartResources
# The resourceType indicates the indicator type (img, css, etc.) to identify the resources by.
# The minimum resource count set will allow you to weed out the resources with just 1 or 2 requests that always populate the slowest resources.  The value
#  should be based on how popular the resource server chosen is.  If the server has over 1M hits, then 1_000 - 10_000 can be used.  If it's 100K - 1M then
#  100 - 1_000 can be used.  If it's less than 10K - 100K, then as low as 10 - 1_000 can be used, depending on how diverse the resource URLs are.
chartResources(startTime, endTime; resourceType = resourceType2, minResourceCount = 5_000);

The following graphs use box-plots to compare the different groups of data.  If you're not familiar with box-plots, watch this <a href="https://www.youtube.com/watch?v=b2C9I8HuCe4&feature=youtu.be&t=16">short video</a> to understand how they work.

# For official documentation, try using: ?chartLoadTimeStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
chartLoadTimeStats(startTime, endTime; url = domain, isFirstParty = false);

# For official documentation, try using: ?chartResources
# The resourceType indicates the indicator type (img, css, etc.) to identify the resources by.
# The minimum resource count set will allow you to weed out the resources with just 1 or 2 requests that always populate the slowest resources.  The value
#  should be based on how popular the resource server chosen is.  If the server has over 1M hits, then 1_000 - 10_000 can be used.  If it's 100K - 1M then
#  100 - 1_000 can be used.  If it's less than 10K - 100K, then as low as 10 - 1_000 can be used, depending on how diverse the resource URLs are.
chartResources(startTime, endTime; resourceType = resourceType, minResourceCount = 5_000);

# For official documentation, try using: ?chartResourceServerStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
# The file arguments allows you to save to the results to a .csv file with the provided file name (ex. rssProduct.csv).  Removing the outputFile
#  argument will allow you to use the inputFile as the result to graph.  If you want to do a live demo this can be a quick way to show results without
#  having to run the query.  The outputFile argument is set to the file to allow you to create a new or overwrite an old data file.  Using the inputFile argument
#  will short-circuit the function call with a prepared result so remove it if you want to run the DB query to get new results.
file = "chartResServers";
#chartResourceServerStats(startTime, endTime; url = domain, isFirstParty = false, resourceType = resourceType, inputFile = file);
chartResourceServerStats(startTime, endTime; url = domain, isFirstParty = false, resourceType = resourceType, outputFile = file);
