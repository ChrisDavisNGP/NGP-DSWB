using RT;

dsn = "vfdsn";
beacons = "anonymous_beacons";
resources = "$(beacons)_rt";

topPageGroup = "Product";

sessions = "$(beacons)_sessions";
resourceLT = "$(resources)_lt";

setRedshiftEndpoint(dsn);
setTable(beacons);
setTable(resources; tableType = RESOURCE_TABLE);
setTable(sessions; tableType = SESSIONS_TABLE);

getBeaconsFirstAndLast()

startTime = getBeaconsFirstAndLast()[symbol("First Beacon")][1];
endTime = getBeaconsFirstAndLast()[symbol("Last Beacon")][1];

domain = getDomains(startTime, endTime)[1]

# Resource Analysis
---
Analysis using the `RT` library for the resources used on `www.site.com`.

chartActivityImpactByPageGroup(startTime, endTime);

# For official documentation, try using: ?chartResourceStats
# The value passed by domain indicates what is considered a first party.  All non-matching resources will be considered 3rd party.
chartResourceStats(domain, startTime, endTime);

# Resource Load Times by Resource Type
---
The following graphs use box-plots to compare the different groups of data.  If you're not familiar with box-plots, watch this <a href="https://www.youtube.com/watch?v=b2C9I8HuCe4&feature=youtu.be&t=16">short video</a> to understand how they work.

# For official documentation, try using: ?chartLoadTimeStats
# The dimension argument indicate how you want the load-times to be grouped.  Possible arguments: RESOURCE_TYPE (default; img, css, etc.), PG (page group), OS (Windows, Mac OS X, etc.),
#  Browser (Chrome, IE, Firefox, etc.), DEVICE_TYPE (Desktop, Mobile, etc.), COUNTRY_CODE (United States, Germany, UK, etc.), and beacon columns are also usable.
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
chartLoadTimeStats(startTime, endTime; dimension = PG, url = domain, isFirstParty = false);

# Resource Load Times for Resource Servers
---

# For official documentation, try using: ?chartResourceServerStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
# The file arguments allows you to save to the results to a .csv file with the provided file name (ex. rssProduct.csv).  Removing the outputFile
#  argument will allow you to use the inputFile as the result to graph.  If you want to do a live demo this can be a quick way to show results without
#  having to run the query.  The outputFile argument is set to the file to allow you to create a new or overwrite an old data file.  Using the inputFile argument
#  will short-circuit the function call with a prepared result so remove it if you want to run the DB query to get new results.
file = "rssProduct";
#chartResourceServerStats(startTime, endTime; url = domain, isFirstParty = false, pageGroup = topPageGroup, inputFile = file);
chartResourceServerStats(startTime, endTime; url = domain, isFirstParty = false, pageGroup = topPageGroup, outputFile = file);

# For official documentation, try using: ?chartTopNLoadTimeDistributions
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
# The outliersOnly argument allows you to just show the top outliers amongst the servers listed above.
# The serverFile lets you skip having to query again for the domains list already provided above and only run the query to get the load time distribution for each
#   of the server domains in the list.
chartTopNLoadTimeDistributions(startTime, endTime; url = domain, isFirstParty = false, pageGroup = topPageGroup, outliersOnly = true, inputFileServers = "rssProduct");
