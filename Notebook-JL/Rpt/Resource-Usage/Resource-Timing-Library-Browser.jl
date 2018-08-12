using RT;
using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "beacons_4744_rt"
sessions = "beacons_4744_sessions";

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = RESOURCE_TABLE)
setTable(sessions; tableType = SESSIONS_TABLE);

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

browser = "Chrome Mobile";
worstResource = "http://www.site.com/assets/css/fonts/fontawesome-webfont.woff?v=4.5.0";
conversionMetric = :custom_metrics_1;
setConversionMetric(conversionMetric);

domain = getDomains(TV.startTimeUTC, TV.endTimeUTC)[1]

# Resource Analysis
#---
#Analysis using the `RT` library for the resources used on `www.site.com`.

chartConversionsByBrowser(TV.startTimeUTC, TV.endTimeUTC);

# For official documentation, try using: ?chartResourceStats
# The value passed by domain indicates what is considered a first party.  All non-matching resources will be considered 3rd party.
# The dimension argument indicate how you want the resources to be grouped.  Possible arguments: RESOURCE_TYPE (default; img, css, etc.), PG (page group), OS (Windows, Mac OS X, etc.),
#  Browser (Chrome, IE, Firefox, etc.), DEVICE_TYPE (Desktop, Mobile, etc.), COUNTRY_CODE (United States, Germany, UK, etc.), and beacon columns are also usable.
chartResourceStats(domain, TV.startTimeUTC, TV.endTimeUTC; dimension = BROWSER);

#(**Note**: `Mobile Safari` and `Safari` do not show up in the resource analysis because `resource timing` is _not_ supported by those browsers.  Resources that appear under `Mobile Safari` and `Safari` should be ignored.)

# Resource Load Times by Resource Type
#---
#The following graphs use box-plots to compare the different groups of data.  If you're not familiar with box-plots, watch this <a href="https://www.youtube.com/watch?v=b2C9I8HuCe4&feature=youtu.be&t=16">short video</a> to understand how they work.

# For official documentation, try using: ?chartLoadTimeStats
# The dimension argument indicate how you want the load-times to be grouped.  Possible arguments: RESOURCE_TYPE (default; img, css, etc.), PG (page group), OS (Windows, Mac OS X, etc.),
#  Browser (Chrome, IE, Firefox, etc.), DEVICE_TYPE (Desktop, Mobile, etc.), COUNTRY_CODE (United States, Germany, UK, etc.), and beacon columns are also usable.
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
chartLoadTimeStats(TV.startTimeUTC, TV.endTimeUTC; dimension = BROWSER, url = domain, isFirstParty = false);

# Top 10 Worst Resources on Chrome Mobile (with at least 5,000 requests)
#---

# For official documentation, try using: ?chartResources
# Only the resources coming from the specified browser will be considered.
# If the amount of resources associated with a specific resource URL is less than 5,000, exclude it from the results.
# The file arguments allows you to save to the results to a .csv file with the provided file name (ex. browserResources.csv).  Removing the outputFile
#  argument will allow you to use the inputFile as the result to graph.  If you want to do a live demo this can be a quick way to show results without
#  having to run the query.  The outputFile argument is set to the file to allow you to create a new or overwrite an old data file.  Using the inputFile argument
#  will short-circuit the function call with a prepared result so remove it if you want to run the DB query to get new results.
file = "browserResources";
#chartResources(startTime, endTime; browser = browser, minResourceCount = 5_000, inputFile = file);
chartResources(TV.startTimeUTC, TV.endTimeUTC; browser = browser, minResourceCount = 5000, outputFile = file);

# Trending Resource For Resource Load Time Insight
#---

# For official documentation, try using: ?chartResourceTrend
# Only the resources coming from the specified browser will be considered.
# This showTable argument allow you to hide the table and only show the chart, if it does not add to the presentation.
file = "resourceTrending";
#chartResourceTrend(worstResource, startTime, endTime, datepart; browser = browser, inputFile = file);
chartResourceTrend(worstResource, TV.startTimeUTC, TV.endTimeUTC, TV.datepart; browser = browser, outputFile = file);

#The graph below is the same graph (as the one above) but showing the median, 75th and 95th percentiles.

# For official documentation, try using: ?chartResourceTrend
# Only the resources coming from the specified browser will be considered.
# This showTable argument allow you to hide the table and only show the chart, if it does not add to the presentation.
# Percentiles argument allow you to enter the percentiles of load times you want to graph so you have a better idea of the load time spread over time.
chartResourceTrend(worstResource, TV.startTimeUTC, TV.endTimeUTC, TV.datepart; browser = browser, percentiles = AbstractFloat[0.5, 0.75, 0.9])
