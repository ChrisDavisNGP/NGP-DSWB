using RT;
using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift endpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"
sessions = "beacons_4744_sessions";

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = RESOURCE_TABLE)
setTable(sessions; tableType = SESSIONS_TABLE);

include("../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)
#
#---

# SOASTA 3rd Party Resource Analysis

#This notebook analyzes 3rd party resource performance using the `RT` library. It is divided into four sections:
#
#1. **Setup/Configuration**
#    > Which database is used and what is the name of the table the beacons exist in?

#1. **What is the scope of my third party usage?**
#    > How many resources were served from my site and each of my page groups, and how many of those resources were from third party domains?

#2. **Where are my problem areas?**
#    > Who are my worst performing third party resource providers? How does third party performance break down by resource type or device type?

#3. **Break down a page for me.**
#    > Take the page group with the highest resource count, and break down the top 25 worst 3rd party performers for that group.

# A value called slowDomain is set at the end of this notebook.  It is located in the code block below:
#   How are the resource load times distributed for a given resource provider?
# For different customers, that value should be changed.  See code comments in the code block for help.

TV.startTimeUTC = getBeaconsFirstAndLast()[Symbol("First Beacon")][1];
TV.endTimeUTC = getBeaconsFirstAndLast()[Symbol("Last Beacon")][1];

domain = getDomains(TV.startTimeUTC, TV.endTimeUTC)[1]

### Create the Load Time Table Based off of the Resource Table

# The following code will create the load time table based on the full time range of the resource table.
# This is recommended if you are doing different time range analysis for the client.
createLoadTimeTable(getBeaconsFirstAndLast(; table = tableRt)[Symbol("First Beacon")][1], getBeaconsFirstAndLast(; table = tableRt)[Symbol("Last Beacon")][1]);

# 2. What is the Scope of my Third Party Usage?
#** Eliminating 3rd party performance issues begins with understanding the importance of 3rd party resources to your overall site performance **

## How Many Resources Are on My Site and What is the Breakdown Between 1st and 3rd Parties for My Top Page Groups?
#The table below breaks down 1st and 3rd party resource beacon count and percentage for each page group and the entire site

# For official documentation, try using: ?chartResourceStats
# The value passed by domain indicates what is considered a first party.  All non-matching resources will be considered 3rd party.
chartResourceStats(domain, TV.startTimeUTC, TV.endTimeUTC);

# 3. Where are my problem areas?
#** By looking at the load times through different dimension comparisons, a larger picture concerning the biggest problem-areas for the site may come to light. **

## Break Down Performance by Resource Provider

#This tree map allows you to see relative demand and performance of 3rd parties by different dimensions. The chart shows you relative overall resource performance by resource domains.  The size of the boxes indicates the relative beacon count for that domain, the color indicates the median performance indicated by the respective beacons. The key for what each color means is below the graph.

#Tree maps are a powerful way to quickly discover specific problem areas on which to focus your attention, and to understand the relative importance of each element within a given beacon set.

# For official documentation, try using: ?chartTreemapResources
# Treemap dimensions list have the following (short-cut/tab-complete) options:
#   COUNTRY_CODE (United States, UK, etc.), DEVICE_TYPE (Desktop, Mobile, Tablet, etc.), RESOURCE_TYPE (img, css, link, etc.), OS (Windows, Mac OS X, etc.), BROWSER (Chrome,
#   IE, Firefox, etc.), Page Group (Homepage, Category, Checkout, etc.).
# You can use specific dimensions from for the above short-cuts or DB column names directly.  Example: :user_agent_os (equivalent to OS), :compression_types,
#   :http_method, :geo_rg, etc.
# Only one dimension can be used at this time to ensure accuracy of data.
chartTreemapResources(TV.startTimeUTC, TV.endTimeUTC; dimensions = [DEVICE_TYPE]);

# For official documentation, try using: ?chartTreemapResources
# Treemap dimensions list have the following (short-cut/tab-complete) options:
#   COUNTRY_CODE (United States, UK, etc.), DEVICE_TYPE (Desktop, Mobile, Tablet, etc.), RESOURCE_TYPE (img, css, link, etc.), OS (Windows, Mac OS X, etc.), BROWSER (Chrome,
#   IE, Firefox, etc.), Page Group (Homepage, Category, Checkout, etc.).
# You can use specific dimensions from for the above short-cuts or DB column names directly.  Example: :user_agent_os (equivalent to OS), :compression_types,
#   :http_method, :geo_rg, etc.
# Only one dimension can be used at this time to ensure accuracy of data.
deviceType = "Desktop"
chartTreemapResources(TV.startTimeUTC, TV.endTimeUTC; deviceType = deviceType, dimensions = [RESOURCE_URL], chartTitle = deviceType);

# For official documentation, try using: ?chartTreemapResources
# Treemap dimensions list have the following (short-cut/tab-complete) options:
#   COUNTRY_CODE (United States, UK, etc.), DEVICE_TYPE (Desktop, Mobile, Tablet, etc.), RESOURCE_TYPE (img, css, link, etc.), OS (Windows, Mac OS X, etc.), BROWSER (Chrome,
#   IE, Firefox, etc.), Page Group (Homepage, Category, Checkout, etc.).
# You can use specific dimensions from for the above short-cuts or DB column names directly.  Example: :user_agent_os (equivalent to OS), :compression_types,
#   :http_method, :geo_rg, etc.
# Only one dimension can be used at this time to ensure accuracy of data.
deviceType = "Mobile"
chartTreemapResources(TV.startTimeUTC, TV.endTimeUTC; deviceType = deviceType, dimensions = [RESOURCE_URL], chartTitle = deviceType);

# For official documentation, try using: ?chartTreemapResources
# Treemap dimensions list have the following (short-cut/tab-complete) options:
#   COUNTRY_CODE (United States, UK, etc.), DEVICE_TYPE (Desktop, Mobile, Tablet, etc.), RESOURCE_TYPE (img, css, link, etc.), OS (Windows, Mac OS X, etc.), BROWSER (Chrome,
#   IE, Firefox, etc.), Page Group (Homepage, Category, Checkout, etc.).
# You can use specific dimensions from for the above short-cuts or DB column names directly.  Example: :user_agent_os (equivalent to OS), :compression_types,
#   :http_method, :geo_rg, etc.
# Only one dimension can be used at this time to ensure accuracy of data.
deviceType = "Tablet"
chartTreemapResources(TV.startTimeUTC, TV.endTimeUTC; deviceType = deviceType, dimensions = [RESOURCE_URL], chartTitle = deviceType);

#- For more details concerning resource analysis using `resource types` (images, CSS, etc.), try <a href="../Resource%20Timing%20Library%20-%20Resources.ipynb">here</a>.
#- For more details concerning resource analysis using `page groups`, try <a href="Resource%20Timing%20Library%20-%20Page%20Group.ipynb">here</a>.

## How Do Load Times Break Down by Resource Type?
#The following graphs use box-plots to compare the different groups of data.  If you're not familiar with box-plots, watch this <a href="https://www.youtube.com/watch?v=b2C9I8HuCe4&feature=youtu.be&t=16">short video</a> to understand how they work.

# For official documentation, try using: ?chartLoadTimeStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
chartLoadTimeStats(TV.startTimeUTC, TV.endTimeUTC; url = domain, isFirstParty = false);

#** For more details concerning resource analysis using `resource types` (images, CSS, etc.), try <a href="../Resource%20Timing%20Library%20-%20Resources.ipynb#Resource-Analysis">here</a>.

## How Do Load Times Break Down by Device Type?

# For official documentation, try using: ?chartLoadTimeStats
# The dimension argument indicate how you want the load-times to be grouped.  Possible arguments: RESOURCE_TYPE (default; img, css, etc.), PG (page group), OS (Windows, Mac OS X, etc.),
#  Browser (Chrome, IE, Firefox, etc.), DEVICE_TYPE (Desktop, Mobile, etc.), COUNTRY_CODE (United States, Germany, UK, etc.), and beacon columns are also usable.
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
# The file arguments allows you to save to the results to a .csv file with the provided file name (ex. fullDayDevType3rdParty.csv).  Removing the outputFile
#  argument will allow you to use the inputFile as the result to graph.  If you want to do a live demo this can be a quick way to show results without
#  having to run the query.  The outputFile argument is set to the file to allow you to create a new or overwrite an old data file.  Using the inputFile argument
#  will short-circuit the function call with a prepared result so remove it if you want to run the DB query to get new results.
file = "fullDayDevType3rdParty";
# chartLoadTimeStats(TV.startTimeUTC, TV.endTimeUTC; dimension = DEVICE_TYPE, url = domain, isFirstParty = false, inputFile = file);
chartLoadTimeStats(TV.startTimeUTC, TV.endTimeUTC; dimension = DEVICE_TYPE, url = domain, isFirstParty = false, outputFile = file);

#** For more details concerning resource analysis using `device types`, try <a href="../Resource%20Timing%20Library%20-%20Device%20Type.ipynb#Resource-Analysis">here</a> and if it's `browser` specific breakdowns needed, try <a href="../Resource%20Timing%20Library%20-%20Browser.ipynb#Resource-Analysis">here</a>.

## How Do Load Times Break Down by Resource Providers?

# For official documentation, try using: ?chartResourceServerStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
chartResourceServerStats(TV.startTimeUTC, TV.endTimeUTC; url = domain, isFirstParty = false);

#---

# 4. Break Down a Page Group For Me

## What is the Breakdown of Resource Domain Performance For A Single Page Group?

# For official documentation, try using: ?chartResourceServerStats
# The url indicates the first-party resource domain to identify the resources against.
# Setting the isFirstParty to false indicates you want to look at all resources that do not come from the domain provided in the url argument.
# The file arguments allows you to save to the results to a .csv file with the provided file name (ex. rssProduct.csv).  Removing the outputFile
#  argument will allow you to use the inputFile as the result to graph.  If you want to do a live demo this can be a quick way to show results without
#  having to run the query.  The outputFile argument is set to the file to allow you to create a new or overwrite an old data file.  Using the inputFile argument
#  will short-circuit the function call with a prepared result so remove it if you want to run the DB query to get new results.
file = "rssProduct";
#chartResourceServerStats(TV.startTimeUTC, TV.endTimeUTC; url = domain, isFirstParty = false, pageGroup = UP.pageGroup, inputFile = file);
chartResourceServerStats(TV.startTimeUTC, TV.endTimeUTC; url = domain, isFirstParty = false, pageGroup = UP.pageGroup, outputFile = file);

##How are the resource load times distributed for a given resource provider?
#A load time histogram is a powerful way of visualizing how resources from a given provider are affecting user experience, and answering questions like how caching, network latency, and other factors are impacting that load time.

# For official documentation, try using: ?chartLoadTimeDistribution
# The domain to look at is taken from the domain servers listed above. (#25)
slowDomain = "http://recs.coremetrics.com"
chartLoadTimeDistribution(slowDomain, TV.startTimeUTC, TV.endTimeUTC)

##Which are the worst resources in this page group, and which are the most important to address?
#This bar chart displays the median load times for the worst individual resources in this page group. The blue bar is the median load time, and the green dot is the volume of resource requests for this resource type. The idea is to begin troubleshooting the worst resources with the biggest impact on user experience in order to have the biggest impact on conversion or page views.

# For official documentation, try using: ?chartResources
# The url indicates the resource domain to identify the resources against.
# The minimum resource count set will allow you to weed out the resources with just 1 or 2 requests that always populate the slowest resources.  The value
#  should be based on how popular the resource server chosen is.  If the server has over 1M hits, then 1_000 - 10_000 can be used.  If it's 100K - 1M then
#  100 - 1_000 can be used.  If it's less than 10K - 100K, then as low as 10 - 1_000 can be used, depending on how diverse the resource URLs are.
minResourceCount = 100;
chartResources(TV.startTimeUTC, TV.endTimeUTC; url = slowDomain, minResourceCount = minResourceCount);

##More Page Group Data
#For more details concerning resource analysis using `page groups`, try <a href="Resource%20Timing%20Library%20-%20Page%20Group.ipynb#Resource-Analysis">here</a>.
