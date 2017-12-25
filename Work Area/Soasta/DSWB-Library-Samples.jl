# 2. The DSWB Library

This tutorial will focus on the capabilities that comes with the `DSWB` library and will walk you trough some of its available functions. Topics will include:
+ Basic functions
+ Functions that return tables
+ Functions that display charts
+ Function arguments and syntax
+ Load Times
+ Concurrent Sessions
+ Top Page Groups, Landing Pages & Referrers
+ Conversion Analysis
    + Conversion Rates vs Average Load Times
    + Conversions by OS
    + Page Groups by Conversion Impact
+ External Referrers

***

## 2.1 Basic Functions

Let's start wtih a running a couple of basic functions that you will likely want to call when you first connect to your beacon data. However, you first need to include the `DSWB` library.  Afterwards, you can connect to your Redshift back-end and set the name of your beacons table. Feel free to replace the retailer DSN and beacon table name in the cell below with your own DSN and table name, and then run to initialize these 3 things.

using DSWB

dsn = "retailerdsn"
beaconsTable = "retailer_beacons"

setRedshiftEndpoint(dsn)
setTable(beaconsTable)

# Note: you can run a cell with the PARAMS global dictionary to verify that the appropriate parameter initializations have been made after calling the above functions
# PARAMS

Now run the below function to get the date range for the beacons that you have currently loaded into Redshift. This function simply finds and reports the first and last beacons, by date, in your beacons table.

getBeaconsFirstAndLast()

Run the below cell to get the total number of beacons in your beacon table.

getBeaconCount()

Note that the above function does a little more than a simple `COUNT(*)` SQL query - it filters out beacons that have a non-NULL value for the `params_rt_quit` field. It is not necessary to go too in-depth as to what this beacon parameter means, but it is a field that indicates whether the beacon was sent on `unload` of the page. If `params_rt_quit` is NULL, this is a beacon that fired on the `unload` event, and since we already count beacons that are sent on the page `onload`, we don't want to count each page twice.

***

## 2.2 Function Arguments

In the functions that were called above we did not pass any arguments. However, most functions that are made available from the `DSWB` library have mandatory arguments, optional arguments, or both. In particular, almost all of the functions in `Charting` and `Analysis`, included in `DSWB`, take at least `startTime` and `endTime` parameters.  This allows you to query against your beacon data for a specific date range of your choosing, rather than against the whole beacon set. Other functions might take optional arguments, such as a page group name or url. Usually these optional arguments are *keyword arguments*.  *Keyword arguments* require you to pass a parameter identifier along with the parameter's value.

Let's begin with running a function with the *starttime* and *endtime* parameters. These parameters are variables of type `DateTime` in the Julia programming language. In the cell below, let's define 2 `DateTime` variables. These variables can be initialized using the `DateTime` constructor, which is a method that takes arguments of the form `DateTime(Year,Month,Day,Hour,Minute,Second,Millisecond)`. Note that, you only have to specify the year when declaring DateTime variables - all other arguments are optional. For more information on using Dates in Julia, please see the [`Dates.jl` documentation](http://docs.julialang.org/en/latest/manual/dates/).

Change the `DateTime()` arguments below to specify a date range of 1 week, or perhaps 1 day, that is within your beacon range (within the date range defined by your First Beacon and Last Beacon).

# Choose a start and end time that is shorter than the 1 month gap defined in the example below.
# Remember: The longer the date range, the longer the runtime for each query.

year = 2014
startMonth = 9
endMonth = 10
startDay = 1
endDay = 1

startTime = DateTime(year, startMonth, startDay)
endTime = DateTime(year, endMonth, endDay)

Now, rerun the `getBeaconCount()` in the cell below, this time using your `DateTime` parameters that you've specified above.

# If startTime and endTime are set in an earlier cell, the values
# will carry over to later cells.

getBeaconCount(startTime, endTime)

Let's run another function to get the top Page Groups, by requests, for the date range we've already specified. Run the below cell

getGroupPercentages(startTime, endTime)

Now, let's run a function with an optional keyword argument. We'll return to the `getBeaconCount()` function, but this time we'll pass the optional `pageGroup` argument - an argument to count the beacons for a particular page group, rather than for all beacons. From the output above, pick any page group that is of interest to you. In the cell below, change the group name (currently specified as "Home") to the page group name you've chosen from the above output. Run the cell.

# Call the getBeaconCount() function with the group keyword paramaeter
# The count returned below should be the same as the count for this page group in the above output

pageGroupName = "Home"
getBeaconCount(startTime, endTime, pageGroup=pageGroupName)

Note that the `startTime` and `endTime` arguments are NOT keyword arguments, while the `pageGroup` argument is a keyword. The `getBeaconCount()` function also takes one other optional keyword argument - a `table` parameter, which defines which table in Redshift that you'd like to query against. Nearly all functions made available by `DSWB` will have this optional keyword argument, but usually the table is already defined by a prior call to `setTable()` as we've done near the top of this notebook. In any case, once you have defined all non-keyword arguments in a function call, the keyword arguments can get be inserted in any sequence you like - **order is not important for keyword arguments**. For more information on function arguments, please see the [functions documentation](http://docs.julialang.org/en/latest/manual/functions/).

***

## 2.3 Top Page Groups, Referrers & Landing Pages

Now lets get our top page groups, referrers and landing pages by number of requests. In an above example, we already returned a table of our top page groups. This was acheived by the `getTopPageGroups()` function. There are corresponding functions for referrers and landing pages as well. These functions all return a `DataFrame` into memory; in fact, most functions that have names prefaced with ***get*** will return a `DataFrame`, which is basically the in-memory version of the corresponding database table. Other functions that have names prefaced with ***chart*** will actually display a visualization for you, as well as return the corresponding `DataFrame`.

There are 3 functions we'll try out here:
* `getTopPageGroups()`
* `getTopReferrers()`
* `getTopLandingPages()`

These functions all take the typical *starttime* and *endtime* `DateTime` arguments, as well as an optional keyword argument `n`, which defines the number of distinct page groups/referrers/landing pages to return. This argument has a default value of *20* if left unspecified. Run the below cells, noting that we are storing the returned `DataFrames` by setting a variable equal to each function call.

numTopPageGroups = 25
topPageGroups = getTopPageGroups(startTime, endTime, n=numTopPageGroups)

topReferrers = getTopReferrers(startTime, endTime)

numTopLandingPages = 100
topLandingPages = getTopLandingPages(startTime, endTime, n=numTopLandingPages)

Let's make use of the function that will display a donut chart for these tables. This function is named `chartTopN()`; it requires the `startTime` and `endTime` arguments, and takes optional keywoard arguments `variable` and `n`. By default, `varable` = *pagegroups* and `n` = *20*. Run the below cells to get familiar with this function call.

typeOfChart = :pageGroups
numTopPageGroups = 25

chartTopN(startTime, endTime, variable=typeOfChart, n=numTopPageGroups)

typeOfChart = :referrers

# Default will chart the top 20 Referrers
chartTopN(startTime, endTime, variable=typeOfChart)

typeOfChart = :landingPages
numTopLandingPages = 5

chartTopN(startTime, endTime, variable=typeOfChart, n=numTopLandingPages)

***

## 2.4 Trending Data

Some of the most insightful data comes in the form of time series data - aggregated statistics over time. In this section, we'll look at time series charts for load times and concurrent sessions. The functions that display these charts will always take the typical *starttime* and *endtime* arguments, as well as a required *datepart* argument - a parameter that specifies the aggregate resolution. For example, we could look at Median load times over the last month, by day, by hour, by minute or even by second. The *datepart* argument would be either `:day`, `:hour`, `:minute` or `:second`. Notice the `:` that prefaces each of these values - this indicates that the *datepart* argument is of type `Symbol`, another variable type in the Julia programming language. In the `DSWB` library, all *datepart* arguments are of type: `Symbol`.

Let's start with charting load times over time. The `chartLoadTimes()` function plots the *front-end*, *back-end* and *full page* load times together, over time. Run each of the below cells, first changing the DateTime arguments to be dates that are compatible with your beacons time range.

**IMPORTANT NOTE:** The time range you are querying over, as well as the datepart resolution, will heavily impact the runtime of your query. The longer the time range (the greater the difference between the *starttime* and *endtime*) and the greater the resolution of the datepart (second > minute > hour > day > month > year), the more intense the load on the Redshift cluster to execute that query. So, for example, if you try to chart load times by second for a whole year, you may be waiting for a very long time for that query to complete!

# Chart median load times for 1 month, by day
year = 2014
startMonth = 11
endMonth = 12
startDay = 1
endDay = 1

startTime = DateTime(year, startMonth, startDay)
endTime = DateTime(year, endMonth, endDay)

chartLoadTimes(startTime, endTime, :day)

# Chart median load times for 1 week, by hour
endMonth = 11
startDay = 22
endDay = 30

startTime = DateTime(year, startMonth, startDay)
endTime = DateTime(year, endMonth, endDay)

chartLoadTimes(startTime, endTime, :hour)

#chart median load times for 1 day, by minute

startTime = DateTime(2014,11,27)
endTime = DateTime(2014,11,28)

chartLoadTimes(startTime, endTime,:minute)

Notice that, if you hover your mouse cursor over the chart, you'll get a tool-tip pop-up which will show you the individual load times for each datepart value. Additionally, you can zoom in and out using the mouse scrool-wheel. Also, you can disable/enable individual metrics by clicking on the desired item in the legend.

## 2.5 Conversion Analysis

## 2.5 Conversion Analysis

Most mPulse tenants and eCommerce applications have the concept of a *conversion*. A *conversion* occurs when a user session results in direct online revenue, such as adding a product to the shopping cart, submitting payment details and then confirming purchase. In DSWB, we typically differentiate converted sessions from unconverted sessions by first making use of the `setConversionGroup()` and `setConversionPage()` initialization functions to define what a conversion is. So, we are effectively defining whether a user session has converted by checking to see if that user reached either the conversion url (defined by `setConversionPage()`) or the conversion page group (defined by `setConversionGroup`).

Let's start by using the initialization functions to define our conversion page, conversion group, or both. Note the following:
* When using the `setConversionGroup()` function, use the exact name of the Page Group, as it has been defined in your mPulse tenant.
* When using the `setConversionPage()` function, you may use the full url string that defines your conversion page, or you can simply just use part of the url (e.g. "thankyou.jsp"). If the conversion page has a url with dynamic parameters in it (such as a session ID or maybe a product ID), then this a case where you'd want to set the conversion page url with only the relevant part of the url (the part of the url that is a common denomenator for all users).
* It is not necessary to define both the conversion page and conversion url, though you may do so if you wish.

Run the below cell, uncommenting at least one of the function calls, or both, first replacing the necessary text so that you are setting the page group and/or url that corresponds to conversions in YOUR web application.

# Un-comment at least one of the function calls below (both if you desire), and run the cell

# define your Conversion Group
conversionGroupName = "Order Review"
setConversionGroup(conversionGroupName)

# define your Conversion Page
conversionPageURL = "thankyou.jsp"
# setConversionPage(conversionPageURL)

To confirm that you have successfully initialized your values correctly, run the cell below.  It will output the current `TENANT_PARAMS` dictionary values for you to verify.

TENANT_PARAMS

You should have at least one of `CONVERSION_PAGE` or `CONVERSION_GROUP` set to the appropriate value(s).

Now, recall that we can return DataFrames with functions that are prefaced with ***get***, and we can display data visualizations (aka charts) with functions that are prefaced with ***chart***. In a code cell, if you type the text ***get***, for example, and then hit the **tab** key, you should see a little menu popup with autocomplete suggestions. You can use this technique for discovering function names that are prefaced with ***get*** as well as ***chart***.

In the following example, we'll use the `chartConversionsByOS` function display charts that summarize the conversion breakdown by operating system. In the below cell, change the date parameters appropriately so they fit within your beacon time range and then run the cell.

# This functions displays several charts to summarize the breakdown of conversions by OS
year = 2014
startMonth = 11
endMonth = 11
startDay = 27
daysPassed = 1

startTime = DateTime(year, startMonth, startDay)
endTime = DateTime(year, endMonth, startDay + daysPassed)

chartConversionsByOS(startTime, endTime)

There are 3 charts that are drawn - a donut chart to show sessions by OS, another donut chart to show conversions by OS, and a bar chart to rank the OS's by conversion rate. The conversion rate for each OS is effectively calculated by taking the number of total conversions (aka converted sessions) and dividing by the total number of sessions.

When dealing with Conversion Rates, it is necessary to pay attention to how many sessions are involved with the calculation. For example, with the conversions by OS, you may have one OS that has a very high conversion rate, but a very low session count relative to the other OS's. One of the most basic principles in statistics is that the bigger the sample size, or the larger the number of observations, the more accurately you can make estimatations from that set of data. So, you might see 1 OS with a 100% conversion rate, but with only 1 total session count - in other words, there was exactly 1 user with that particular OS, and that single user just happened to convert. That doesn't tell you a whole lot about the tendency of users with this OS to convert, and for that reason we also include total session counts and total conversion counts along with the conversion rates. Note that, in the bar chart ranking OS by conversion rate, only OS's with *significant* session counts are considered in the ranking. As currently configured, the session count for an OS has to be at least `0.0001 x N` where `N` is the sum of all session counts across all OS's. In other words, if an OS has a session count that is less than *0.01%* of all the sessions recorded, then that OS is not even considered in the conversion rate ranking

***

Now let's look at some charts which will help you visualize how sensitve conversion rates are to page load times. We will use the `chartConversionsVsLoadTimes()` function, which handily includes all of 4 different metrics into one chart:
1. Average Load Times in Seconds (aggregated to the nearest tenth of a second)
2. Number of Converted Sessions
3. Number of Unconverted Sessions
4. Conversion Rates

The primary relationship of interest is between conversion rates and average page load times. Run the below (change your *starttime* and *endtime* parameters if necessary).

# Chart Conversion Rates vs Load Times for all Pages

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

chartConversionsVsLoadTimes(startTime, endTime)

In the output above, you should see a chart with 2 y-axes using a stacked bar graph combined with a line graph. The x-axis represents average load times, aggregated to the nearest tenth of a second. For each average load time bucket, the bar graph shows the number of converted sessions & unconverted sessions, while the line graph shows the conversion rate. Similar to the *Conversions by OS* charts, this one is filtering out load time buckets that have *insignificant* session counts. Additionally, the chart itself will only show average load times up to about 15 or 16 seconds (the corresponding DataFrame that is returned below the chart contains ALL of the aggregated load time buckets).

Typically, these charts have session distributions that look similar to log-normal curves - like normal distributions (bell curves) stretched out to the right. What this means is that, as load times increase, there should be fewer and fewer session counts. For most applications, there should be very few sessions with average page load times greater than about 15 seconds. The key thing to keep in mind regarding this concept is that fewer sessions (e.g. fewer observations/data) result in more variability in the data. Again, this comes from the basic premise in Statistics that the smaller your sample size is, the more *random noise* you will have (e.g. higher variation and standard deviation). So, in your chart above, you may notice the conversion rate line become more and more choppy as it tails off towards the right. This is simply a consequence of the higher average load time buckets having fewer sessions, and thus more variability in the conversion rate metric.

Most importantly, this chart suggests what the ***Optimal Page Load Time*** is for your application. If you hover your mouse over the chart and go to the point where the conversion rate line is at its peak, the load time that corresponds to this point is your ***Optimal Page Load Time*** - the load time average that maximizes your conversion rate.

***

In the above example, we looked at how average full page load times over all pages affect conversion rates. The `chartConversionsVsLoadTimes()` function also takes some optional keyword arguments that will allow you to:

* Looks at how average full page load times for a specific page URL affects your conversion rates (optional keyword argument is `page`).
* Looks at how average full page load times for a specific page group affects your conversion rates (optional keyword argument is `group`).
* Looks at how either average back-end load times or average front-end load times affects your conversion rates (optional **keyword argument** `timer` by default is `timers_t_done`, the full page load time).
* Looks at any combinations of the above.

Observe the syntax in the cells below as they correspond to what's described above. Replace any variable value so that the function calls corresponds to your pages.

# Chart Conversion Rates vs Load Times for a specific url

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

urlPattern = "%Women.jsp%"
chartConversionsVsLoadTimes(startTime, endTime, url=urlPattern)

# Chart Conversion Rates vs Load Times for a specific page group

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

pageGroupName = "Home"
chartConversionsVsLoadTimes(startTime, endTime, pageGroup=pageGroupName)

# Chart Conversion Rates vs Front-end Load Times

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

timerType = :timers_t_page # Indicates the front-end load time.
chartConversionsVsLoadTimes(startTime, endTime, timer=timerType)

# Chart Conversion Rates vs Back-end Load Times for a specific Page Group

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

timerType = :timers_t_done # Indicates the front-end and back-end load times.
pageGroupName = "Sign-In or Register"

chartConversionsVsLoadTimes(startTime, endTime, timer=timerType, pageGroup=pageGroupName)

On your own, feel free to try any of the ***get*** functions for conversion-related metrics. Remember, functions that are prefaced with ***get*** will typically return a DataFrame (aka table). And, to get a quick list of these functions that are available, in a code cell type out *getConversion* and then hit the **tab** key.

***

## 2.5 Ranking Page Groups by Impact Scores

In the previous section we charted conversion rates vs average load times. One of the particular ways in which we used the `chartConversionsVsLoadTimes()` function was to show how the average load times for a specific page group affected conversion rates. Indeed, if you called this function once for each of your page groups, you could do an eyeball comparison between the charts for each group and then rank the page groups by the tendency of their load times to impact conversion rates. However, consider that it may be more useful to rank the page groups by their impact on number of conversions, rather than conversion rates. Why? Depending on the user behavior on your website and how you've partitioned your website into page groups, you may have page groups that get considerably more requests than others. As a result, these page groups might possibly have lower conversion rates than the other page groups, while still having more total conversions. For this reason we have invented something called a ***Conversion Impact Score*** - a relative score used to rank page groups by their propensity to impact conversions. In addition, we also have something called an ***Activity Impact*** - another relative score which is used to rank page groups by their propensity to impact user activity, or number of page views in a user session. The latter score may be more useful to web applications that do not have the traditional concept of a conversion (such as media sites or sites that earn revenue through ad clicks). Both scores are *relative* - this means they have meaning in the context of the other page groups' scores.

### 2.5.1 Ranking Page Groups by Negative Impact to Conversions

The ***Conversion Impact Scores*** is a relative score.  It ranks page groups by how conversions are negatively impacted due to long load times.

***Conversion Impact Score*** per page group = (requests for that page group) * (the [Spearman Rank Correlation](http://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient) between its load times and number of conversions)

The ***Conversion Impact Scores*** will always be a number between **-1 and 1**. Scores between (0, 1] should be very rare. The more negative the score is, the more detrimental long load times are to conversions.

Properties of the ***Conversion Impact Score***:

* Each page group's score is calculated as the product of the proportion of that group's requests to overall requests and the Spearman Rank Correlation between that page group's average load times and conversions.
* The score is a continuous variable that can have a value between -1 and 1.
* A value of 0 or near 0 indicates that page group has little to no impact on conversions, relative to the other page groups.
* The closer the score nears -1 the more detrimental to conversions the increasing load times are for that page group.
* Scores greater than 0 should be rare.  This is because a positive conversion impact score means that higher load times correlates with higher conversions.

Let's start with getting a DataFrame of the page groups ordered by the relative conversion impact score. Run the below cell, and note that we are storing this DataFrame in a variable called `groupsWithHighConversionImpact`.

# Get Groups by Conversion Impact Score

# startTime = DateTime(2014,11,27)
# endTime = DateTime(2014,11,28)

groupsWithHighConversionImpact = getTopGroupsByConversionImpact(startTime,en)

Now let's take the top 20 of these page groups and draw a chart using the `drawImpact()` function in the cell below.

# Chart the top 20 Page Groups by Conversion Impact Score
pageGroup = "Page Group"
pageLoadTime = "Full Page Load Time"
title = "Relative Conversion Impact Score"

startIndex = 1
endIndex = 20

convImpactTop20 = groupsWithHighConversionImpact[1:20, [symbol(pageGroup), symbol(pageLoadTime), symbol(title)]]
drawImpact(convImpactTop20)

### 2.5.2 Ranking Page Groups by Negative Impact to User Activity

The ***Activity Impact Score*** is also a relative score.  It ranks page groups by how user activity is negatively impacted due to long load times.

Each page group it is calculated using the product of the proportion of overall requests that are associated with that group, along with the [Spearman Rank Correlation](http://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient) between its load times and number of pages requested per session.

The ***Activity Impact Scores*** will always be a number between **-1 and 1**.  Scores between (0, 1] should be very rare. The closer the score is to -1, the more detrimental to user activities the long load times are.

Properties of the ***Activity Impact Score***:
* Each page group's score is calculated as the product of the proportion of that group's requests to overall requests and the Spearman Rank Correlation between that page group's average load times and page views per session.
* The score is a continuous variable that can have a value between -1 and 1.
* A value of 0 or near 0 indicates that page group has little to no impact on how many pages users will visit, relative to the other page groups.
* The closer the score nears -1 the more detrimental to the number of page views the increasing load times are for that page group.
* Scores greater than 0 should be rare.  This is because a positive activity impact score would indicate that higher load times correlates with more page views.

Let's walk through the same process that we did for the Conversion Impact score, but this time using the `getTopGroupsByActivityImpact()` function to get groups orderered by their Activity Impact Score. Run the 2 cells below consecutively.

groupsWithHighActivityImpact = getTopGroupsByActivityImpact(startTime, endTime)

# Chart the top 20 Page Groups by Activity Impact Score
pageGroup = "Page Group"
pageLoadTime = "Full Page Load Time"
title = "Relative Activity Impact Score"

startIndex = 1
endIndex = 20

activityImpactTop20 = groupsWithHighActivityImpact[startIndex:endIndex, [symbol(pageGroup), symbol(pageLoadTime), symbol(title)]]
drawImpact(activityImpactTop20)

***

## 2.6 External Referrers

Previously, we used the `chartTopN()` function to get a ranking of the top page referrers by number of requests. Now, we seek to look specifically at External Referrers

chartExternalReferrerSummary(startTime, endTime)

*Copyright 2015 SOASTA, Inc.
All rights reserved.
Proprietary and confidential.*

using Reactive
using Interact
using DataFrames
using DataStructures

# 3. Querying Your Beacon Data

The `DSWB` library has many useful functions for analyzing and visualizing beacon data. However, you are not limited by what is provided by our libraries. You can tailor and create new functions and Redshift queries.

This notebook tutorial will help get you started on writing your own custom queries against your mPulse beacon data.

***

## 3.1 Query Syntax and Data Type Considerations

Amazon Redshift is based on PostgreSQL, but there are differences between the two. For more information on this, please see [Amazon SQL Reference](http://docs.aws.amazon.com/redshift/latest/dg/cm_chap_SQLCommandRef.html).

In Redshift, there are a few quirks to consider when writing queries against your beacon data.  Quirks may involve: Dealing with timestamps and dates, aggregating metrics data, dealing with `NULL` values, ensuring necessary packages are included, and using the appropriate fields from the beacon table in your queries.

Let's start with getting connected to your data source. You can either include the `DSWB` library and then use the `setRedshiftEndpoint()` function, or you can use the `ODBC.connect()`.   Use the `setRedshiftEndpoint()` option to set your DSN if you are planning on making use of pre-built functions.

For this example, we'll use the former. The `DSWB` library still contains some useful utility functions that you can use to supplement custom queries.

Run the cell below, replacing the `dsn` value with your own DSN.

using DSWB

dsn = "retailerdsn"

setRedshiftEndpoint(dsn)

### 3.1.1 Timetamps and Dates

When writing your own queries, you will deal with the beacon EPOCH timestamp fields. These fields are integer values in the beacon table. They have the Redshift data type of `bigint`. There is some formatting to be done if you want to return these timestamps back into human-readable dates. The primary timestamp fields you'll likely deal with are:
* `timestamp` - indicating when the beacon fired was received by the server.
* `session_start` - indicating when the first beacon was fired in a user's session.
* `session_latest` - indicating the timestamp of the last user action on the client-side.  This value is used as a part of what is needed to determine the end of a session.

The above variables take integer values only.  The time unit is milliseconds.  The value is the number of miliseconds passed since January 1st, 1970.

Let's start by the Julia `DateTime()` function that we've been using to declare the `startTime` and `endTime` for the pre-built functions. Run the cell below.

Change the year, month, day arguments of the start and end date that is within the time range of your beacon data.

startTime = DateTime(2014,11,27)
endTime = DateTime(2014,11,28)

The `startTime` and `endTime` variables above are of type `DateTime`. If you look at the above output, you should notice that these variables look something like "2014-11-28T00:00:00".

Suppose that you'd like to run a query against your beacon data between the start time and end time defined above.  We will filter the data (rows) returned by specifying what to filter on (`timestamp`) and the limits we want to impose on it (`startTime` and `endTime`).  The argument snippet will look something like this:
`WHERE timestamp BETWEEN startTime AND endTime`.

Below is the full example query.  Run the cell after replacing the table name with your beacons' table name.

table = "retailer_beacons"

# Get the average full page load time between start and end time
query("""\
    SELECT AVG(timers_t_done)
    FROM $table
    WHERE timestamp BETWEEN $startTime AND $endTime
""")

You should see an error in the output above.

The ODBC errors can be crytpic and difficult to interpret.  The above error is caused by us submitting a query to Redshift using an incompatible data type for `timestamp`.

Recall that timestamp fields in Redshift have integer values. However, the `DateTime` type defines the `startTime` and `endTime` values we are passing.  These values have the form "YYYY-MM-DDThh:mm:ss".

To fix this, we will convert the `DateTime` values into integer representations.  This representation will be the EPOCH time in milliseconds.

Please use the `datetimeToMs()` function, made available in `DSWB` library, to do the conversion.  (You can also use this function via `Utilities`.)

We do this by declaring 2 new variables: `startTimeMs` and `endTimeMs`. They are the EPOCH millisecond equivalents of `startTime` and `endTime`.

Run the cell below.

# get milliseconds from start time and end time

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

In the output above, you should notice that the `startTimeMs` and `endTimeMs` variables are integer values.

Re-run the query we tried before using the new variables.

# Get the average full page load time between startTime and endTime

table = "retailer_beacons"

query("""\
    SELECT AVG(timers_t_done)
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
""")

The errors should be gone and you should see a valid output.  The output shows the average full page load time (in milliseconds) between the dates specified by `startTime` and `endTime`.

***

### 3.1.2 Aggregating Data

In the context of SQL queries, aggregating data means grouping or combining summarized statistics.  There's already been previous examples of data aggregation in the [The DSWB Library](2. The DSWB Library.ipynb) tutorial.

One previous example: The `chartLoadTimes()` function aggregated median load times based on a given `datepart` (day, hour, minute, etc.). Another example is the `chartConversionsByOS()` function, which aggregated conversion counts by operating system.

Generally, for performance reasons, it is better to do all the data aggregation through the Redshift query. You could do the data aggregation, in memory, using the DSWB notebooks.  This would be done after returning a full, unaggregated result set.  However, performing data aggregation in memory is not recommended because it can have large impacts on performance.

In this section we'll look at some of the data aggregation techniques that we use on beacon data.

Let's begin with a quick summary of what's available in Redshift.

Like other versions of SQL, there are *aggregate functions* and *window functions*. We'll highlight the most common ones, but for a list of everything that's available, visit: [Amazon Redshift Functions Reference](http://docs.aws.amazon.com/redshift/latest/dg/c_SQL_functions.html).

**Aggregate Functions**
* `AVG` - calculates the arithmetic mean
* `COUNT` - counts the rows
* `MIN` - returns the minimum value
* `MAX` - returns the maximum value
* `SUM` - returns the sum of an input column

**Window Functions**
*The above aggregate functions are available in window functions, in addition to the following:*
* `MEDIAN` - calculates the 50th percentile
* `PERCENTILE_CONT` - calculates a percentile for a range of continuous values
* `PERCENTILE_DISC` - calculates a percentile for a range of discrete values

The difference between *aggregate* and *window functions* is in how they are used.

*Aggregate functions* compute a **single** result from a set of input values.

*Window functions* operate on a partition or "window" of a result set.  It then returns a value for every row in that window, a **result set**.

In analyzing beacon data, we are usually just concerned with computing a single aggregate result for a given page group.  For example, the average load times by day, or where the data is grouped by day.  This is different from returning an aggregate result for each row.  This means that for basic inquiries, the *aggregate functions* would be more useful.

However, if we need to calculate medians or percentiles, then we must resort to using the corresponding window function.

#### 3.1.2.1 Aggregating by Datepart

This type of data aggregation is useful for returning results in a *time series* form.  It helps you analyze trends over time.

We make use of Redshift's `DATE_TRUNC()` function, which truncates the timestamp, see the [Redshift Documentation](http://docs.aws.amazon.com/redshift/latest/dg/r_DATE_TRUNC.html)).  The truncation is based on the `datepart` argument.

We also use the `CAST()` function.  This function converts the *datepart value* returned into a fixed 19-character string (YYY-MM-DD hh:mm:ss).

The query below returns the average full page load times by hour for a 1-day time range. Change the `startTime` and `endTime` values below to define a 1-day time range within your beacons data time range. Run the query when you are ready.

**Note**: We are defining `year`, `month`, `startDate`, `endDate`, `startTime`, `endTime`, `table` and `datepart` values before the query itself.  Using variables makes the code clear.

year = 2014
month = 11
startDate = 27
endDate = 28

startTime = DateTime(year,month,startDate)
endTime = DateTime(year,month,endDate)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table    = "retailer_beacons"
datepart = :hour

query("""\
    SELECT CAST(DATE_TRUNC('$datepart', (TIMESTAMP WITH TIME ZONE 'EPOCH' + timestamp/1000 * INTERVAL '1 second')) AS VARCHAR(20)) AS $datepart,
        AVG(timers_t_done) AS avgloadtime
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
    GROUP BY $datepart
    ORDER BY $datepart
""")

The `DATE_TRUNC` part of the query looks complex, but this is an expression that is easy to replicate.  To get other types of results, try changing the `datepart` argument to `:minute` or `:second`.

The `SELECT` is telling Redshift to truncate the timestamp field based on the specified `datepart`.  A second argument is also passed to `DATE_TRUNC()`: An expression that defines what type of timestamp we are dealing with. (Ex. Let's say `datepart` is `:hour` and `TIMESTAMP` gives me *2014-11-27 12:32:24* and after `DATE_TRUNC` that becomes *2014-11-27 12:00:00*.)

The `CAST()` function is called to make sure that the returned *datepart values* are all strings with the same number of characters.  This is for output readability.  You can remove the `CAST()` function to see the difference in output.

Because we are using an *aggregate function* (`AVG()`) with a by variable (`datepart`), you must use the `GROUP BY` SQL keyword.  The `GROUP BY` specifies how to split the data into groups and take the average of each group.  In this case, it is the average of each `datepart` group we want.

Finally, we use the `ORDER BY` SQL keyword to order the results by `datepart` so that we have a chronological sequence of averages. By default, this is in ascending order.

#### 3.1.2.2 Aggregation Using Percentiles

Let's try finding the median load times by `datepart`.

In this case, we will use Redshift's *window functions* to aggregate using percentiles (the median is the 50th percentile).  These functions are more complex than the standard *aggregate functions*.  There are usually multiple ways to use these functions in a query.

The key thing to know is that you cannot use the `GROUP BY` keywords in the same way as with standard *aggregate functions*.

**Remember**: *Window functions* operate on partitions of the result set. They also return a result for each row, unlike *aggregate functions* that return 1 result for each group.

The 2 main things we need to address when trying to aggregate using percentiles are:
1. The `OVER` keyword is **mandatory**.  When it is empty, it creates a single window over the entire table.  When the optional `PARTITION BY` clause is added, multiple windows are created based on the argument passed into the `PARTITION BY` clause.  In our case, the windows are partitioned by `datepart`. ([window function syntax](http://docs.aws.amazon.com/redshift/latest/dg/r_Window_function_synopsis.html)).
2. The `DISTINCT` keyword eliminates the duplicate aggregated percentiles.  Too many duplicates can kill the server memory, so it is recommended that you always use `DISTINCT` for these types of queries.

The query below is equivalent to the previous query on aggregating average load time by datepart.  The difference is that we are now using the `MEDIAN()` window function.

**Note**: Change the `startTime` and `endTime` query parameters to be compatible with your beacon data.  Also, make sure the `table` and `datepart` arguments are correct before running the cell.

**Warning**: Do **not** remove the `DISTINCT` keyword.

startTime = DateTime(2014,11,27)
endTime = DateTime(2014,11,28)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table    = "retailer_beacons"
datepart = :hour

query("""\
    SELECT DISTINCT CAST(DATE_TRUNC('$datepart', (TIMESTAMP WITH TIME ZONE 'EPOCH' + timestamp/1000 * INTERVAL '1 second')) AS VARCHAR(20)) AS $datepart,
        MEDIAN(timers_t_done) OVER(PARTITION BY $datepart) AS medianloadtime
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
""")

**Note**: Excluding the `DISTINCT` keyword would allow duplicate rows in the returned `DataFrame`.  This `DataFrame` could have millions of rows and the `DISTINCT` keyword specifies that we unique rows, one for each datepart partition.  For *aggregate functions* using percentiles, we only need 1 row for each datepart.

***

### 3.1.3 NULL Values & Missing Data

Sometimes your beacon data will have a `NULL` value for some of the fields. In the case of a timer (i.e. `timers_t_page`) this might indicate a user who abandons a request before the page finished loading.  It could also indicate a page that lacks navigation timing (see the [W3C Navigation Timing documentation](https://dvcs.w3.org/hg/webperf/raw-file/tip/specs/NavigationTiming/Overview.html)).

It's rare for timers to have `NULL` values, but there are legitmate reasons for it to happen.  You should also be aware that *aggregate functions* such as `AVG()` and `SUM()`, etc. handle these `NULL` values by excluding them from the calculation.

In [The DSWB Library](2. The DSWB Library.ipynb) tutorial we mentioned that the `params_rt_quit` field lets us know if the beacon was sent on an `unload` event:

* If `params_rt_quit` is `NULL`, then the beacon was sent on `unload` of the page.  This indicates a previous beacon for this page was sent on the `onload` event.
* If `params_rt_quit` is **NOT `NULL`**, then this beacon was fired on the `onload` event.

For most data analysis (counts, averages, medians, etc.) we want to exclude beacons that have `params_rt_quit` equal to `NULL`.  Including them means we count each page twice. The beacons where `params_rt_quit` is `NULL` are useful for other situations.  An example of this is when a user abandons a page.  The `params_rt_quit` being `NULL` would indicate the abandoment and help us correctly calcuate a session's lengths.

To get a sense for the number of beacons you have where `params_rt_quit` is `NULL`, run the query below.

**Note**: Change the table name to the name of your beacons' table.

table = "retailer_beacons"

query("""\
    SELECT 'NOT NULL' AS params_rt_quit, COUNT(*) AS total
    FROM $table
    WHERE params_rt_quit IS NOT NULL

    UNION

    SELECT 'NULL' AS params_rt_quit, COUNT(*) AS total
    FROM $table
    WHERE params_rt_quit IS NULL
""")

**Important**: In general, almost all of your data analysis queries (getting counts, averages, percentiles, etc.) should filter out beacons where `params_rt_quit` is **NOT `NULL`**. The cases where you'd want to include beacons where `params_rt_quit` is `NULL` is rare.

***

## 3.2 Query Examples

This section will provide you with some fundamental queries.  You can copy or build upon them.  Before running any of these queries, be sure to switch out the variable values (`startTime`, `endTime`, `table`, etc) for ones that are compatible with your beacon data.

### Example 1: Median Load Times Over Time

The following gets the median load times, by hour, for a period of 1 day.

The `datepart` can be changed to: `:year`, `:month`, `:week`, `:day`, `:hour`, `:minute` or `:second`.

**Warning**: Querying over a long time range, and/or changing the `datepart` to be too granular for the given time-range, may kill your Redshift cluster and/or DSWB server. For the sake of understanding the queries, you might not want something too memory or database intensive.

# Get Median Front-end, Back-end and Full Page Load Times by Hour
startYear = 2014
startMonth = 11
startDay = 27

endYear = 2014
endMonth = 11
endDay = 28

startTime = DateTime(startYear, startMonth, startDay)
endTime = DateTime(endYear, endMonth, endDay)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table = "retailer_beacons"
datepart = :hour

query("""
    SELECT DISTINCT CAST(DATE_TRUNC('$datepart', (TIMESTAMP WITH TIME ZONE 'epoch' + timestamp/1000 * INTERVAL '1 second')) AS VARCHAR(20)) AS $datepart,
        MEDIAN(timers_t_resp) OVER(PARTITION BY $datepart) AS backendloadtime,
        MEDIAN(timers_t_page) OVER(PARTITION BY $datepart) AS frontendloadtime,
        MEDIAN(timers_t_done) OVER(PARTITION BY $datepart) AS fullpageloadtime
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
        AND params_rt_quit IS NULL
    ORDER BY $datepart
""")

### Example 2: Median Load Times By Operating System

We've been aggregating load times by `datepart`, but you can also do it by `user_agent_os`.  The `user_agent_os` is a beacon field that can be used to get load times by operating system.

# Get Median Front-end, Back-end and Full Page Load Times by Operating System, Ordered from worst full page load time to worst
startYear = 2014
startMonth = 11
startDay = 27

endYear = 2014
endMonth = 11
endDay = 28

startTime = DateTime(startYear, startMonth, startDay)
endTime = DateTime(endYear, endMonth, endDay)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table = "retailer_beacons"

query("""
    SELECT DISTINCT user_agent_os AS OS,
        MEDIAN(timers_t_resp) OVER(PARTITION BY OS) AS backendloadtime,
        MEDIAN(timers_t_page) OVER(PARTITION BY OS) AS frontendloadtime,
        MEDIAN(timers_t_done) OVER(PARTITION BY OS) AS fullpageloadtime
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
        AND params_rt_quit IS NULL
    ORDER BY fullpageloadtime DESC
""")

### Example 3: 75th Percentile Load Times By Page Group

We can also capture load times by another dimension, the *page group*.  This time let's get the 75th percentile rather than the median.

**Note**: We are using the `PERCENTILE_CONT()` function rather than `PERCENTILE_DISC()`.  The timers are considered continuous variables in `PERCENTILE_CONT()`, rather than discrete variables.

# Get the 75th Percentile Front-end, Back-end and Full Page Load Times by Page Group, Ordered from worst full page load time to worst
startYear = 2014
startMonth = 11
startDay = 27

endYear = 2014
endMonth = 11
endDay = 28

startTime = DateTime(startYear, startMonth, startDay)
endTime = DateTime(endYear, endMonth, endDay)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table = "retailer_beacons"

query("""
    SELECT DISTINCT page_group AS pagegroup,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timers_t_resp) OVER(PARTITION BY pagegroup) AS backendloadtime,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timers_t_page) OVER(PARTITION BY pagegroup) AS frontendloadtime,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timers_t_done) OVER(PARTITION BY pagegroup) AS fullpageloadtime
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
        AND params_rt_quit IS NULL
    ORDER BY fullpageloadtime DESC
""")

### Example 4: Top Referrers by Requests

This query simply returns the top `n` referrers in terms of number of requests.

**Note**: We are specifying `session_pages = 1` in the query; this indicates that we only want to see referrer values for the first page, in each user session.

# Get the 75th Percentile Front-end, Back-end and Full Page Load Times by Page Group, Ordered from worst full page load time to worst
startYear = 2014
startMonth = 11
startDay = 27

endYear = 2014
endMonth = 11
endDay = 28

startTime = DateTime(startYear, startMonth, startDay)
endTime = DateTime(endYear, endMonth, endDay)

startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

table = "retailer_beacons"
n = 25 # number of referrers to return

query("""\
    SELECT params_r AS referrer,
        COUNT(timestamp) AS referrals
    FROM $table
    WHERE timestamp BETWEEN $startTimeMs AND $endTimeMs
        AND session_pages = 1
        AND params_r IS NOT NULL
        AND params_r != ''
        AND params_rt_quit IS NULL
    GROUP BY params_r
    ORDER BY referrals DESC
    LIMIT $n
""")

### Example 5: Median Session Length

This query requires a couple of extra things we have not used so far:

First, we need to add the `Dates` package, which provides us with a multitude of functions for working with dates (see [Dates Documentation](https://github.com/quinnj/Dates.jl/blob/master/docs/docs.rst)).  We will use the `Dates.Minute()` function to do some Dates math.

Secondly, we need to know the web application's session timeout value. For most applications, the session timeout is 30 minutes. (A value can be change in the cell below if desired.) If x = a session timeout value (in minutes), then we need to look at beacons ± x minutes outside of the intended timeframe.

Let x = a session timeout value (in minutes). Here are some bullet points for calculating a session's length:

* Be sure **not** to filter out beacons where `params_rt_quit` is `NULL`.  We need these beacon to get the user's latest timestamp.
* We need to look at beacons after `startTime` - x. This is to account for those users that start a session before `startTime`, but whose session won't timeout until *after* `startTime`.
* We need to look at beacons before `endTime` + x. This is to capture those sessions that were active during the intended time window, but whose last beacon was fired *after* `endTime`.
* To determine each session's last timestamp, take the maximum of `session_latest` and then call this session's `session_end`.
* Each session's duration is then calculated as `session_end - session_start`.
* After the above calculations have been made, we need to filter out extraneous beacons by taking only those beacons where `session_start` is *before* the `endTime` and `session_end` is *after* the `startTime`. The median is calculated using this result set.

The query below handles all of the above. Before running the cell below, be sure to update the necessary parameters (`startTime`, `endTime`, `sessionTimeoutMins`, `table`, etc.) to be compatible with your beacon data.

# need to include the Dates package to make use of the Dates.Minute() function below
using Dates
startYear = 2014
startMonth = 11
startDay = 27

endYear = 2014
endMonth = 11
endDay = 28

# This defines the intended time window to look at
startTime = DateTime(startYear, startMonth, startDay)
endTime = DateTime(endYear, endMonth, endDay)

# Define the Session Timeout Value of your application (in minutes)
sessionTimeoutMins = 30

# This Again defines the Query Time Window, using EPOCH timestamps in milliseconds
startTimeMs = datetimeToMs(startTime)
endTimeMs = datetimeToMs(endTime)

# This Defines a wider time window based on the session timeout value; contains the query time window;
lowerBoundMs = datetimeToMs(startTime - Minute(sessionTimeoutMins))
upperBoundMs = datetimeToMs(endTime + Minute(sessionTimeoutMins))

table = "retailer_beacons"

query("""\
    SELECT MEDIAN(session_end - session_start)/60000 OVER() AS session_duration_minutes
    FROM(
            SELECT session_id,
                    session_start,
                    MAX(session_latest) AS session_end
            FROM $table
            WHERE timestamp BETWEEN $lowerBoundMs AND $upperBoundMs
                AND session_id IS NOT NULL
            GROUP BY session_id, session_start
    )
    WHERE session_start <= $endTimeMs AND session_end >= $startTimeMs
    LIMIT 1
""")


***
