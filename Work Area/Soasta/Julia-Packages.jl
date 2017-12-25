*&copy; 2015 SOASTA, Inc.
All rights reserved.
Proprietary and confidential.*

# General Julia Packages

This walkthrough contains the executable code from the examples in the Julia Packages page.

**Note**: Make sure you trust the notebook under `File`, so the plots will show up.

<div id='toc' class="fixedHeader_pageJump"></div>

### Table Of Contents
>1. [DataFrames](#dataframes)
>2. [Dates](#dates)
>3. [Gadfly](#gadfly)
>4. [Google Charts](#googlecharts)
>5. [JSON](#json)
>6. [ODBC](#odbc)

***

<div id="dataframes" class="fixedHeader_pageJump"></div>
## 1. DataFrames

### DataFrame Basics

using DataFrames

data = readtable("data/data.csv");

writetable("data/myData.csv",data)

### Non-manipulative functions
These functions are useful for learning about the data in your DataFrame.

#### size

size(data)

size(data, 1)

#### names

names(data)

#### describe

describe(data)

#### nrows & ncols

nrow(data)

ncol(data)

#### showcols

showcols(data)

#### head & tail

head(data)

tail(data)

#### Accessing a specific column in your DataFrame

data[:page_group]

data[:,2]

#### isna

isna(data[:,11])

### Manipulative functions
These functions are useful for changing and manipulating the data in your DataFrame.

#### complete_cases!

exampleData = data; # To preserve the original data
show(exampleData[:,10:12])
complete_cases!(exampleData)
show(exampleData[:,10:12])

#### sort!

exampleData = data; # To preserve the original data
show(sort!(exampleData[:,10:12]))

show(sort!(exampleData[:,10:12],rev=true))

#### eachrow

for x in eachrow(data)
    println(x)
end

#### eachcol

for x in eachcol(data)
    println(x[1])
end

#### colwise

colwise(mean, data[:,20:22])

#### groupby

gd = groupby(data[:,10:12],:user_agent_os)

gd[1]

gd[2]

gd[3]

#### join

data1 = data[4:5,10:11]

data2 = data[9:10,[10,12]]

fullData = join(data1, data2, on=:user_agent_os)

#### filtering

[1,2,3]==[1,4,3]

[1,2,3].==[1,4,3]

data[data[:,2] .== "class",2]

#### by

byData = by(data, :page_group, nrow)
sort(byData, cols=[:x1], rev=true)

#### aggregate

aggregate(data[:,[2,20]], :page_group, [length, mean])

#### [Return to Table of Contents](#toc)

***

<div id='dates' class="fixedHeader_pageJump"></div>
## Dates

using Dates
date = Date(2015,1,1)

dateTime = DateTime(2015,1,1,10,0,0)

year(date)

hour(dateTime)

second(dateTime)

millisecond(dateTime)

dayofweek(date)

dayname(date)

#### [Return to Table of Contents](#toc)

<div id="gadfly" class="fixedHeader_pageJump"></div>
***
## 3. Gadfly

using Gadfly
df = readtable("data/gadflyData.csv");

p = Gadfly.plot(df, x="timers_t_done", color="user_agent_os",
    Theme(panel_fill=color("white"), background_color=color("white")),
    Geom.density,
    Guide.xlabel("Page Load Time (seconds)"),
    Guide.colorkey("operating System"))

#### [Return to Table of Contents](#toc)

<div id="googlecharts" class="fixedHeader_pageJump"></div>
***
## 4. GoogleCharts

using GoogleCharts
googleChartsData = readtable("data/geoData.csv");
countries = googleChartsData[:countries];
visits = googleChartsData[:visits];

geo_data = DataFrame(
    Country = countries,
    Visitations = visits
);

options = {:title => "Visitations by Country",
       :colorAxis => {:colors => ["#dae3e5", "#507dbc"]},
 :backgroundColor => "#FFFFFF"
}

chart = geo_chart(geo_data, options)

chart = geo_chart(geo_data)

#### [Return to Table of Contents](#toc)

<div id="json" class="fixedHeader_pageJump"></div>
***
## 5. JSON

using JSON
json1 = JSON.json({:string => "Hello", :array => [1,2,3]})

df = DataFrame(
    col1 = ["SOASTA", "DSWB", "Julia"],
    col2 = [100, 200, 300]
)

json2 = JSON.json(df)

JSON.parse(json1)

JSON.parse(json2)

#### [Return to Table of Contents](#toc)

<div id="odbc" class="fixedHeader_pageJump"></div>
***
## 6. ODBC

using ODBC
listdsns()

dsn = "vfdsn" # Change this to a dsn that was returned from listdsns()
ODBC.connect(dsn)
# The following will not execute, because they do not have the correct credentials.
# They are for example purposes only.
    # ODBC.connect("vfdsn", usr="UserName", pwd="Password")
    # connString = "DSN=someDSN;UID=username;PWD=password;"
    # ODBC.advancedconnect(connString)

# The following query, depending on which dsn you are using, may not work.
# For any
result = query("""\

    SELECT COUNT(*)
    FROM soasta_beacons_rt

""")

#### [Return to Table of Contents](#toc)
