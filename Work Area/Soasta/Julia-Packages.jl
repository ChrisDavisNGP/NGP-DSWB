#**Note**: Make sure you trust the notebook under `File`, so the plots will show up.
#

using DataFrames

data = readtable("data/data.csv");
writetable("data/myData.csv",data)

### Non-manipulative functions
#These functions are useful for learning about the data in your DataFrame.

#### isna

isna(data[:,11])

### Manipulative functions
#These functions are useful for changing and manipulating the data in your DataFrame.

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

## 3. Gadfly

using Gadfly
df = readtable("data/gadflyData.csv");

p = Gadfly.plot(df, x="timers_t_done", color="user_agent_os",
    Theme(panel_fill=color("white"), background_color=color("white")),
    Geom.density,
    Guide.xlabel("Page Load Time (seconds)"),
    Guide.colorkey("operating System"))

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
