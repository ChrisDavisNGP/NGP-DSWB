##Notebook Information:
Notebook Type: `mPulse` <br>
Graph Type: `Treemap`<br>


#### <span style="color:red">Please make a copy of this template when creating a DSWB widget in mPulse</span>

using DSWB;
setEndpointForTenantId(%tenant%); # %tenant% (tenant ID) received from mPulse
setTableForDomainId(%domain%); # %domain% (domain ID) received from mPulse

# %date% (startTime), %date2% (endTime), and %country% (country filter) are received from mPulse
@soasta_time treeData = getTreemapData(DateTime(%date%), DateTime(%date2%), countryFilter=%country%) "julia"

#### <span style="color:red">The widget is dependent upon the following code, edit with extreme caution </span>

ccmap = getMapCountryNames();
rgmap = getMapRegionsNames();
treeData[:title] = "Treemap";
treeData[rand(size(treeData, 1)) .> .99, :];
titleCol = :title;
fieldNames = [:pagegroupname, :geo_cc, :geo_rg];
data = filter(x -> x["title"] != "--", groupTreeByTitle(treeData, titleCol));
numCharts = length(unique(treeData[treeData[titleCol] .!= "--", titleCol]));
dict = Dict("treeData" => data,"ccmap" => ccmap,"rgmap" => rgmap,"numCharts" => numCharts, "fieldNames" => fieldNames);
JSON.json(dict) # Do not use semicolon here
