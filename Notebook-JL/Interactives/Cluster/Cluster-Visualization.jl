# Cluster Analysis
# To run the Cluster Analysis visualization, we first include our Clustering library

using DSWB
using Formatting
include(CLUSTERING_PATH)

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt" # resource table name

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2016,12,21,19,0,2016,12,21,23,59);

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

#conversionMetric = "custom_metrics_0" # conversion metric column

defaultBeaconCreateView(TV,UP,SP)
btv = UP.btView
setTable(btv)

# Get Data
#
# We then write some code to pull data out of our DataBase.  The following fields are required:
#
#For beacon intensity (colour)
#* `pageloadtime`
#
#For beacon size (radius)
#* `params_dom_sz`
#* `params_dom_ln`
#* `params_dom_script`
#* `params_dom_img`
#
#Dimensions are all optional, but you won't get clusters without them... These are the most useful:
#* `page_group`
#* `geo_cc`
#* `geo_org`
#* `geo_netspeed`
#* `user_agent_family`
#* `operatingsystemname`
#
#The following are helpful
#* `geo_rg`
#* `geo_city`
#* `geo_postalcode` (currently not available in DSWB database, but we'll add it)
#* `user_agent_model`
#
#The following are only used to render beacon information
#* `url`
#* `user_agent_major`
#* `user_agent_osversion`

#@which getLatestResults(hours=2, minutes=0, table_name="$(page_group_table)")

if SP.debugLevel > 4
    results = getLatestResults(hours=10, minutes=0, table_name=btv)

    #size(results)
    display(results[1:10,:])
end
#display(results)

#dropped geo_cc
results2 = select("""\
    select timestamp,page_group, devicetypename,user_agent_family,
        params_dom_sz,params_dom_ln,params_dom_script,params_dom_img,
        pageloadtime,timers_t_resp,timers_t_page
    from $(btv)
    where
        paramsu ilike '$(UP.urlRegEx)'
    limit $(UP.limitQueryRows)
""");

# Render
#
#Finally, we pass these results to the `clusterViz` function.  This function also takes in two optional parameters:
#* `subgroups`, a number that specifies how many subgroups each cluster should be divided into.  This can be a number between 1 & 5, and defaults to 1, which is a good default.
#* `drawLinks`, a boolean that specifies whether to draw links between related clusters or not.  Links are only drawn at the end of the rendering.
#
#**Warning:** This can blow up with a large number of groups, we're still trying to determine what this limit is so that we can add safety checks into the code
#
#Zoom and Pan with the Mouse, and filter to a particular load time range by dragging along the legend.

if SP.debugLevel > 4
    clusterViz(results, drawLinks=false)
end

clusterViz(results2, drawLinks=false)

# Documentation

#This code implements a few tricks to visualize a few thousand beacons in an efficient and aesthetically pleasing way.

#We use a [d3 force directed layout](https://github.com/mbostock/d3/wiki/Force-Layout) to draw the chart

# Reducing Links

#We borrowed ideas from Economics and Anthropology.  In particular, we looked at studies around the [Product Space](http://en.wikipedia.org/wiki/The_Product_Space) and
#the relatedness of Polynesian cultures with respect to the distances between them.

#The idea being that these provide clean visualizations of relatedness within a cluster as well as those between clusters.

#By default, our beacon graph is represented as a $N^2$ matrix with each cell showing how close two beacons are to each other.
#The actual method for measuring relatedness is not important for this visualization.

#Unfortunately, an $N^2$ matrix results in very bad performance as the number of beacons increases.

#The ideas behind The Product Space help reduce this with the following algorithm:

#1. Convert the graph to a maximum spanning tree to maximise the cost of jumping between nodes
#2. Add in links for the most closely related points
#
#Now since our relatedness score uses 0 for no relation and increases with relatedness, we instead use a [minimum spanning tree](http://en.wikipedia.org/wiki/Minimum_spanning_tree)
#and finally add in links for the most closely related points and groups (highest 2 levels of relatedness).
#
#The minimum spanning tree is generated quickly by using [Kruskal's Algorithm](http://www.stats.ox.ac.uk/~konis/Rcourse/exercise1.pdf)
#
#
## Further reducing links
#
#While this allows us to increase the number of nodes by quite a large number, it can take longer to generate the MST as the
#beacon count grows and adding in the most related nodes can also take a long time for large clusters or cluster groups.
#
#We optimize this further by taking each cluster as a single point. ie, we pick a representative beacon from each cluster, and
#generate the relatedness matrix for these groups.  This results in a $G^2$ matrix where $G$ is the number of groups and is typically
#2+ orders of magnitude smaller than the number of beacons.  We also add the highest weighted links between the group representative
#and all other beacons in the group.
#
#This results in a faster rendering, but not as aesthetically pleasing as the one above.
#
#Instead, we tweaked the `charge` and `linkDistance` for each node such that group representative nodes have a very high magnitude
#negative charge ($-300000/groups.length$) while beacons that are linked within a cluster have a charge between $-10$ and $-40$.  `linkDistance` for unrelated
#beacons is set to $200 \times maxweight$ while that for closely related beacons is set to $0$ and loosely related is set to $65$.
#`linkStrength` for beacons within a cluster is set to $1$ while for related clusters it's set to $0.6$.  Unrelated beacons have a `linkStrength` of
#$0.2$.  This allows unrelated clusters to pull apart from each other while related clusters stay close and related beacons
#within the cluster stay even closer.
#

## Enhancing the group vis

#Even though we now have clusters that are reasonably segregated, it's sometimes hard to tell where one cluster ends and another starts.
#To make the vis clearer, we draw a convex hull around all points in the cluster.  We set the hull's transparency to 0.6 and give it
#rounded corners to look smooth.  This also makes it clear when clusters overlap.  The hull's colour is based on the median load time
#of all beacons within the cluster.
#
#We only draw hulls around clusters that have more than 0.12% of all beacons.  This enhances the appearance of the largest clusters.

## Drawing beacons

#We draw beacons as circles.  The radius of the circle is based on the complexity of the beacon which includes byte size, node count,
#script count and image count.  The colour of the circle is based on beacon performance with green being fast, red being slow and yellow
#and orange being in between.

### Beacons in the hull

#The colour of the hull is based on the median of all becons within the cluster.  As a result, some beacons end up being the same colour
#as the hull and cannot be seen.  We get around this by adding an outline for beacons that are close to the median load time of the group.
#We don't add an outline for all beacons as the vis tends to get crowded with a large number of points.


## Labels

#Labels are rendered using a separate force directed graph.  Each label is represented by a pair of nodes with a link between them.  There
#are no links between label node pairs.  One end of the label node pair is fixed to the centre of the group that it labels.  This node has
#no visual content.  The other end contains the actual label text.  The label force directed graph has a high charge (-1000) for all nodes
#that forces them as far away from each other as possible while keeping all fixed nodes fixed to group centres.

### Label link lines

#Label links are rendered with rounded ends and link length is based on the size of the group.  Larger groups have longer links.  This helps
#the labels stay outside of the group.  Links are also semi-transparent so anything under a link is visible.

### Label size

#The label text is rendered on 2 or 3 lines depending on group size.  Smaller groups only list the group name and basic beacon information
#(count and median).  Larger groups also have a 3rd line that contains beacon spread (min, q1, median, q3, max) rendered as a sort of textual
#box & whiskers plot.  Additionally, the label font size is adjusted based on group size such that small groups use smaller fonts and larger
#groups use larger fonts.  Font sizes are bound between 5pt & 12pt.

### Label readability

#Labels are drawn with a light, opaque fill and a dark, semi-transparent outline.  This makes labels appear crisp regardless of the background
#they float over, so labels that ar partially over a beacon cluster are still readable with sufficient contrast.

#Groups are ordered such that smaller labels are drawn later so that they appear above larger labels if they happen to overlap.

### Label sliding

#Labels are first positioned such that the label link terminates in the centre of the label.  Then, if the angle of the label link with the
#x-axis is less than 45˚ (in either direction), we move the label a little based on the cotangent of the angle.  Labels to the left of the
#group are moved left while labels to the right of the group are moved right.  This makes the label link terminate closer to the nearest edge
#of the label rather than the centre.

#Similarly, if the angle with the y-axis is less than 45˚, we do the same with the top/bottom edge.

### Hidden labels

#We do not render labels for all groups.  The smallest groups, even those with hulls, may not have a label.  This is to avoid crowding the space
#with unimportant labels.


## Fading in

#To avoid having too much activity early on while beacons are in a chaotic state, we delay rendering the hull, labels and label links until
#the simulation is in a more stable state.

#The hull is drawn once the state drops below 0.085 and labels and label links are drawn when the state drops below 0.07.

#To avoid the sudden appearance of the hull and labels, we fade them in from black.  The hull and links are faded in slowly while labels are
#faded in at twice the rate of the links.

#Label positions are calculated throughout the simulation, but they are only made visible later when they are closer to an equilibrium state.
#
#1
#
#1
#
#1
