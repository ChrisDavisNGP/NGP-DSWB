# Dimension Co-occurrence

using DSWB

ODBC.connect("dswb-natgeo")

retailer_results = getLatestResults(hours=24, minutes=10, table_name="beacons_4744")
size(retailer_results)

doit(retailer_results, showDimensionViz=true, showProgress=true);
