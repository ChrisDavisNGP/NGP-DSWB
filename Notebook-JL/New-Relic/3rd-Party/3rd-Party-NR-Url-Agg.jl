#
# Remember that this would only work on NR Synthetic data for now.  NR Browser data does not show the requests
#

using QueryAPI
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")
include("../../../Lib/URL-Classification-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,10)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP);
WellKnownPath = wellKnownPathDictionary(SP);

statsAndTreemaps(TV,UP,SP)
;
