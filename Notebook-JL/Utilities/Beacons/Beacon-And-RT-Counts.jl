using QueryAPI
using DataFrames
using DSWB
using Formatting
using URIParser
using JSON

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")

TV = pickTime()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
SP.reportLevel = 10  # Set to 10 to see all non-debug info including long queries
ShowParamsValidate(SP)

beaconAndRtCountsWorkflow(TV,UP,SP)
;
