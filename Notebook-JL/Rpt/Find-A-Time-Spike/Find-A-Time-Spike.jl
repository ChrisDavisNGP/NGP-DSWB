## Tables and Data Source setup

using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,6,21,14,0,2017,6,21,14,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

findATimeSpikeWorkflow(TV,UP,SP)
;
