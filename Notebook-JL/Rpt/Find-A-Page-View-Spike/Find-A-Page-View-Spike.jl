using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(table)
# setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,11,15,23,59,2017,11,16,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

findAPageViewSpikeWorkflow(TV,UP,SP)
;
