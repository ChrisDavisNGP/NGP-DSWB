using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

include("../../../Lib/Include-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,6,7,10,0,2017,6,7,10,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

minimumEncoded = 500000

aemLargeResourcesWorkflow(TV,UP,SP,minimumEncoded)
;
