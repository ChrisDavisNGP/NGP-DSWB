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

TV = yesterdayTimeVariables()

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

WellKnownUrlGroup = wellKnownUrlGroup(SP);

printWellKnownUrlGroup(SP)

printCsvWellKnownUrlGroup(SP)
;
