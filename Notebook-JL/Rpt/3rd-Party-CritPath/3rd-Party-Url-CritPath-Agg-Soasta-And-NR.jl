## Tables and Data Source setup

using ODBC
using DataFrames
using DSWB
using Formatting
using URIParser

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"
tableErr = "$(table)_error"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(table)

include("../../../Lib/Include-Package.jl")
include("../../../Lib/URL-Classification-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,12,6,13,0,2017,12,6,13,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

CU = CurlParamsInit(scriptName)
CurlParamsValidate(CU)

NR = NrParamsInit()

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP);
WellKnownPath = wellKnownPathDictionary(SP);
WellKnownUrlGroup = wellKnownUrlGroup(SP);

criticalPathAggWorkflow(TV,UP,SP,CU,NR)
;
