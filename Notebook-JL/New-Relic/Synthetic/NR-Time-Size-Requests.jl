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
db = setSnowflakeEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

include("../../../Lib/Include-Package.jl")
include("../../../Lib/URL-Classification-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,10,27,23,59,2017,11,3,23,59)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

CU = CurlParamsInit(scriptName)
CurlParamsValidate(CU)

NR = NrParamsInit()

WellKnownHostDirectory = wellKnownHostEncyclopedia(SP);

# Get the data frame of data by using CURL and Json Parse

synChkBodySizeDict = synCheckWorkflow(TV,SP,CU)

if !isdefined(:synChkBodySizeDict)
    return
end

newRelicConvert(SP,NR,synChkBodySizeDict)

timeSizeRequestsWorkflow(TV,SP,NR,CU)
;
