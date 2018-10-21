using QueryAPI
using DataFrames
using DSWB
using Formatting

dsn = "tenant_232301"
table = "RUM_PRD_BEACON_FACT_DSWB_34501" # beacon table name
tableRt = "$(table)_rt"

# Connect to Beacon Data
db = setSnowflakeEndpoint(dsn)
setTable(table)
setTable(tableRt, tableType = "RESOURCE_TABLE")

#include("../../../Lib/Include-Package.jl")
include("Structures-Package.jl")
#include("Structures-New-Relic.jl")
include("Time-Package.jl")

include("Body-Parts.jl")
#include("Curl-Package.jl")
#include("Debug-Package.jl")
#include("Explain-Package.jl")
include("Grouping-Package.jl")
#include("New-Relic-Package.jl")
include("Page-Group-Package.jl")
include("Path-Package.jl")
include("Peak-Package.jl")
include("Quartiles-Package.jl")
include("Referrals-Package.jl")
include("Sessions-Package.jl")
include("Stats-Package.jl")
include("Tables-Package.jl")
include("Tables-Print-Package.jl")
include("Treemaps-Package.jl")
include("URL-Package.jl")
include("Utilities-Package.jl")
include("Workflow-Package.jl")

TV = pickTime()
#TV = timeVariables(2017,6,14,6,0,2017,6,15,0,5)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

urlDetailsWorkflow(TV,UP,SP)
;
