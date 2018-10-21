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

include("../../../Lib/Structures-Package.jl")
include("../../../Lib/Time-Package.jl")

include("../../../Lib/Body-Parts.jl")
include("../../../Lib/Page-Group-Package.jl")
include("../../../Lib/Peak-Package.jl")
include("../../../Lib/Quartiles-Package.jl")
include("../../../Lib/Referrals-Package.jl")
include("../../../Lib/Sessions-Package.jl")
include("../../../Lib/Stats-Package.jl")
include("../../../Lib/Tables-Package.jl")
include("../../../Lib/Tables-Print-Package.jl")
include("../../../Lib/Treemaps-Package.jl")
include("../../../Lib/URL-Package.jl")
include("../../../Lib/Utilities-Package.jl")
include("../../../Lib/Workflow-Package.jl")

TV = pickTime(1)
#TV = timeVariables(2017,6,14,6,0,2017,6,15,0,5)

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

urlDetailsWorkflow(TV,UP,SP)
;
