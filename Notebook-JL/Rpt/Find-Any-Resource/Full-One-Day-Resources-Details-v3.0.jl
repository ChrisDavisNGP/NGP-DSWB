using ODBC
using DataFrames
using DSWB
using Formatting

dsn = "dswb-natgeo" # Redshift esetTable(tableRt, tableType = "RESOURCE_TABLE")ndpoint
table = "beacons_4744" # beacon table name
tableRt = "beacons_4744_rt"

# Connect to Beacon Data
setRedshiftEndpoint(dsn)
setTable(tableRt, tableType = "RESOURCE_TABLE")
setTable(table)

# Packages
include("../../../Lib/Include-Package-v2.1.jl")

TV = pickTime()
#TV = timeVariables(2017,4,15,10,0,2017,4,15,10,9);

UP = UrlParamsInit(scriptName)
UrlParamsValidate(UP)

SP = ShowParamsInit()
ShowParamsValidate(SP)

linesOutput = 3
resourceMatched(TV,UP,SP;linesOut=linesOutput)

resourceSummaryAllFields(TV,UP,SP;linesOut=linesOutput)

linesOutput = SP.showLines
resourceSummary(TV,UP,SP;linesOut=linesOutput)

minimumEncoded = 0
resourceSize(TV,UP,SP;linesOut=linesOutput,minEncoded=minimumEncoded)

resourceScreenPrintTable(TV,UP,SP;linesOut=linesOutput)

resourceSummaryDomainUrl(TV,UP,SP;linesOut=linesOutput)

resourceTime1(TV,UP,SP;linesOut=linesOutput)

resourceTime2(TV,UP,SP;linesOut=linesOutput)

resourceTime3(TV,UP,SP;linesOut=linesOutput)
