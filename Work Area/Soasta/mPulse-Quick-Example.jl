#using mPulseAPI

# mPulse 56 requires username/password for authentication
#token = getRepositoryToken(tenant="Nat Geo", userName="chris.davis@natgeo.com", password="xxx")


#variables
apiTokenValue="6ed53c3e-13a5-4a7b-bc81-26233ec15add"
appNameValue="nationalgeographic.com"
domainIdValue=34501
tenantValue="Nat Geo"
;


# mPulse 57 uses apiToken for authentication
using mPulseAPI
token = getRepositoryToken(tenant=tenantValue, apiToken=apiTokenValue)




# Get a domain by app name
domain = getRepositoryDomain(token, appName=appNameValue)

# Get a domain by api key
#domain = getRepositoryDomain(token, appKey="<api key from mPulse>")

apiKeyValue = domain["attributes"]["apiKey"]                            # Gets the API Key for this app
domain["custom_metrics"]                                  # Gets a Dict of custom metrics
#domain["custom_metrics"]["Conversion Rate"]               # Gets mapping for Conversion Rate custom metric
#domain["custom_metrics"]["Conversion Rate"]["fieldname"]  # Gets field name for Conversion Rate custom metric

# Get all domains in tenant
domains = getRepositoryDomain(token)


# Get a tenant
tenant = getRepositoryTenant(token, name=tenantValue)

getRepositoryDomain(token)

getRepositoryDomain(token; appName=appNameValue)

getRepositoryDomain(token; domainID=domainIdValue)

apiDf = mPulseAPI.getAPIResults(token, apiKeyValue, "summary")

apiDf = mPulseAPI.getAPIResults(token, apiKeyValue, "histogram")

apiDf = mPulseAPI.getHistogram(token, apiKeyValue)
apiDf["buckets"]

browsers = mPulseAPI.getBrowserTimers(token, apiKeyValue)

browsers = mPulseAPI.getBrowserTimers(token, apiKeyValue;filters=Dict("page-group" => "News Article"))

browsers = mPulseAPI.getGeoTimers(token, apiKeyValue;filters=Dict("page-group" => "News Article"))

#browsers = mPulseAPI.getMetricOverPageLoadTime(token, apiKeyValue;filters=Dict("page-group" => "News Article"))
browsers = mPulseAPI.getMetricOverPageLoadTime(token, apiKeyValue)

#browsers = mPulseAPI.getMetricOverPageLoadTime(token, apiKeyValue;filters=Dict("page-group" => "News Article"))
#browsers = mPulseAPI.getMetricsByDimension(token, apiKeyValue,"browser")
mPulseAPI.getMetricsByDimension(token, apiKeyValue, "browser")

mPulseAPI.getPageGroupTimers(token,apiKeyValue)

mPulseAPI.getSessionsOverPageLoadTime(token,apiKeyValue)

mPulseAPI.getSummaryTimers(token,apiKeyValue)

mPulseAPI.getTimerByMinute(token,apiKeyValue,timer="PageLoad")

mPulseAPI.getTimersMetrics(token,apiKeyValue)
