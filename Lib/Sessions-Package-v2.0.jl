#function showAvailableSessions(localTableDF::DataFrame,localTableRtDF::DataFrame,rangeLowerMs::Float64,rangeUpperMs::Float64;showCriticalPathOnly::Bool=true,showLines::Int64=10)
function showAvailableSessions(TV::TimeVars,UP::UrlParams,SP::ShowParams,localTableDF::DataFrame,localTableRtDF::DataFrame;
  showCriticalPathOnly::Bool=true,showLines::Int64=10)
    try
        full = join(localTableDF,localTableRtDF, on = [:session_id,:timestamp])
        i = 0
        io = 0
        for subdf in groupby(full,[:session_id,:timestamp])
            i += 1
            s = size(subdf)
            #println("Size=",s," Timer=",subdf[1,:timers_t_done]," rl=",rangeLowerMs," ru=",rangeUpperMs)
            if (subdf[1,:timers_t_done] >= UP.timeLowerMs && subdf[1,:timers_t_done] <= UP.timeUpperMs)
                io += 1
                if io <= showLines
                    s1 = subdf[1,:session_id]
                    s2 = subdf[1,:timestamp]
                    s3 = subdf[1,:timers_t_done]
                    #if (SP.reportLevel > 1) println("individualPageReport(\"",s1,"\",",s2,",showCriticalPathOnly=$(showCriticalPathOnly)) #    Time=",s3) end
                    if (SP.reportLevel > 1) println("individualPageReportV2(TV,UP,SP,WellKnownHost,WellKnownPath,\"",UP.urlRegEx,"\",",s3,",\"",s1,"\",",s2,") #    Time=",s3) end
                end
            end
        end
    catch y
        println("showAvailSessions Exception ",y)
    end
end
