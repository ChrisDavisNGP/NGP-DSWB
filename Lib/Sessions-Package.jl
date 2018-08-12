function showAvailableSessions(UP::UrlParams,SP::ShowParams,localTableDF::DataFrame,localTableRtDF::DataFrame)
    try
        fullJoin = join(localTableDF,localTableRtDF, on = [:sessionId,:timestamp])
        i = 0
        io = 0
        for subdf in groupby(fullJoin,[:sessionId,:timestamp])
            i += 1
            s = size(subdf)

            if (SP.debugLevel > 8)
                println("Size=",s," Timer=",subdf[1,:timers_t_done]," rl=",UP.timeLowerMs," ru=",UP.timeUpperMs)
            end

            if (subdf[1,:timers_t_done] >= UP.timeLowerMs && subdf[1,:timers_t_done] <= UP.timeUpperMs)
                io += 1
                if io <= UP.limitPageViews
                    s1 = subdf[1,:sessionId]
                    s2 = subdf[1,:timestamp]
                    s3 = subdf[1,:timers_t_done]
                    if (SP.reportLevel > 0) println("executeSingleSession(TV,UP,SP,",s3,",\"",s1,"\",",s2,") #    Time=",s3) end
                end
            end
        end
    catch y
        println("showAvailSessions Exception ",y)
    end
end
