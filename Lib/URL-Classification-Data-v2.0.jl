function YourshotClassification(uriPath::ASCIIString)
    try
        newString = "NGP Yourshot"
        #println("Classify $(uriPath)")

        if (ismatch(r"/u/.*",uriPath))
            newString = "Yourshot Renditions"
        elseif (ismatch(r"/media/.*",uriPath))
            newString = "Yourshot Media Files"  # yes, repeated
        elseif (ismatch(r"/static/.*",uriPath))
            newString = "Yourshot Static Files"
        elseif (ismatch(r"/rpc/.*",uriPath))
            newString = "Yourshot Ajax Calls"
        elseif (ismatch(r"/api/.*",uriPath))
            newString = "Yourshot Ajax Calls"  # Yes, repeated
        elseif (ismatch(r"/header/.*",uriPath))
            newString = "Yourshot Ajax Calls"  # Yes, repeated
        elseif (ismatch(r"/features/.*",uriPath))
            newString = "Yourshot Features Calls"
        elseif (ismatch(r"/storage-server.*",uriPath))
            newString = "Yourshot Storage"
        elseif (ismatch(r"/photos.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/daily-doz.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/profile.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/akam.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/_.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/assignments.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/categories.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/widgets.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/search.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/tags.*",uriPath))
            newString = "Yourshot Misc"
        elseif (ismatch(r"/upload.*",uriPath))
            newString = "Yourshot Misc"
        else
            println("Yourshot to do: Classify $(uriPath)")
        end
        return newString
     catch y
        println("Yourshotclassification Exception ",y)
    end

end
