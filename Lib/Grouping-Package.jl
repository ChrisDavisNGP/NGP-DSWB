# Given a list of dimensions, and the number of dimensions to be included in a group by,
# this function returns an array of all possible combinations of dimensions to group by
# You should filter this list to make sure that other constraints are met

function generateDimensionMatrix(cols::Array{Symbol, 1}, dims::Int64; constraints::Dict{Symbol, Symbol}=Dict{Symbol,Symbol}())
    n = size(cols, 1)

    dimcols = [ [x] for x=cols ]
    for i in 2:dims
        append!(dimcols, collect(combinations(cols, i)))
    end

    filter!(function (dimcol)
            for constraint in constraints
                # If the constraint key is in, but its prerequisites are not, then drop it
                if constraint[1] ∈ dimcol && constraint[2] ∉ dimcol
                    return false
                end
            end
            return true
        end, dimcols)

    return dimcols
end

function getBestGrouping(results::DataFrame, summaryDF::DataFrame; showProgress=true)
    minspread = summaryDF[summaryDF[:spread] .== minimum(summaryDF[:spread]), :]

    if showProgress
        newProgress(initial="Best group dimension(s): $(minspread[1, :name]) with $(minspread[1, :n]) groups")
    end

    minspreadnames = map(x -> convert(Symbol, x), split(minspread[1, :name], ','))

    if minspreadnames[1] == Symbol("")
        r = getGroupSummary(results)
    else
        r = by(results, minspreadnames, getGroupSummary)
    end

    return sort!(r, cols = (order(:n, rev=true), :median))
end

function getGroupSummary{T <: AbstractDataFrame}(df::T)
#function getGroupSummary(df::DataFrame)

    summaryDF = summarystats(df[:pageloadtime])
    fw = 0.0
    p2 = 0.0
    p98 = 0.0
    wh1 = 0.0
    wh2 = 0.0
    sumMedian = 0
    spread = 0.0
    p = 0.0
    minf = 0
    low_wm = 0
    p25 = 0
    p75 = 0
    high_wm = 0
    maxf = 0
    bytes = 0
    nodes = 0
    scripts = 0
    images = 0

    try
        fw  = (summaryDF.q75-summaryDF.q25)*1.5
        p2  = percentile(df[:pageloadtime], 2)
        p98 = percentile(df[:pageloadtime], 98)
    catch y
        println("Exceptn 1 ",y);
    end
    try
        wh1 = max(summaryDF.min, summaryDF.q25-fw)
    catch y
        println("Exceptn 2a ",y);
    end

    try
        wh2 = min(summaryDF.max, summaryDF.q75+fw)
    catch y
        println("Exceptn 2b ",y);
    end

    spread = p98/p2 * wh2/wh1
    n = size(df, 1)

    try
        sumMedian = round(Int64, summaryDF.sumMedian)
    catch y
        println("Exceptn 3a ",y);
    end
    try
        spread = round(spread, 3)
    catch y
        println("Exceptn 3b ",y);
    end
    try
        p = round(spread/n, 3)
    catch y
        println("Exceptn 3c ",y);
    end
    try
        minf = round(Int64, summaryDF.min)
    catch y
        println("Exceptn 3d ",y);
    end
    try
        low_wm = round(Int64, wh1)
    catch y
        println("Exceptn 3e ",y);
    end
    try
        p2 = round(Int64, p2)
    catch y
        println("Exceptn 3f ",y);
    end
    try
        p25 = round(Int64, summaryDF.q25)
    catch y
        println("Exceptn 3g ",y);
    end
    try
        p75 = round(Int64, summaryDF.q75)
    catch y
        println("Exceptn 3h ",y);
    end
    try
        p98 = round(Int64, p98)
    catch y
        println("Exceptn 3i ",y);
    end
    try
        high_wm = round(Int64, wh2)
    catch y
        println("Exceptn 3j ",y);
    end
    try
        maxf = round(Int64, summaryDF.max)
    catch y
        println("Exceptn 3k ",y);
    end
    try

        bytes = round(Int64, mean(df[:params_dom_sz]))
    catch y
        #println("Exceptn 3l ",y);
        bytes = 0
    end
    try
        nodes = round(Int64, mean(df[:params_dom_ln]))
    catch y
        #println("Exceptn 3m ",y);
        nodes = 0
    end
    try
        scripts = round(Int64, mean(df[:params_dom_script]))
    catch y
        #println("Exceptn 3n ",y);
        scripts = 0
    end
    try
        images = round(Int64, mean(df[:params_dom_img]))
    catch y
        #println("Exceptn 3o ",y);
        images = 0
    end
    try

    f = DataFrame(
        n = n,
        sumMedian = sumMedian,
        spread = spread,
        p = p,
        minf = minf,
        low_wm = low_wm,
        p2 = p2,
        p25 = p25,
        p75 = p75,
        p98 = p98,
        high_wm = high_wm,
        maxf = maxf,
        bytes = bytes,
        nodes = nodes,
        scripts = scripts,
        images = images
    )

    cols = names(df)
    cols = cols[Bool[!ismatch(r"^(params_dom_|timers_)", string(x)) for x in cols]]

    for col in cols
        local nc = size(unique(df[col]), 1)

        if nc > 1
            f[col] = nc
        end
    end

    return f

    catch y
        println("Exceptn 6 ",y);
    end

end

function getLatestResults(;table_name::ASCIIString="RUM_PRD_BEACON_FACT_DSWB_34501", hours::Int64=3, minutes::Int64=0)
    # Get the date range of the table and format it using strftime
    timestamp_range = select("SELECT min(timestamp) as min, max(timestamp) as max FROM $(table_name)")

    timelimit = timestamp_range[1, 2] - (hours*60 + minutes) * 60 * 1000;

    select("
        SELECT pagegroupname, paramsu,
            geo_cc, geo_rg, geo_city, geo_org, geo_netspeed,
            user_agent_family, user_agent_major, operatingsystemname, user_agent_osversion, user_agent_model,
            params_dom_sz, params_dom_ln, params_dom_script, params_dom_img,
            pageloadtime
         FROM $(table_name)
         WHERE pagegroupname IS NOT NULL
           AND (paramsrtquit IS NULL)
           AND pageloadtime IS NOT NULL
           AND pageloadtime BETWEEN 0 AND 600000
           AND timestamp > $(timelimit)
    ")
#           AND (paramsrtquit IS NULL OR paramsrtquit = FALSE)
end

function groupResults(results::DataFrame; dims::Int64=1, showProgress::Bool=false, progressID::ASCIIString="")

    #showProgress = false

    if showProgress
        if progressID == ""
            newProgress()
        end
        updateProgress("Starting...")
    end

    totalresults = size(results, 1)

    cols = names(results)

    cols = cols[Bool[!ismatch(r"^(params_dom_|timers_)", string(x)) for x in cols]]

    groups = Array(DataFrame, 0)
    groupsummary = DataFrame(name = ASCIIString[], spread = Float64[], n = Int64[], p = Float64[], count = Int64[])

    grouped = getGroupSummary(results)

    push!(groupsummary, ["", grouped[1, :spread], grouped[1, :n], grouped[1, :spread]/grouped[1, :n], 1])

    cols = generateDimensionMatrix(cols, dims, constraints = Dict(
        (:geo_rg => :geo_cc),
        (:geo_city => :geo_rg),
        (:user_agent_major => :user_agent_family),
        (:user_agent_osversion => :operatingsystemname)
        )
    )

    if showProgress
        updateProgress("Trying $(size(cols, 1)) dimension combinations...")
    end

    blacklisteddims = [:geo_netspeed,:operatingsystemname,:geo_cc,:user_agent_family]

    for (index, colnames) in enumerate(cols)

        if showProgress
            updateProgress("Trying $(index)/$(size(cols, 1)) ($(size(groupsummary, 1)) candidates)...")
        end

        #if !isempty(blacklisteddims ∩ colnames)
        #    continue
        #end
        skipIt = false
        for blcol in blacklisteddims
            for colname in colnames
                #println("colname=|",colname,"| blcol=|",blcol,"|")
                if blcol == colname
                    skipIt = true
                    #println("found")
                end
            end
        end
        if (skipIt)
            continue
        end

        grouped = by(results, colnames, getGroupSummary)

        gsizebefore = size(grouped, 1)

        for colname in colnames
            grouped = grouped[!isna(grouped[colname]), :]
        end

        grouped = grouped[grouped[:n] .> 1, :]

        gsize = size(grouped, 1)

        # If more than 20% of this group's elements were eliminated for being too small or null,
        # Then ignore this group
        if gsize < gsizebefore * 0.8
            continue
        end

        if gsize > 1
            if showProgress
                updateProgress("Found candidate: $(index)/$(size(cols, 1))")
            end

            push!(groups, grouped)
            cnames = join(map(x -> string(x), colnames), ',')
            push!(groupsummary, [cnames, mean(grouped[:spread]), floor(mean(grouped[:n])), mean(grouped[:p]), gsize])
        else
            blacklisteddims = cat(1, blacklisteddims, colnames[1])
        end
    end

    if showProgress
        updateProgress("Done: $(size(groupsummary, 1)) candidates")
    end

    groups, groupsummary
end

function newProgress(;initial::ASCIIString="")
    displayid = "progress-" * string(rand())
    display("text/html", """
        <p id="$(displayid)">$(initial)</p>
        <script>
        function groupResultsProgress(text) {
            var p = document.getElementById("$(displayid)");
            if(p) {
                p.innerText = text;
            }
            var parent = p.parentNode.parentNode.parentNode;
            if(parent) {
                var divs = parent.getElementsByClassName("output_area");
                for(var i=1; i<divs.length; i++) {
                    divs[i].parentNode.removeChild(divs[i])
                }
            }
        }
    """)

    return displayid
end

function updateProgress(text::ASCIIString)
    display("text/html", """<script>groupResultsProgress("$text");</script>""")
end
