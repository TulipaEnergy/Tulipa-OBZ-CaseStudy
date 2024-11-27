# Functions to convert user input files to Tulipa input files

using OrderedCollections: OrderedDict

"""
    transform_profiles_assets_file(input_file::String, output_file::String)

Transforms a CSV file containing profile asset data into a long format and writes the result to a new CSV file.

# Arguments
- `input_file::String`: The path to the input CSV file.
- `output_file::String`: The path to the output CSV file.

# Description
1. Reads the input CSV file into a DataFrame.
2. Identifies the columns to be transformed, excluding "year" and "timestep".
3. Stacks the DataFrame to convert it to a long format.
4. Adds a new column `rep_period` with a constant value of 1.
5. Reorders the columns to `profile_name`, `year`, `rep_period`, `timestep`, and `value`.
6. Writes a header row with units to the output file.
7. Writes the transformed DataFrame to the output CSV file, appending to the header.

# Returns
- The resulting DataFrame after processing the user files.

"""
function transform_profiles_assets_file(input_file::String, output_file::String)
    df = CSV.read(input_file, DataFrame)
    columns = setdiff(names(df), ["year", "timestep"])
    new_df = stack(df, columns; variable_name = :profile_name, value_name = :value)
    new_df[!, :rep_period] .= 1
    new_df = select(new_df, [:profile_name, :year, :rep_period, :timestep, :value])
    open(output_file, "w") do io
        println(io, join(["", "", "", "", "p.u."], ","))
    end
    CSV.write(output_file, new_df; append = true, writeheader = true)
    return new_df
end

"""
    process_user_files(
        input_folder::String,
        output_file::String,
        schema::Union{NTuple, OrderedDict},
        starting_name_in_files::String,
        ending_name_in_files::String,
        default_values::Dict;
        map_to_rename_user_columns::Dict=Dict(),
    )

Process user files from a specified input folder, apply transformations, and save the result to an output file.

# Arguments
- `input_folder::String`: The folder containing the input files.
- `output_file::String`: The path to the output file.
- `schema::Union{NTuple, OrderedDict}`: The schema defining the columns to be included in the output DataFrame.
- `starting_name_in_files::String`: The starting substring to filter input files.
- `ending_name_in_files::String`: The ending substring to filter input files.
- `default_values::Dict`: A dictionary of default values to fill in missing data.
- `map_to_rename_user_columns::Dict=Dict()`: An optional dictionary to rename columns in the input files.

# Description
1. Reads files from the `input_folder` that match the specified starting and ending substrings.
2. Creates an empty DataFrame with columns defined by the `schema`.
3. Reads each matching file into a DataFrame, renames columns as specified by `map_to_rename_user_columns`, and concatenates it to the main DataFrame.
4. Fills missing values in the DataFrame with the specified `default_values`.
5. Selects only the columns defined in the `schema`.
6. Writes the resulting DataFrame to the `output_file`.

# Returns
- The resulting DataFrame after processing the user files.
"""
function process_user_files(
    input_folder::String,
    output_file::String,
    schema::Union{NTuple,OrderedDict},
    starting_name_in_files::String,
    ending_name_in_files::String,
    default_values::Dict;
    map_to_rename_user_columns::Dict = Dict(),
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))

    files = filter(
        file ->
            startswith(file, starting_name_in_files) && endswith(file, ending_name_in_files),
        readdir(input_folder),
    )

    for file in files
        _df = CSV.read(joinpath(input_folder, file), DataFrame; header = 2)
        for (key, value) in map_to_rename_user_columns
            if key in names(_df)
                _df = rename!(_df, key => value)
            end
        end
        for column in columns
            if String(column) ∉ names(_df)
                _df[!, column] .= missing
            end
        end
        _df = select(_df, columns)
        df = vcat(df, _df; cols = :union)
    end

    for (key, value) in default_values
        if key in names(df)
            df[!, key] = coalesce.(df[!, key], value)
        end
    end

    df = select(df, columns)

    open(output_file, "w") do io
        println(io, repeat(",", size(df, 2) - 1))
    end
    CSV.write(output_file, df; append = true, writeheader = true)
    return df
end

"""
    process_flows_rep_period_partition_file(
        assets_partition_file::String,
        flows_data_file::String,
        output_file::String,
        schema::Union{NTuple, OrderedDict},
        default_values::Dict
    )

Processes flow data and partitions it based on asset partition information.

# Arguments
- `assets_partition_file::String`: Path to the CSV file containing asset partition information.
- `flows_data_file::String`: Path to the CSV file containing flow data.
- `output_file::String`: Path to the output CSV file where the processed data will be saved.
- `schema::Union{NTuple, OrderedDict}`: Schema defining the columns of the output DataFrame.
- `default_values::Dict`: Dictionary containing default values for columns in the DataFrame.

# Description
1. Reads the asset partition and flow data from the provided CSV files.
2. Merges the flow data into a DataFrame.
3. Fills missing values in the DataFrame with the provided default values.
4. Assigns partitions to each row based on the asset partition information.
5. Selects the columns defined in the schema.
6. Writes the processed DataFrame to the output CSV file.

# Returns
- The resulting DataFrame after processing the user files.

"""
function process_flows_rep_period_partition_file(
    assets_partition_file::String,
    flows_data_file::String,
    output_file::String,
    schema::Union{NTuple,OrderedDict},
    default_values::Dict,
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))

    df_assets_partition = CSV.read(assets_partition_file, DataFrame; header = 2)
    df_flows = CSV.read(flows_data_file, DataFrame; header = 2)

    df = vcat(df, df_flows; cols = :union)

    for (key, value) in default_values
        if key in names(df)
            df[!, key] = coalesce.(df[!, key], value)
        end
    end

    for row in eachrow(df)
        from_partition = df_assets_partition[df_assets_partition.asset.==row.from_asset, :partition]
        to_partition = df_assets_partition[df_assets_partition.asset.==row.to_asset, :partition]
        if !isempty(from_partition) && !isempty(to_partition)
            row.partition = max(from_partition[1], to_partition[1])
        elseif !isempty(from_partition)
            row.partition = from_partition[1]
        elseif !isempty(to_partition)
            row.partition = to_partition[1]
        end
    end

    df = select(df, columns)

    open(output_file, "w") do io
        println(io, repeat(",", size(df, 2) - 1))
    end
    CSV.write(output_file, df; append = true, writeheader = true)
    return df
end

function get_default_values(; default_year::Int = 2030)
    return Dict(
        "active" => true,
        "capacity" => 0.0,
        "capacity_storage_energy" => 0.0,
        "carrier" => "electricity",
        "commission_year" => default_year,
        "consumer_balance_sense" => missing,
        "discount_rate" => 0.0,
        "economic_lifetime" => 1.0,
        "efficiency" => 1.0,
        "energy_to_power_ratio" => 0,
        "fixed_cost" => 0.0,
        "fixed_cost_storage_energy" => 0.0,
        "group" => missing,
        "initial_export_units" => 0.0,
        "initial_import_units" => 0.0,
        "initial_storage_level" => missing,
        "initial_storage_units" => 0,
        "initial_units" => 0,
        "investment_cost" => 0.0,
        "investment_cost_storage_energy" => 0.0,
        "investment_integer" => false,
        "investment_integer_storage_energy" => false,
        "investment_limit" => missing,
        "investment_limit_storage_energy" => missing,
        "investment_method" => "none",
        "investable" => false,
        "is_milestone" => true,
        "is_seasonal" => false,
        "is_transport" => false,
        "max_energy_timeframe_partition" => missing,
        "max_ramp_down" => missing,
        "max_ramp_up" => missing,
        "min_energy_timeframe_partition" => missing,
        "min_operating_point" => 0.0,
        "num_timesteps" => 8760,
        "partition" => 1,
        "peak_demand" => 0,
        "period" => 1,
        "rep_period" => 1,
        "resolution" => 1.0,
        "ramping" => false,
        "specification" => "uniform",
        "storage_inflows" => 0,
        "storage_method_energy" => false,
        "technical_lifetime" => 1.0,
        "unit_commitment" => false,
        "unit_commitment_integer" => false,
        "unit_commitment_method" => missing,
        "units_on_cost" => 0.0,
        "use_binary_storage_method" => missing,
        "variable_cost" => 0.0,
        "weight" => 1.0,
        "year" => default_year,
        "country" => missing,
        "technology" => missing,
        "lat" => 0,
        "lon" => 0,
    )
end

# Functions to get the results

"""
    get_hubs_electricity_prices_dataframe(energy_problem::EnergyProblem)

Generate a DataFrame containing electricity prices for hubs over time from the given energy problem.

# Arguments
- `energy_problem::EnergyProblem`: An instance of the `EnergyProblem` type containing the necessary data and solution.

# Returns
- `DataFrame`: A DataFrame with columns `:asset`, `:year`, `:rep_period`, `:time`, and `:price`, representing the electricity prices for hubs over time.

# Description
This function processes the `energy_problem` to extract and compute electricity prices for hubs. It filters the relevant data, calculates the duration of each timestep block, and constructs a new DataFrame with the time and price information for each hub. The resulting DataFrame is grouped by `:asset`, `:year`, and `:rep_period`, and the time steps are expanded accordingly.

"""
function get_hubs_electricity_prices_dataframe(energy_problem::EnergyProblem)
    df = energy_problem.dataframes[:highest_in_out]
    unit_ranges = df[!, :timesteps_block]
    start_values = [range[1] for range in unit_ranges]
    df[!, :time] = start_values

    df_hubs = filter(row -> energy_problem.graph[row.asset].type == "hub", df)
    df_hubs[!, :price] = energy_problem.solution.duals[:hub_balance] * 1e3
    df_hubs[!, :duration] = df_hubs[!, :timesteps_block] .|> length

    df_prices = DataFrame(Dict(col => Vector{eltype(df_hubs[!, col])}() for col in names(df_hubs)))
    grouped_df = groupby(df_hubs, [:asset, :year, :rep_period])

    for group in grouped_df
        time_step = 1
        for row in eachrow(group)
            for _ in 1:row[:duration]
                row.time = time_step
                time_step += 1
                push!(df_prices, row)
            end
        end
    end

    select!(df_prices, [:asset, :year, :rep_period, :time, :price])
    return df_prices
end

"""
    get_intra_storage_levels_dataframe(energy_problem::EnergyProblem)

Generate a DataFrame containing the intra-storage levels for a given energy problem.

# Arguments
- `energy_problem::EnergyProblem`: An instance of the `EnergyProblem` type containing the energy problem data.

# Returns
- A `DataFrame` with the intra-storage levels for the specified energy problem.
"""
function get_intra_storage_levels_dataframe(energy_problem::EnergyProblem)
    df = energy_problem.dataframes[:lowest_storage_level_intra_rp]
    unit_ranges = df[!, :timesteps_block]
    start_values = [range[1] for range in unit_ranges]
    df[!, :time] = start_values
    df[!, :duration] = df[!, :timesteps_block] .|> length

    df[!, :SoC] = [
        row.solution / (
            if energy_problem.graph[row.asset].capacity_storage_energy == 0
                1
            else
                energy_problem.graph[row.asset].capacity_storage_energy
            end
        ) for row in eachrow(df)
    ]

    df_intra = DataFrame(Dict(col => Vector{eltype(df[!, col])}() for col in names(df)))
    grouped_df = groupby(df, [:asset, :year, :rep_period])

    for group in grouped_df
        time_step = 1
        for row in eachrow(group)
            for _ in 1:row[:duration]
                row.time = time_step
                time_step += 1
                push!(df_intra, row)
            end
        end
    end

    select!(df_intra, [:asset, :year, :rep_period, :time, :SoC])
    return df_intra
end

# Function for plotting the prices

"""
    plot_electricity_prices(
    prices::DataFrame;
    assets = [],
    years = [],
    rep_periods = [],
    xticks = [],
)

Plots electricity prices over time for specified assets, years, and representative periods.

# Arguments
- `prices::DataFrame`: A DataFrame containing the electricity prices data. It should have columns `:asset`, `:year`, `:rep_period`, `:time`, and `:price`.
- `assets`: An optional array of assets to filter the data. If empty, all assets are included.
- `years`: An optional array of years to filter the data. If empty, all years are included.
- `rep_periods`: An optional array of representative periods to filter the data. If empty, all representative periods are included.
- `xticks`: An optional array of x-ticks to set on the plot.

# Returns
- A plot object with electricity prices over time for the specified filters.

"""
function plot_electricity_prices(
    prices::DataFrame;
    assets = [],
    years = [],
    rep_periods = [],
    xticks = [],
)

    # filtering the assets
    if isempty(assets)
        df_prices = prices
    else
        df_prices = filter(row -> row.asset in assets, prices)
    end

    # filtering the years
    if isempty(years)
        df_prices = df_prices
    else
        df_prices = filter(row -> row.year in years, df_prices)
    end

    # filtering the representative periods
    if isempty(rep_periods)
        df_prices = df_prices
    else
        df_prices = filter(row -> row.rep_period in rep_periods, df_prices)
    end

    # group by asset, year, and representative period
    grouped_df = groupby(df_prices, [:asset, :year, :rep_period])

    # for each group, plot the time vs the price in the same plot
    p = plot()
    for group in grouped_df
        sorted_group = sort(group, :price; rev = true)
        plot!(
            group[!, :time],
            sorted_group[!, :price];
            label = group.asset[1],
            legend = :topright,
            xlabel = "Hour",
            ylabel = "Price [€/MWh]",
            title = "Electricity Prices",
            linewidth = 2,
            dpi = 600,
        )
    end

    # if xticks are provided, set them
    if !isempty(xticks)
        xticks!(xticks)
    end
    return p
end

function plot_intra_storage_level_NL_battery()
    df_storage_level = energy_problem.dataframes[:lowest_storage_level_intra_rp]
    unit_ranges = df_storage_level[!, :timesteps_block]
    end_values = [range[end] for range in unit_ranges]
    df_storage_level[!, :time] = end_values

    # filtering the assets starting with NL in the name
    df_storage_level_filtered = filter(row -> occursin(r"NL", String(row.asset)), df_storage_level)

    # get max value of the solution
    max_value = maximum(df_storage_level_filtered.solution)

    # normalize the solution
    df_storage_level_filtered.solution = df_storage_level_filtered.solution ./ max_value

    range_to_plot = 4368:4536 #1:168

    p = @df df_storage_level_filtered[range_to_plot, :] plot(
        #:time,
        :solution,
        group = (:asset, :rep_period),
        legend = :none,
        xlabel = "Hour",
        xticks = 0:24:8760,
        ylabel = "SoC [p.u.]",
        #title = "SoC Batteries in NL",
        linewidth = 3,
        dpi = 600,
        legend_font_pointsize = 10,
        #legend_title = "Representative period",
        #size = (800, 600),
    )

    savefig("outputs/eu-case-battery-nl.png")

    return @show p
end

function plot_balance_NL()
    energy_problem.dataframes[:flows]

    df_production = DataFrame()
    df_interconnection = DataFrame()

    tech_names = Dict(
        ("NL_Battery", "NL_E_Balance") => "BatteryCharge",
        ("NL_E_Balance", "NL_Battery") => "BatteryDischarge",
        ("NL_Wind_Onshore", "NL_E_Balance") => "WindOnshore",
        ("NL_Wind_Offshore", "NL_E_Balance") => "WindOffshore",
        ("NL_Solar", "NL_E_Balance") => "Solar",
        ("NL_Gas", "NL_E_Balance") => "Gas",
        ("NL_OCGT", "NL_E_Balance") => "OCGT",
        #("NL_E_Balance", "NL_E_Demand") => "Demand",
        ("NL_E_ENS", "NL_E_Demand") => "ENS",
        # ("NL_E_Balance", "BE_E_Balance") => "NL_BE",
        # ("DE_E_Balance", "NL_E_Balance") => "DE_NL",
        # ("DK_E_Balance", "NL_E_Balance") => "DK_NL",
        # ("UK_E_Balance", "NL_E_Balance") => "UK_NL",
        # ("NO_E_Balance", "NL_E_Balance") => "NO_NL",
    )
    # filter by from and to for NL

    for (key, name) in tech_names
        df_flows = filter(
            row -> occursin(key[1], String(row.from)) && occursin(key[2], String(row.to)),
            energy_problem.dataframes[:flows],
        )
        df_production[!, name] = df_flows[!, :solution] / 1e3
    end

    df_production[!, "Battery"] =
        df_production[!, "BatteryCharge"] - df_production[!, "BatteryDischarge"]

    interconnection_names = Dict(
        ("NL_E_Balance", "BE_E_Balance") => "NL_BE",
        ("DE_E_Balance", "NL_E_Balance") => "DE_NL",
        ("DK_E_Balance", "NL_E_Balance") => "DK_NL",
        ("UK_E_Balance", "NL_E_Balance") => "UK_NL",
        ("NO_E_Balance", "NL_E_Balance") => "NO_NL",
    )

    for (key, name) in interconnection_names
        df_flows = filter(
            row -> occursin(key[1], String(row.from)) && occursin(key[2], String(row.to)),
            energy_problem.dataframes[:flows],
        )
        # @show name
        rows = size(df_flows)[1]
        if rows < 8760
            values = []
            n_repeat = 8760 / rows
            for row in 1:rows
                for i in 1:n_repeat
                    push!(values, df_flows[row, :solution])
                end
            end
        else
            values = df_flows[!, :solution]
        end
        # @show size(values)
        df_interconnection[!, name] = values / 1e3
    end

    df_production[!, "Exchange"] =
        df_interconnection[!, "DE_NL"] +
        df_interconnection[!, "DK_NL"] +
        df_interconnection[!, "UK_NL"] +
        df_interconnection[!, "NO_NL"] - df_interconnection[!, "NL_BE"]

    df_demand = filter(
        row ->
            occursin("NL_E_Balance", String(row.from)) &&
                occursin("NL_E_Demand", String(row.to)),
        energy_problem.dataframes[:flows],
    )

    new_col_order = [
        "ENS",
        "OCGT",
        "Gas",
        "Exchange",
        "Battery",
        "WindOffshore",
        "WindOnshore",
        "Solar",
        #"Demand",
    ]

    tech_colors = Dict(
        "ENS" => :red,
        "OCGT" => :lightskyblue,
        "Gas" => :navyblue,
        "Exchange" => :ivory3,
        "Battery" => :orchid,
        "WindOffshore" => :aquamarine,
        "WindOnshore" => :darkgreen,
        "Solar" => :darkorange,
        #"Demand" => :black,
    )

    df_production = df_production[!, new_col_order]

    labels_names = DataFrames.names(df_production)

    tech_colors_name = [tech_colors[name] for name in labels_names]

    range_to_plot = 1:168 #4368:4536

    groupedbar(
        Matrix(df_production[range_to_plot, :]);
        bar_position = :stack,
        labels = reshape(labels_names, 1, length(labels_names)),
        color = reshape(tech_colors_name, 1, length(tech_colors_name)),
        yticks = -10:5:25,
        ylims = (-10, 25),
        #legend = :outerbottom,
        #legend = :topright,
        #legend = :outertopright,
        legend = :bottom,
        legend_column = 5,
        legend_font_pointsize = 8,
        size = (1200, 600),
        left_margin = [5mm 0mm],
        bottom_margin = [5mm 0mm],
        #top_margin = [5mm 0mm],
        xlabel = "Hour",
        xticks = 0:12:168,
        xlims = (0, 168),
        ylabel = "[GWh]",
        #title = "Production in NL",
        dpi = 600,
    )

    # add a line for the demand
    p = plot!(
        df_demand[range_to_plot, :solution] / 1e3;
        label = "Demand",
        color = :black,
        linewidth = 3,
        linestyle = :dash,
    )

    savefig("outputs/eu-case-balance.png")

    return @show p
end