# Functions to convert user input files to Tulipa input files
using OrderedCollections: OrderedDict

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
    number_of_rep_periods::Int = 1,
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))

    files = filter(
        file ->
            startswith(file, starting_name_in_files) && endswith(file, ending_name_in_files),
        readdir(input_folder),
    )

    for file in files
        _df = CSV.read(joinpath(@__DIR__, input_folder, file), DataFrame; header = 2)
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

    if number_of_rep_periods > 1
        _df = copy(df)
        for rp in 2:number_of_rep_periods
            _df.rep_period .= rp
            df = vcat(df, _df; cols = :union)
        end
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
    default_values::Dict;
    number_of_rep_periods::Int = 1,
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))

    df_assets_partition = CSV.read(assets_partition_file, DataFrame)
    df_flows = CSV.read(flows_data_file, DataFrame)

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
            if row.is_transport
                row.partition = max(from_partition[1], to_partition[1])
            else
                row.partition = min(from_partition[1], to_partition[1])
            end
        elseif !isempty(from_partition)
            row.partition = from_partition[1]
        elseif !isempty(to_partition)
            row.partition = to_partition[1]
        end
    end

    df = select(df, columns)

    if number_of_rep_periods > 1
        _df = copy(df)
        for rp in 2:number_of_rep_periods
            _df.rep_period .= rp
            df = vcat(df, _df; cols = :union)
        end
    end

    CSV.write(output_file, df; append = true, writeheader = true)
    return df
end

"""
    create_one_file_for_assets_basic_info(file_name::String, user_input_dir::String, output_dir::String, default_values::Dict{String,Any})

Create a single file containing basic information about assets.

# Arguments
- `file_name::String`: The name of the output file to be created.
- `user_input_dir::String`: The directory containing user input files.
- `output_dir::String`: The directory where the output file will be saved.
- `default_values::Dict{String,Any}`: A dictionary containing default values for missing data.

# Returns
- `DataFrame`: A DataFrame containing the processed asset information.

# Description
This function processes user input files located in `user_input_dir`, applies a predefined schema,
and saves the resulting data to a file named `file_name` in the `output_dir`.
The schema includes columns for name, type, bidding_zone, technology, latitude, and longitude.
Default values for missing data are provided by the `default_values` dictionary.

"""
function create_one_file_for_assets_basic_info(
    file_name::String,
    user_input_dir::String,
    output_dir::String,
    default_values::Dict{String,Any},
)
    schema = (
        :name => "VARCHAR",
        :type => "VARCHAR",
        :bidding_zone => "VARCHAR",
        :technology => "VARCHAR",
        :lat => "DOUBLE",
        :lon => "DOUBLE",
    )
    df = process_user_files(
        user_input_dir,
        joinpath(output_dir, file_name),
        schema,
        "assets",
        "basic-data.csv",
        default_values,
    )
    return df
end

function create_timeframe_partition_file(
    seasonal_assets::DataFrame,
    output_file::String,
    schema::Union{NTuple,OrderedDict},
    default_values::Dict,
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))
    df = vcat(df, seasonal_assets; cols = :union)
    for (key, value) in default_values
        if key in names(df)
            df[!, key] = coalesce.(df[!, key], value)
        end
    end

    df = select(df, columns)
    CSV.write(output_file, df; append = true, writeheader = true)

    return df
end

function get_default_values(; default_year::Int = 2050)
    return Dict(
        "active" => true,
        "capacity" => 0.0,
        "capacity_storage_energy" => 0.0,
        "carrier" => "electricity",
        "milestone_year" => default_year,
        "commission_year" => default_year,
        "consumer_balance_sense" => missing,
        "decommissionable" => false,
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
        "bidding_zone" => missing,
        "technology" => missing,
        "lat" => 0,
        "lon" => 0,
        "length" => 8760,
    )
end

# Functions to get the results

"""
    unroll_dataframe(df::DataFrame, cols_to_groupby::Vector{Symbol}) -> DataFrame

Unrolls a DataFrame by expanding rows based on the duration of each timestep block.

# Arguments
- `df::DataFrame`: The input DataFrame containing the data to be unrolled.
- `cols_to_groupby::Vector{Symbol}`: A vector of column symbols to group by.

# Returns
- `DataFrame`: A new DataFrame with rows expanded according to the duration of each timestep block.

"""
function unroll_dataframe(df::DataFrame, cols_to_groupby::Vector{Symbol})
    df[!, :time] = df[!, :time_block_start]
    _df = DataFrame(Dict(col => Vector{eltype(df[!, col])}() for col in names(df)))
    grouped_df = groupby(df, cols_to_groupby)

    for group in grouped_df
        time_step = 1
        for row in eachrow(group)
            for _ in 1:row[:duration]
                row.time = time_step
                time_step += 1
                push!(_df, row)
            end
        end
    end
    return _df
end

"""
    get_prices_dataframe(connection)

Generate a DataFrame containing prices for hubs and consumers over time.

# Arguments
- `connection`: DB connection to tables in the model.
- `energy_problem::EnergyProblem`: An instance of the `EnergyProblem` type containing the energy problem data.

# Returns
- `DataFrame`: A DataFrame with columns `:asset`, `:year`, `:rep_period`, `:time`, and `:price`, representing the electricity prices for hubs over time.

"""
function get_prices_dataframe(connection, energy_problem::EnergyProblem)
    df_hubs = _process_prices(connection, "cons_balance_hub", :dual_balance_hub, energy_problem)
    df_consumer =
        _process_prices(connection, "cons_balance_consumer", :dual_balance_consumer, energy_problem)
    df_prices = vcat(df_hubs, df_consumer; cols = :union)
    return df_prices
end

function _process_prices(connection, table_name, duals_key, energy_problem)
    # Get the representative periods resolution
    _df = DuckDB.query(
        connection,
        "SELECT cons.*,
                rp.resolution
            FROM $table_name AS cons
        LEFT JOIN rep_periods_data AS rp
            ON cons.year = rp.year
            AND cons.rep_period = rp.rep_period",
    ) |> DataFrame

    # Get the weight for each representative period in the dataframe
    rep_periods_mapping = TulipaIO.get_table(connection, "rep_periods_mapping")
    gdf = groupby(rep_periods_mapping, [:year, :rep_period])
    rep_periods_weight = combine(gdf, :weight => sum => :weight)
    weights = Dict((row.year, row.rep_period) => row.weight for row in eachrow(rep_periods_weight))
    _df[!, :weight] =
        [weights[year, rep_period] for (year, rep_period) in zip(_df.year, _df.rep_period)]

    # Get the duration of each timestep block
    _df[!, :duration] = _df[!, :time_block_end] .- _df[!, :time_block_start] .+ 1

    # Calculate the price
    _df[!, :price] =
        (_df[!, duals_key] * 1e3 ./ _df[!, :resolution]) ./ (_df[!, :duration]) ./ _df[!, :weight]

    # Unroll the DataFrame to have hourly results
    _df = unroll_dataframe(_df, [:asset, :year, :rep_period])
    select!(_df, [:asset, :year, :rep_period, :time, :price])
    return _df
end

"""
    get_intra_storage_levels_dataframe(connection)

Generate a DataFrame containing the intra-storage levels for a given energy problem.

# Arguments
- `connection`: DB connection to tables in the model.

# Returns
- A `DataFrame` with the intra-storage levels for the specified energy problem.
"""
function get_intra_storage_levels_dataframe(connection)
    # Get the storage capacity
    _df = DuckDB.query(
        connection,
        "SELECT var.*,
                asset.capacity_storage_energy
            FROM var_storage_level_rep_period AS var
        LEFT JOIN asset AS asset
            ON var.asset = asset.asset",
    ) |> DataFrame
    # Calculate the state of charge
    _df[!, :SoC] = [row.solution / (
        if row.capacity_storage_energy == 0
            1
        else
            row.capacity_storage_energy
        end
    ) for row in eachrow(_df)]
    _df[!, :duration] = _df[!, :time_block_end] .- _df[!, :time_block_start] .+ 1
    _df = unroll_dataframe(_df, [:asset, :year, :rep_period])
    select!(_df, [:asset, :year, :rep_period, :time, :SoC])
    return _df
end

"""
    get_balance_per_bidding_zone(energy_problem::EnergyProblem, assets::DataFrame) -> DataFrame

Calculate the energy balance per bidding zone based on the given energy problem and assets data.

# Arguments
- `connection`: DB connection to tables in the model.
- `energy_problem::EnergyProblem`: An instance of the `EnergyProblem` type containing the energy problem data.
- `assets::DataFrame`: A DataFrame containing asset information.

# Returns
- `DataFrame`: A DataFrame containing the energy balance per bidding zone with columns:
    - `bidding_zone`: The bidding zone name.
    - `technology`: The technology type.
    - `year`: The year.
    - `rep_period`: The representative period.
    - `time`: The time.
    - `solution`: The calculated balance value.

"""
function get_balance_per_bidding_zone(connection, energy_problem::EnergyProblem, assets::DataFrame)
    # Get the flows dataframe
    df = TulipaIO.get_table(connection, "var_flow")

    # Exclude lat and lon columns from df_assets
    _assets = select(assets, Not([:lat, :lon]))

    # Merge df with df_assets
    df_assets_from = rename(
        _assets,
        Dict(
            :type => :type_from,
            :bidding_zone => :bidding_zone_from,
            :technology => :technology_from,
        ),
    )
    leftjoin!(df, df_assets_from; on = :from => :name)
    df_assets_to = rename(
        _assets,
        Dict(:type => :type_to, :bidding_zone => :bidding_zone_to, :technology => :technology_to),
    )
    leftjoin!(df, df_assets_to; on = :to => :name)

    # Filter and create new columns in the df 
    # note: we only get the flows that are going to or coming from hubs and consumers
    #       since are the only ones that have balance constraints
    df = filter(
        row -> row.type_from in ["hub", "consumer"] || row.type_to in ["hub", "consumer"],
        df,
    )
    df[!, :duration] = df[!, :time_block_end] .- df[!, :time_block_start] .+ 1
    df = unroll_dataframe(df, [:from, :to, :year, :rep_period])

    # Get producers into the balance
    _df = filter(row -> row.type_from == "producer" && row.type_to in ["hub", "consumer"], df)
    gdf = groupby(_df, [:to, :technology_from, :year, :rep_period, :time])
    df_producer = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_producer, [:to => :bidding_zone, :technology_from => :technology])

    # Get storage discharge
    _df = filter(row -> row.type_from == "storage" && row.type_to in ["hub", "consumer"], df)
    gdf = groupby(_df, [:to, :technology_from, :year, :rep_period, :time])
    df_storage_discharge = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_storage_discharge, [:to => :bidding_zone, :technology_from => :technology])
    df_storage_discharge.technology = string.(df_storage_discharge.technology, "_discharge")

    # Get storage charge
    _df = filter(row -> row.type_to == "storage" && row.type_from in ["hub", "consumer"], df)
    gdf = groupby(_df, [:from, :technology_to, :year, :rep_period, :time])
    df_storage_charge = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_storage_charge, [:from => :bidding_zone, :technology_to => :technology])
    df_storage_charge.technology = string.(df_storage_charge.technology, "_charge")
    df_storage_charge.solution = -df_storage_charge.solution

    # Get conversion production
    _df = filter(row -> row.type_from == "conversion" && row.type_to in ["hub", "consumer"], df)
    gdf = groupby(_df, [:to, :technology_from, :year, :rep_period, :time])
    df_conversion_production = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_conversion_production, [:to => :bidding_zone, :technology_from => :technology])

    # get conversion consumption
    _df = filter(row -> row.type_to == "conversion" && row.type_from in ["hub", "consumer"], df)
    gdf = groupby(_df, [:from, :technology_to, :year, :rep_period, :time])
    df_conversion_consumption = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_conversion_consumption, [:from => :bidding_zone, :technology_to => :technology])
    df_conversion_consumption.solution = -df_conversion_consumption.solution

    # get outgoing/incoming to hubs from hubs and consumers
    _df = filter(row -> row.type_to == "hub" && row.type_from in ["hub", "consumer"], df)
    gdf = groupby(_df, [:from, :technology_to, :year, :rep_period, :time])
    df_outgoing_to_hubs = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_outgoing_to_hubs, [:from => :bidding_zone, :technology_to => :technology])
    df_outgoing_to_hubs.technology .= "OutgoingFlowToHub"
    df_outgoing_to_hubs.solution = -df_outgoing_to_hubs.solution

    gdf = groupby(_df, [:to, :technology_from, :year, :rep_period, :time])
    df_incoming_from_hubs = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_incoming_from_hubs, [:to => :bidding_zone, :technology_from => :technology])
    df_incoming_from_hubs.technology .= "IncomingFlowToHub"

    # get outgoing/incoming to consumers from hubs and consumers
    _df = filter(row -> row.type_to == "consumer" && row.type_from in ["hub", "consumer"], df)
    gdf = groupby(_df, [:from, :technology_to, :year, :rep_period, :time])
    df_outgoing_to_consumers = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_outgoing_to_consumers, [:from => :bidding_zone, :technology_to => :technology])
    df_outgoing_to_consumers.technology .= "OutgoingFlowToConsumer"
    df_outgoing_to_consumers.solution = -df_outgoing_to_consumers.solution

    gdf = groupby(_df, [:to, :technology_from, :year, :rep_period, :time])
    df_incoming_from_consumers = combine(gdf) do sdf
        DataFrame(; solution = sum(sdf.solution))
    end
    rename!(df_incoming_from_consumers, [:to => :bidding_zone, :technology_from => :technology])
    df_incoming_from_consumers.technology .= "IncomingFlowToConsumer"

    # Get demand of the consumers
    df_demand = TulipaIO.get_table(connection, "cons_balance_consumer")
    df_demand[!, :solution] = value.(energy_problem.model[:balance_consumer])
    df_demand = leftjoin!(df_demand, _assets; on = :asset => :name)
    df_demand[!, :duration] = df_demand[!, :time_block_end] .- df_demand[!, :time_block_start] .+ 1
    df_demand = unroll_dataframe(df_demand, [:asset, :year, :rep_period])
    df_demand = select(df_demand, [:asset, :technology, :year, :rep_period, :time, :solution])
    rename!(df_demand, [:asset => :bidding_zone])

    df_balance = vcat(
        df_producer,
        df_storage_discharge,
        df_storage_charge,
        df_conversion_production,
        df_conversion_consumption,
        df_outgoing_to_hubs,
        df_incoming_from_hubs,
        df_outgoing_to_consumers,
        df_incoming_from_consumers,
        df_demand,
    )

    df_balance =
        select(df_balance, [:bidding_zone, :technology, :year, :rep_period, :time, :solution])

    return df_balance
end

# Function for plotting the prices

"""
    plot_prices(
    prices::DataFrame;
    assets = [],
    years = [],
    rep_periods = [],
    plots_args = Dict(),
    duration_curve = true,
)

Plots prices over time for specified assets, years, and representative periods.

# Arguments
- `prices::DataFrame`: A DataFrame containing the prices data. It should have columns `:asset`, `:year`, `:rep_period`, `:time`, and `:price`.
- `assets`: An optional array of assets to filter the data. If empty, all assets are included.
- `years`: An optional array of years to filter the data. If empty, all years are included.
- `rep_periods`: An optional array of representative periods to filter the data. If empty, all representative periods are included.
- `plots_args`: Dictionary with extra arguments for the plot from Plots.jl.
- `duration_curve`: A boolean indicating whether to plot the duration curve.

# Returns
- A plot object with prices over time for the specified filters.

"""
function plot_prices(
    prices::DataFrame;
    assets = [],
    years = [],
    rep_periods = [],
    plots_args = Dict(),
    duration_curve = true,
)

    # filtering the assets
    if isempty(assets)
        df = prices
    else
        df = filter(row -> row.asset in assets, prices)
    end

    # filtering the years
    if isempty(years)
        df = df
    else
        df = filter(row -> row.year in years, df)
    end

    # filtering the representative periods
    if isempty(rep_periods)
        df = df
    else
        df = filter(row -> row.rep_period in rep_periods, df)
    end

    # group by representative period
    grouped_df = groupby(df, [:rep_period])

    # create a subplot for each group
    n_subplots = length(grouped_df)
    p = plot(; layout = grid(n_subplots, 1), plots_args...)

    for (i, group) in enumerate(grouped_df)
        if duration_curve
            _group = sort(group, [:asset, :year, :price]; rev = true)
        else
            _group = group
        end

        plot!(
            p[i],
            group[!, :time],
            _group[!, :price];
            group = (_group[!, :asset], _group[!, :year]),
            xlabel = "Hour - rep. period $(group.rep_period[1])",
            ylabel = "Price [€/MWh]",
            linewidth = 2,
            dpi = 600,
            legend = (i == 1),  # Show legend only for the first group
        )
    end

    return p
end

"""
    plot_intra_storage_levels(
        intra_storage_level::DataFrame;
        assets = [],
        years = [],
        rep_periods = [],
        plots_args = Dict(),
    ) -> Plot

Plot the intra storage levels for the given assets, years, and representative periods.

# Arguments
- `intra_storage_level::DataFrame`: The DataFrame containing the intra storage level data.
- `assets`: An array of assets to filter the data by. If empty, all assets are included.
- `years`: An array of years to filter the data by. If empty, all years are included.
- `rep_periods`: An array of representative periods to filter the data by. If empty, all representative periods are included.
- `plots_args`: Dictionary with extra arguments for the plot from Plots.jl.

# Returns
- `Plot`: A plot object showing the storage levels over time for the specified filters.

"""
function plot_intra_storage_levels(
    intra_storage_level::DataFrame;
    assets = [],
    years = [],
    rep_periods = [],
    plots_args = Dict(),
)

    # filtering the assets
    if isempty(assets)
        df = intra_storage_level
    else
        df = filter(row -> row.asset in assets, intra_storage_level)
    end

    # filtering the years
    if isempty(years)
        df = df
    else
        df = filter(row -> row.year in years, df)
    end

    # filtering the representative periods
    if isempty(rep_periods)
        df = df
    else
        df = filter(row -> row.rep_period in rep_periods, df)
    end

    # group by representative period
    grouped_df = groupby(df, [:rep_period])

    # create a subplot for each group
    n_subplots = length(grouped_df)
    p = plot(; layout = grid(n_subplots, 1), plots_args...)

    for (i, group) in enumerate(grouped_df)
        plot!(
            p[i],
            group[!, :time],
            group[!, :SoC];
            group = (group[!, :asset], group[!, :year]),
            xlabel = "Hour - rep. period $(group.rep_period[1])",
            ylabel = "Storage level [p.u.]",
            linewidth = 3,
            dpi = 600,
            legend = (i == 1),  # Show legend only for the first group
        )
    end

    return p
end

"""
    plot_inter_storage_levels(
        connection;
        assets = [],
        plots_args = Dict(),
    ) -> Plot

Plot the inter storage levels for the given assets.

# Arguments
- `connection`: DB connection to tables in the model.
- `assets`: An array of assets to filter the data by. If empty, all assets are included.
- `plots_args`: Dictionary with extra arguments for the plot from Plots.jl.

# Returns
- `Plot`: A plot object showing the storage levels over time for the specified filters.

"""
function plot_inter_storage_levels(connection; assets = [], plots_args = Dict())
    _df = TulipaIO.get_table(connection, "var_storage_level_over_clustered_year")

    # filtering the assets
    if isempty(assets)
        df = _df
    else
        df = filter(row -> row.asset in assets, _df)
    end

    assest_info = TulipaIO.get_table(connection, "asset")
    capacity_storage_energy =
        Dict(row.asset => row.capacity_storage_energy for row in eachrow(assest_info))
    df[!, :SoC] = [row.solution / (
        if capacity_storage_energy[row.asset] == 0
            1
        else
            capacity_storage_energy[row.asset]
        end
    ) for row in eachrow(df)]

    # rename period_block_start as period
    rename!(df, :period_block_start => :period)

    p = plot(; plots_args...)

    plot!(
        df[!, :period],
        df[!, :SoC];
        group = df[!, :asset],
        xlabel = "Period",
        ylabel = "Storage level [p.u.]",
        linewidth = 3,
        dpi = 600,
    )

    return p
end

function plot_bidding_zone_balance(
    df::DataFrame;
    bidding_zone::String,
    year::Int,
    rep_period::Int,
    plots_args = Dict(),
)
    df = filter(
        row ->
            row.bidding_zone == bidding_zone && row.year == year && row.rep_period == rep_period,
        df,
    )
    technologies = unique(df.technology)
    technologies = push!(technologies, "NetExchangeWithHubs")
    technologies = push!(technologies, "NetExchangeWithConsumers")
    technologies = filter!(
        x ->
            x ∉ [
                "Demand",
                "OutgoingFlowToHub",
                "IncomingFlowToHub",
                "OutgoingFlowToConsumer",
                "IncomingFlowToConsumer",
            ],
        technologies,
    )
    has_demand = "Demand" in unique(df.technology) ? true : false

    df_unstack = unstack(df, :technology, :solution)
    if "OutgoingFlowToHub" ∉ names(df_unstack)
        df_unstack.OutgoingFlowToHub = zeros(size(df_unstack, 1))
    end
    if "IncomingFlowToHub" ∉ names(df_unstack)
        df_unstack.IncomingFlowToHub = zeros(size(df_unstack, 1))
    end
    if "OutgoingFlowToConsumer" ∉ names(df_unstack)
        df_unstack.OutgoingFlowToConsumer = zeros(size(df_unstack, 1))
    end
    if "IncomingFlowToConsumer" ∉ names(df_unstack)
        df_unstack.IncomingFlowToConsumer = zeros(size(df_unstack, 1))
    end
    df_unstack.NetExchangeWithHubs = df_unstack.IncomingFlowToHub .+ df_unstack.OutgoingFlowToHub
    df_unstack.NetExchangeWithConsumers =
        df_unstack.IncomingFlowToConsumer .+ df_unstack.OutgoingFlowToConsumer
    demand = has_demand ? df_unstack.Demand : zeros(size(df_unstack, 1))
    df_unstack = select!(df_unstack, technologies)

    # change the NetExchangeWithConsumers to be the first column in the dataframe
    df_unstack = df_unstack[:, [end; 1:end-1]]
    df_columns = names(df_unstack)

    groupedbar(
        Matrix(df_unstack) / 1000;
        labels = reshape(df_columns, 1, length(df_columns)),
        bar_position = :stack,
        size = (1200, 600),
        left_margin = [4mm 0mm],
        bottom_margin = [4mm 0mm],
        legend_column = min(length(df_columns), 4),
        xlabel = "Hour",
        ylabel = "[GWh]",
        dpi = 600,
        palette = :Paired_11,
    )

    # add a line for the demand
    p = plot!(; plots_args...)
    plot!(demand / 1000; label = "Demand", color = :black, linewidth = 3, linestyle = :dash)

    return p
end

function plot_flow(
    connection,
    from_asset = [],
    to_asset = [],
    year = [],
    rep_period = [];
    plots_args = Dict(),
)
    _df = TulipaIO.get_table(connection, "var_flow")

    # filtering the flows
    _df = filter(
        row ->
            row.from == from_asset &&
                row.to == to_asset &&
                row.year == year &&
                row.rep_period == rep_period,
        _df,
    )

    _df[!, :duration] = _df[!, :time_block_end] .- _df[!, :time_block_start] .+ 1
    _df = unroll_dataframe(_df, [:from, :to, :year, :rep_period])

    # group by representative period
    grouped_df = groupby(_df, [:rep_period])

    # create a subplot for each group
    n_subplots = length(grouped_df)
    p = plot(; layout = grid(n_subplots, 1), plots_args...)

    for (i, group) in enumerate(grouped_df)
        plot!(
            p[i],
            group[!, :time],
            group[!, :solution] / 1000;
            group = (group[!, :from], group[!, :to]),
            label = string(from_asset, " -> ", to_asset),
            xlabel = "Hour",
            ylabel = "[GWh]",
            dpi = 600,
            legend = (i == 1),  # Show legend only for the first group
        )
    end

    return p
end
