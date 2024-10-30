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
"""
function transform_profiles_assets_file(input_file::String, output_file::String)
    df = CSV.read(input_file, DataFrame)
    columns = setdiff(names(df), ["year", "timestep"])
    new_df = stack(df, columns, variable_name=:profile_name, value_name=:value)
    new_df[!, :rep_period] .= 1
    new_df = select(new_df, [:profile_name, :year, :rep_period, :timestep, :value])
    open(output_file, "w") do io
        println(io, join(["", "", "", "", "p.u."], ","))
    end
    CSV.write(output_file, new_df; append=true, writeheader=true)
end

"""
    process_user_files(input_folder::String, output_file::String, schema, starting_name_in_files::String, ending_name_in_files::String, default_values::Dict)

Reads multiple CSV files containing user data, concatenates them into a single DataFrame, fills missing values with defaults, and writes the result to a new CSV file.

# Arguments
- `input_folder::String`: The directory containing the input CSV files.
- `output_file::String`: The path to the output CSV file.
- `schema::Union{NTuple,OrderedDict}`: The schema defining the columns of the DataFrame.
- `starting_name_in_files::String`: The starting pattern of the input file names.
- `ending_name_in_files::String`: The ending pattern of the input file names.
- `default_values::Dict`: A dictionary of default values for missing data.

# Description
1. Creates an empty DataFrame with columns from the schema.
2. Reads all files in the input folder that match the specified patterns.
3. Concatenates the data from these files into a single DataFrame.
4. Fills missing values with the provided default values.
5. Writes the resulting DataFrame to the output CSV file.
"""
function process_user_files(
    input_folder::String,
    output_file::String,
    schema::Union{NTuple,OrderedDict},
    starting_name_in_files::String,
    ending_name_in_files::String,
    default_values::Dict
)
    columns = [name for (name, _) in schema]
    df = DataFrame(Dict(name => Vector{Any}() for name in columns))
    df = select(df, columns)

    files = filter(
        file -> startswith(file, starting_name_in_files) && endswith(file, ending_name_in_files),
        readdir(input_folder)
    )

    for file in files
        _df = CSV.read(joinpath(input_folder, file), DataFrame, header=2)
        df = vcat(df, _df; cols=:union)
    end

    for (key, value) in default_values
        if key in names(df)
            df[!, key] = coalesce.(df[!, key], value)
        end
    end

    open(output_file, "w") do io
        println(io, repeat(",", size(df, 2)))
    end
    CSV.write(output_file, df; append=true, writeheader=true)
end

function get_default_values()
    return Dict(
        "group" => missing,
        "investment_method" => "none",
        "capacity" => 0.0,
        "technical_lifetime" => 1.0,
        "economic_lifetime" => 1.0,
        "discount_rate" => 0.0,
        "capacity_storage_energy" => 0.0,
        "active" => true,
        "commission_year" => 2030,
        "investable" => false,
        "investment_integer" => false,
        "investment_limit" => missing,
        "initial_units" => 0,
        "peak_demand" => 0,
        "consumer_balance_sense" => missing,
        "is_seasonal" => false,
        "storage_inflows" => 0,
        "initial_storage_units" => 0,
        "initial_storage_level" => missing,
        "energy_to_power_ratio" => 0,
        "storage_method_energy" => false,
        "investment_cost_storage_energy" => 0.0,
        "fixed_cost_storage_energy" => 0.0,
        "investment_limit_storage_energy" => missing,
        "investment_integer_storage_energy" => false,
        "use_binary_storage_method" => missing,
        "max_energy_timeframe_partition" => missing,
        "min_energy_timeframe_partition" => missing,
        "unit_commitment" => false,
        "unit_commitment_method" => missing,
        "units_on_cost" => 0.0,
        "unit_commitment_integer" => false,
        "min_operating_point" => 0.0,
        "ramping" => false,
        "max_ramp_up" => missing,
        "max_ramp_down" => missing,
        "carrier" => "electricity",
        "is_transport" => false,
        "variable_cost" => 0.0,
        "initial_export_units" => 0.0,
        "initial_import_units" => 0.0,
        "efficiency" => 1.0,
    )
end

# Function for plotting the prices

function plot_electricity_prices_NL()
    df_highest_in_out = energy_problem.dataframes[:highest_in_out]
    unit_ranges = df_highest_in_out[!, :timesteps_block]
    end_values = [range[end] for range in unit_ranges]
    df_highest_in_out[!, :time] = end_values

    # filter the hub assets
    df_highest_in_out_hubs = filter(row -> occursin(r"Balance", String(row.asset)), df_highest_in_out)

    # add dual solution
    df_highest_in_out_hubs[!, :dual] = energy_problem.solution.duals[:hub_balance] * 1e3

    df_highest_in_out_hubs[!, :duration] = df_highest_in_out_hubs[!, :timesteps_block] .|> length

    # multiply the dual by the duration
    df_highest_in_out_hubs[!, :dual_times_duration] =
        df_highest_in_out_hubs[!, :dual] .* df_highest_in_out_hubs[!, :duration]

    # group by asset and sum the dual and duration
    df_highest_in_out_hubs_grouped = combine(
        groupby(df_highest_in_out_hubs, [:asset]),
        :dual_times_duration => sum,
        :duration => sum,
    )

    df_highest_in_out_hubs_grouped[!, :avg_price] =
        df_highest_in_out_hubs_grouped[!, :dual_times_duration_sum] ./
        df_highest_in_out_hubs_grouped[!, :duration_sum]
    # export the data frame to csv
    CSV.write("outputs/eu-case-avg-prices.csv", df_highest_in_out_hubs_grouped)

    # filter the hub assets starting with NL in the name
    df_highest_in_out_hubs_filtered =
        filter(row -> occursin(r"NL", String(row.asset)), df_highest_in_out_hubs)

    # order the data frame by the dual solution
    sort!(df_highest_in_out_hubs_filtered, :dual; rev=true)

    p = @df df_highest_in_out_hubs_filtered plot(
        #:time,
        :dual,
        group=(:asset, :rep_period),
        legend=:none,
        xlabel="Hour",
        xticks=0:730:8760,
        ylabel="[€/MWh]",
        ylims=(0, 250),
        title="Prices in NL",
        linewidth=3,
        dpi=600,
        legend_font_pointsize=10,
        color=:orangered,
        #legend_title = "Representative period",
        #size = (800, 600),
    )

    savefig("outputs/eu-case-prices.png")
    return @show p
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
        group=(:asset, :rep_period),
        legend=:none,
        xlabel="Hour",
        xticks=0:24:8760,
        ylabel="SoC [p.u.]",
        #title = "SoC Batteries in NL",
        linewidth=3,
        dpi=600,
        legend_font_pointsize=10,
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
        @show name
        @show rows = size(df_flows)[1]
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
        @show size(values)
        df_interconnection[!, name] = values / 1e3
    end

    df_production[!, "Exchange"] =
        df_interconnection[!, "DE_NL"] +
        df_interconnection[!, "DK_NL"] +
        df_interconnection[!, "UK_NL"] +
        df_interconnection[!, "NO_NL"] - df_interconnection[!, "NL_BE"]

    df_demand = filter(
        row ->
            occursin("NL_E_Balance", String(row.from)) && occursin("NL_E_Demand", String(row.to)),
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
        bar_position=:stack,
        labels=reshape(labels_names, 1, length(labels_names)),
        color=reshape(tech_colors_name, 1, length(tech_colors_name)),
        yticks=-10:5:25,
        ylims=(-10, 25),
        #legend = :outerbottom,
        #legend = :topright,
        #legend = :outertopright,
        legend=:bottom,
        legend_column=5,
        legend_font_pointsize=8,
        size=(1200, 600),
        left_margin=[5mm 0mm],
        bottom_margin=[5mm 0mm],
        #top_margin = [5mm 0mm],
        xlabel="Hour",
        xticks=0:12:168,
        xlims=(0, 168),
        ylabel="[GWh]",
        #title = "Production in NL",
        dpi=600,
    )

    # add a line for the demand
    p = plot!(
        df_demand[range_to_plot, :solution] / 1e3;
        label="Demand",
        color=:black,
        linewidth=3,
        linestyle=:dash,
    )

    savefig("outputs/eu-case-balance.png")

    return @show p
end