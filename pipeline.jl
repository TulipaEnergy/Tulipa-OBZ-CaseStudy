using TulipaEnergyModel
using DuckDB
using TulipaIO
using HiGHS
using Plots
using Plots.PlotMeasures
using DataFrames
using StatsPlots
using JuMP
using CSV

# Read raw input files
df = CSV.read(joinpath(input_dir, "profiles-rep-periods.csv"), DataFrame, header = 2)
df_stacked = unstack(df,:profile_name,:value)
CSV.write("raw-input-files/profiles-eu.csv", df_stacked)

# set up the solver
optimizer = HiGHS.Optimizer
parameters = Dict("output_flag" => true, "mip_rel_gap" => 0.0, "mip_feasibility_tolerance" => 1e-5)
 
# using Gurobi
# optimizer = Gurobi.Optimizer
# parameters = Dict("OutputFlag" => 1, "MIPGap" => 0.0, "FeasibilityTol" => 1e-5)
 
# Read Tulipa input files 
input_dir = "tulipa-files-EU-case-study"
connection = DBInterface.connect(DuckDB.DB)
read_csv_folder(connection, input_dir; schemas = TulipaEnergyModel.schema_per_table_name)

# solve the problem
energy_problem = run_scenario(
    connection;
    optimizer = optimizer,
    parameters = parameters,
    write_lp_file = false,
    show_log = true,
    log_file = "log_file.log",
)
 
# save solution
save_solution_to_file("outputs", energy_problem)
 
# plots
 
## price of electricity in the NL
df_highest_in_out = energy_problem.dataframes[:highest_in_out]
unit_ranges = df_highest_in_out[!, :timesteps_block]
end_values = [range[end] for range in unit_ranges]
df_highest_in_out[!, :time] = end_values
 
# filter the hub assets
df_highest_in_out_hubs = filter(row -> occursin(r"Balance", String(row.asset)), df_highest_in_out)
 
# add dual solution
df_highest_in_out_hubs[!, :dual] = energy_problem.solution.duals[:hub_balance] #* 1e3
 
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
sort!(df_highest_in_out_hubs_filtered, :dual; rev = true)
 
@df df_highest_in_out_hubs_filtered plot(
    #:time,
    :dual,
    group = (:asset, :rep_period),
    legend = :none,
    xlabel = "Hour",
    xticks = 0:730:8760,
    ylabel = "[€/MWh]",
    ylims = (0, 250),
    title = "Prices in NL",
    linewidth = 3,
    dpi = 600,
    legend_font_pointsize = 10,
    color = :orangered,
    #legend_title = "Representative period",
    #size = (800, 600),
)
 
savefig("outputs/eu-case-prices.png")

## intra-storage level in the NL battery
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
 
@df df_storage_level_filtered[range_to_plot, :] plot(
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
 
## plot balance
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
plot!(
    df_demand[range_to_plot, :solution] / 1e3;
    label = "Demand",
    color = :black,
    linewidth = 3,
    linestyle = :dash,
)
 
savefig("outputs/eu-case-balance.png")
 