using TulipaEnergyModel
using DuckDB
using TulipaIO
using HiGHS
using Plots
using DataFrames
using StatsPlots
using JuMP

# set up the solver
optimizer = HiGHS.Optimizer
parameters = Dict("output_flag" => true, "mip_rel_gap" => 0.0, "mip_feasibility_tolerance" => 1e-5)
 
# using Gurobi
# optimizer = Gurobi.Optimizer
# parameters = Dict("OutputFlag" => 1, "MIPGap" => 0.0, "FeasibilityTol" => 1e-5)
 
# Read data 
input_dir = "EU"
connection = DBInterface.connect(DuckDB.DB)
read_csv_folder(connection, input_dir; schemas = TulipaEnergyModel.schema_per_table_name)

# clustering...

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
 
savefig("outputs/eu-case-prices-nl.png")
 