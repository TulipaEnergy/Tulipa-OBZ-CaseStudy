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
using TulipaClustering
using Distances

# Include the helper functions file
include("functions.jl")

# Read and transform user input files to Tulipa input files
user_input_dir = "user-input-files"
tulipa_files_dir = "tulipa-energy-model-files"
default_values = get_default_values(; default_year = 2030)

# Define TulipaClustering data
## Data for clustering
period_duration = 24
n_rp = 30
method = :k_means
distance = SqEuclidean()
## Data for weight fitting
weight_type = :convex
tol = 1e-2
## Data for projected subgradient
niters = 100
learning_rate = 0.001
adaptive_grad = false

# Include file with the pre-processing
include("preprocess-user-inputs.jl")

# set up the solver
optimizer = HiGHS.Optimizer
parameters = Dict("output_flag" => true, "mip_rel_gap" => 0.0, "mip_feasibility_tolerance" => 1e-5)

using Gurobi
optimizer = Gurobi.Optimizer
parameters = Dict("OutputFlag" => 1, "MIPGap" => 0.0, "FeasibilityTol" => 1e-5)

# Read Tulipa input files 
input_dir = "tulipa-energy-model-files"
connection = DBInterface.connect(DuckDB.DB)
read_csv_folder(connection, input_dir; schemas = TulipaEnergyModel.schema_per_table_name)

# Solve the problem and store the solution
energy_problem = run_scenario(
    connection;
    optimizer = optimizer,
    parameters = parameters,
    write_lp_file = false,
    show_log = true,
    log_file = "log_file.log",
)

if energy_problem.termination_status == INFEASIBLE
    compute_conflict!(energy_problem.model)
    iis_model, reference_map = copy_conflict(energy_problem.model)
    print(iis_model)
end

# Create "outputs" folder if it doesn't exist
output_dir = "outputs"
if !isdir(output_dir)
    mkdir(output_dir)
end

# Create a file with the combined basic information of the assets
assets_country_tecnology_file = "assets-country-tecnology-data.csv"
df_assets_basic_data = create_one_file_for_assets_basic_info(
    assets_country_tecnology_file,
    user_input_dir,
    output_dir,
    default_values,
)

# Save solution
prices = get_prices_dataframe(energy_problem)
intra_storage_levels = get_intra_storage_levels_dataframe(energy_problem)
balances = get_balance_per_country(energy_problem, df_assets_basic_data)

# Save the solutions to CSV files
prices_file_name = joinpath(output_dir, "eu-case-prices.csv")
CSV.write(prices_file_name, unstack(prices, :asset, :price))

intra_storage_levels_file_name = joinpath(output_dir, "eu-case-intra-storage-levels.csv")
CSV.write(intra_storage_levels_file_name, unstack(intra_storage_levels, :asset, :SoC))

balance_file_name = joinpath(output_dir, "eu-case-balance-per-country.csv")
CSV.write(balance_file_name, unstack(balances, :technology, :solution; fill = 0))

# Plot the results
prices_plot = plot_electricity_prices(
    prices;
    assets = ["NL_E_Balance", "UK_E_Balance", "OBZLL_E_Balance"],
    plots_args = (xticks = 0:730:8760, ylim = (0, 100)),
)
prices_plot_name = joinpath(output_dir, "eu-case-price-duration-curve.png")
savefig(prices_plot, prices_plot_name)

batteries_storage_levels_plot = plot_intra_storage_levels(
    intra_storage_levels;
    assets = ["NL_Battery", "UK_Battery"],
    plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), xticks = 0:12:8760, ylims = (0, 1)),
)
batteries_storage_levels_plot_name = joinpath(output_dir, "eu-case-batteries-storage-levels.png")
savefig(batteries_storage_levels_plot, batteries_storage_levels_plot_name)

hydro_storage_levels_plot = plot_intra_storage_levels(
    intra_storage_levels;
    assets = ["ES_Hydro_Reservoir", "NO_Hydro_Reservoir", "FR_Hydro_Reservoir"],
    plots_args = (xticks = 0:730:8760, ylims = (0, 1)),
)
hydro_storage_levels_plot_name = joinpath(output_dir, "eu-case-hydro-storage-levels.png")
savefig(hydro_storage_levels_plot, hydro_storage_levels_plot_name)

country = "OBZLL"
balance_plot = plot_country_balance(
    balances;
    country = country,
    year = 2030,
    rep_period = 1,
    plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), xticks = 0:6:8760, ylims = (-2, 2)),
)
balance_plot_name = joinpath(output_dir, "eu-case-balance-$country.png")
savefig(balance_plot, balance_plot_name)

# save individual flows solutions
save_solution_to_file(output_dir, energy_problem)
flows = CSV.read(joinpath(output_dir, "flows.csv"), DataFrame)

# filter rows by from and to columns for a specific values
from_asset = "NL_E_Balance"
to_asset = "NL_electrolyzer"
flows_filtered = filter(row -> row.from == from_asset && row.to == to_asset, flows)

# plot the filtered flows
plot(
    flows_filtered[!, :timestep],
    flows_filtered[!, :value] / 1000;
    label = string(from_asset, " -> ", to_asset),
    xlabel = "Hour",
    ylabel = "[GWh]",
    linewidth = 2,
    xlims = (8760 / 2, 8760 / 2 + 168),
    dpi = 600,
)
savefig(joinpath(output_dir, "flows-$from_asset-$to_asset.png"))
