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
using Statistics

# Include the helper functions file
include("utils/functions.jl")

# Read and transform user input files to Tulipa input files
user_input_dir = "user-input-files"
tulipa_files_dir = "tulipa-energy-model-files"

# Clean old files and create the directory
chmod(joinpath(@__DIR__, tulipa_files_dir), 0o777) # Change permission: all users have read, write, and execute permissions.
rm(joinpath(@__DIR__, tulipa_files_dir); force = true, recursive = true)
mkdir(joinpath(@__DIR__, tulipa_files_dir))

# Define default values
default_values = get_default_values(; default_year = 2050)

# Define TulipaClustering data
## Data for clustering
n_rp = 1               # number of representative periods
period_duration = 8760 # hours of the representative period
method = :k_means
distance = SqEuclidean()
## Data for weight fitting
weight_type = :convex
tol = 1e-2
## Data for projected subgradient
niters = 100
learning_rate = 0.001
adaptive_grad = false

# Include file with the pre-processing of profiles using clustering
include("utils/preprocess-profiles.jl")

# Include file with the pre-processing the rest of the files
include("utils/preprocess-user-inputs.jl")

# set up the solver
optimizer = HiGHS.Optimizer
parameters = Dict("output_flag" => true, "mip_rel_gap" => 0.0, "mip_feasibility_tolerance" => 1e-5)

using Gurobi
optimizer = Gurobi.Optimizer
parameters = Dict("OutputFlag" => 1, "MIPGap" => 0.0, "FeasibilityTol" => 1e-5)

# Read Tulipa input files 
input_dir = "tulipa-energy-model-files"
connection = DBInterface.connect(DuckDB.DB)
read_csv_folder(
    connection,
    joinpath(@__DIR__, input_dir);
    schemas = TulipaEnergyModel.schema_per_table_name,
)

# Solve the problem and store the solution
energy_problem = run_scenario(
    connection;
    optimizer = optimizer,
    parameters = parameters,
    #write_lp_file = true,
    show_log = true,
    log_file = "log_file.log",
    #enable_names = false,
)

if energy_problem.termination_status == INFEASIBLE
    compute_conflict!(energy_problem.model)
    iis_model, reference_map = copy_conflict(energy_problem.model)
    print(iis_model)
end

# Create "outputs" folder if it doesn't exist
output_dir = "outputs"
if !isdir(joinpath(@__DIR__, output_dir))
    mkdir(joinpath(@__DIR__, output_dir))
end

# Create a file with the combined basic information of the assets
assets_country_tecnology_file = "assets-country-tecnology-data.csv"
df_assets_basic_data = create_one_file_for_assets_basic_info(
    assets_country_tecnology_file,
    joinpath(@__DIR__, user_input_dir),
    joinpath(@__DIR__, output_dir),
    default_values,
)

# Save solution
save_solution!(energy_problem)
export_solution_to_csv_files(joinpath(@__DIR__, output_dir), energy_problem)
prices = get_prices_dataframe(connection, energy_problem)
intra_storage_levels = get_intra_storage_levels_dataframe(connection)
balances = get_balance_per_country(connection, energy_problem, df_assets_basic_data)

# Save the solutions to CSV files
prices_file_name = joinpath(@__DIR__, output_dir, "eu-case-prices.csv")
CSV.write(prices_file_name, unstack(prices, :asset, :price))

intra_storage_levels_file_name = joinpath(@__DIR__, output_dir, "eu-case-intra-storage-levels.csv")
CSV.write(intra_storage_levels_file_name, unstack(intra_storage_levels, :asset, :SoC))

balance_file_name = joinpath(@__DIR__, output_dir, "eu-case-balance-per-country.csv")
CSV.write(balance_file_name, unstack(balances, :technology, :solution; fill = 0))

# Plot the results
prices_plot = plot_electricity_prices(
    prices;
    assets = ["NL_E_Balance", "UK_E_Balance", "OBZLL_E_Balance"],
    #rep_periods = [1, 2],
    #plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), ylims = (0, 100)),
    plots_args = (xticks = 0:730:8760, ylim = (0, 100)),
    duration_curve = true,
)
prices_plot_name = joinpath(@__DIR__, output_dir, "eu-case-price-duration-curve.png")
savefig(prices_plot, prices_plot_name)

batteries_storage_levels_plot = plot_intra_storage_levels(
    intra_storage_levels;
    assets = ["NL_Battery", "UK_Battery"],
    #rep_periods = [1, 2],
    plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), xticks = 0:12:8760, ylims = (0, 1)),
)
batteries_storage_levels_plot_name =
    joinpath(@__DIR__, output_dir, "eu-case-batteries-storage-levels.png")
savefig(batteries_storage_levels_plot, batteries_storage_levels_plot_name)

if n_rp > 1
    hydro_storage_levels_plot = plot_inter_storage_levels(
        connection,
        energy_problem;
        assets = ["ES_Hydro_Reservoir", "NO_Hydro_Reservoir", "FR_Hydro_Reservoir"],
        #plots_args = (xticks = 0:730:8760, ylims = (0, 1)),
    )
else
    hydro_storage_levels_plot = plot_intra_storage_levels(
        intra_storage_levels;
        assets = ["ES_Hydro_Reservoir", "NO_Hydro_Reservoir", "FR_Hydro_Reservoir"],
        #rep_periods = [1, 2],
        plots_args = (xticks = 0:730:8760, ylims = (0, 1)),
    )
end
hydro_storage_levels_plot_name = joinpath(@__DIR__, output_dir, "eu-case-hydro-storage-levels.png")
savefig(hydro_storage_levels_plot, hydro_storage_levels_plot_name)

country = "NL"
balance_plot = plot_country_balance(
    balances;
    country = country,
    year = 2050,
    rep_period = 1,
    plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), xticks = 0:6:8760),
)
balance_plot_name = joinpath(@__DIR__, output_dir, "eu-case-balance-$country.png")
savefig(balance_plot, balance_plot_name)

from_asset = "OBZLL_E_Balance"
to_asset = "NL_E_Balance"
year = 2050
rep_period = 1
flow_plot = plot_flow(
    connection,
    from_asset,
    to_asset,
    year,
    rep_period;
    plots_args = (xlims = (8760 / 2, 8760 / 2 + 168), xticks = 0:12:8760),
)
flow_plot_name = joinpath(@__DIR__, output_dir, "flows-$from_asset-$to_asset.png")
savefig(flow_plot, flow_plot_name)
