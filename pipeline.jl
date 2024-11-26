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

# Include the helper functions file
include("functions.jl")

# Read and transform user input files to Tulipa input files
user_input_dir = "user-input-files"
tulipa_files_dir = "tulipa-energy-model-files"
default_values = get_default_values(; default_year = 2030)

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

# Save solution
prices = get_hubs_electricity_prices_dataframe(energy_problem)

# Save the solutions to CSV files
prices_file_name = joinpath(output_dir, "eu-case-prices.csv")
CSV.write(prices_file_name, unstack(prices, :asset, :price))

save_solution_to_file(output_dir, energy_problem)

# Plot the results
prices_plot =
    plot_electricity_prices(prices; assets = ["NL_E_Balance", "UK_E_Balance"], xticks = 0:730:8760)

plot_intra_storage_level_NL_battery()
plot_balance_NL()

# Save the plots
prices_plot_name = joinpath(output_dir, "eu-case-price-duration-curve.png")
savefig(prices_plot, prices_plot_name)
