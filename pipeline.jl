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
default_values = get_default_values(; default_year=2030)

# Include file with the pre-processing
include("preprocess-user-inputs.jl")

# set up the solver
optimizer = HiGHS.Optimizer
parameters = Dict(
    "output_flag" => true,
    "mip_rel_gap" => 0.0,
    "mip_feasibility_tolerance" => 1e-5,
)

using Gurobi
optimizer = Gurobi.Optimizer
parameters = Dict(
    "OutputFlag" => 1,
    "MIPGap" => 0.0,
    "FeasibilityTol" => 1e-5
)

# Read Tulipa input files 
input_dir = "tulipa-energy-model-files"
connection = DBInterface.connect(DuckDB.DB)
read_csv_folder(connection, input_dir; schemas=TulipaEnergyModel.schema_per_table_name)

# Solve the problem and store the solution
energy_problem = run_scenario(
    connection;
    optimizer=optimizer,
    parameters=parameters,
    write_lp_file=false,
    show_log=true,
    log_file="log_file.log",
)

if energy_problem.termination_status == INFEASIBLE
    compute_conflict!(energy_problem.model)
    iis_model, reference_map = copy_conflict(energy_problem.model)
    print(iis_model)
end

## if no "outputs" folder exists, create it
if !isdir("outputs")
    mkdir("outputs")
end

## save solution
save_solution_to_file("outputs", energy_problem)

# Plot the results

## price of electricity in the NL
plot_electricity_prices_NL()

## intra-storage level in the NL battery
plot_intra_storage_level_NL_battery()

## plot balance in NL
plot_balance_NL()