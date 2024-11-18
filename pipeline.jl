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

## transform profiles file
user_file = "profiles.csv"
tulipa_file = "profiles-rep-periods.csv"
transform_profiles_assets_file(
    joinpath(user_input_dir, user_file),
    joinpath(tulipa_files_dir, tulipa_file),
)

## write graph assets data file
tulipa_file = "graph-assets-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.graph.assets,
    "assets",
    "basic-data.csv",
    default_values,
)

## write assets data file
tulipa_file = "assets-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.data,
    "assets",
    "yearly-data.csv",
    default_values,
)

## write assets-profiles data file
tulipa_file = "assets-profiles.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.profiles_reference,
    "assets",
    "profiles.csv",
    default_values,
)

## write assets-rep-periods-partitions data file
tulipa_file = "assets-rep-periods-partitions.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.rep_periods_partition,
    "assets",
    "yearly-data.csv",
    default_values;
    map_to_rename_user_columns=Dict("name" => "asset"),
)

## write graph flows data file
tulipa_file = "graph-flows-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.graph.flows,
    "flows",
    "basic-data.csv",
    default_values,
)

## write flow data file
tulipa_file = "flows-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.data,
    "flows",
    "yearly-data.csv",
    default_values,
)

## write flows profiles data file
tulipa_file = "flows-profiles.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.profiles_reference,
    "flows",
    "profiles.csv",
    default_values,
)

## write rep-periods-data file
tulipa_file = "rep-periods-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.rep_periods.data,
    "year",
    "data.csv",
    default_values,
)

## write rep-periods-mapping file
tulipa_file = "rep-periods-mapping.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.rep_periods.mapping,
    "year",
    "data.csv",
    default_values,
)

## write vintage assets data file
tulipa_file = "vintage-assets-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.vintage_assets_data,
    "assets",
    "basic-data.csv",
    default_values,
)

## write vintage flows data file
tulipa_file = "vintage-flows-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.vintage_flows_data,
    "flows-transport",
    "basic-data.csv",
    default_values,
)

## write year data file
tulipa_file = "year-data.csv"
process_user_files(
    user_input_dir,
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.year.data,
    "year-data",
    ".csv",
    default_values,
)

## write flows-rep-repriods-partitions data file
tulipa_file = "flows-rep-periods-partitions.csv"
process_flows_rep_period_partition_file(
    joinpath(tulipa_files_dir, "assets-rep-periods-partitions.csv"),
    joinpath(tulipa_files_dir, "flows-data.csv"),
    joinpath(tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.rep_periods_partition,
    default_values,
)

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