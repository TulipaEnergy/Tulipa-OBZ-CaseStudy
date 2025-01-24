## write graph assets data file
tulipa_file = "graph-assets-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.graph.assets,
    "assets",
    "basic-data.csv",
    default_values,
)

## write assets data file
tulipa_file = "assets-data.csv"
assets_data = process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.data,
    "assets",
    "yearly-data.csv",
    default_values,
)

## if n_rp = 1 (full-year optimization) update is_seasonal to false
if n_rp == 1
    assets_data.is_seasonal .= false
    output_file = joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file)
    open(output_file, "w") do io
        println(io, repeat(",", size(assets_data, 2) - 1))
    end
    CSV.write(output_file, assets_data; append = true, writeheader = true)
end

## write assets-profiles data file
tulipa_file = "assets-profiles.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.profiles_reference,
    "assets",
    "profiles.csv",
    default_values,
)

## write assets-timeframe-profiles.csv file
tulipa_file = "assets-timeframe-profiles.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.profiles_reference,
    "assets",
    "min-max-reservoir-level-profiles.csv",
    default_values,
)

## write assets-rep-periods-partitions data file
tulipa_file = "assets-rep-periods-partitions.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.rep_periods_partition,
    "assets",
    "yearly-data.csv",
    default_values;
    map_to_rename_user_columns = Dict("name" => "asset"),
    number_of_rep_periods = n_rp,
)

## write graph flows data file
tulipa_file = "graph-flows-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.graph.flows,
    "flows",
    "basic-data.csv",
    default_values,
)

## write flow data file
tulipa_file = "flows-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.data,
    "flows",
    "yearly-data.csv",
    default_values,
)

## write flows profiles data file
tulipa_file = "flows-profiles.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.profiles_reference,
    "flows",
    "profiles.csv",
    default_values,
)

## write vintage assets data file
tulipa_file = "vintage-assets-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.vintage_assets_data,
    "assets",
    "basic-data.csv",
    default_values,
)

## write vintage flows data file
tulipa_file = "vintage-flows-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.vintage_flows_data,
    "flows-transport",
    "basic-data.csv",
    default_values,
)

## write year data file
tulipa_file = "year-data.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.year.data,
    "year-data",
    ".csv",
    default_values,
)

## write flows-rep-repriods-partitions data file
tulipa_file = "flows-rep-periods-partitions.csv"
process_flows_rep_period_partition_file(
    joinpath(@__DIR__, "..", tulipa_files_dir, "assets-rep-periods-partitions.csv"),
    joinpath(@__DIR__, "..", tulipa_files_dir, "flows-data.csv"),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.rep_periods_partition,
    default_values,
)
