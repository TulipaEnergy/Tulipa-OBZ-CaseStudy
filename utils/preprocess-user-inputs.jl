## write asset-both data file
tulipa_file = "asset-both.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.both,
    "assets",
    "yearly-data.csv",
    default_values;
    map_to_rename_user_columns = Dict("name" => "asset"),
)

## write asset-commission file
tulipa_file = "asset-commission.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.commission,
    "assets",
    "yearly-data.csv",
    default_values;
    map_to_rename_user_columns = Dict("name" => "asset"),
)

## write asset-milestone file
tulipa_file = "asset-milestone.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.milestone,
    "assets",
    "yearly-data.csv",
    default_values;
    map_to_rename_user_columns = Dict("name" => "asset"),
)

## write asset file
tulipa_file = "asset.csv"
assets = process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.assets.basic,
    "assets",
    "basic-data.csv",
    default_values;
    map_to_rename_user_columns = Dict("name" => "asset"),
)

## if n_rp = 1 (full-year optimization) update is_seasonal to false
if n_rp == 1
    assets.is_seasonal .= false
    output_file = joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file)
    CSV.write(output_file, assets; append = false, writeheader = true)
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

## write flow both file
tulipa_file = "flow-both.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.both,
    "flows",
    "yearly-data.csv",
    default_values,
)

## write flow commission file
tulipa_file = "flow-commission.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.commission,
    "flows",
    "yearly-data.csv",
    default_values,
)

## write flow milestone file
tulipa_file = "flow-milestone.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.milestone,
    "flows",
    "yearly-data.csv",
    default_values,
)

## write flow file
tulipa_file = "flow.csv"
process_user_files(
    joinpath(@__DIR__, "..", user_input_dir),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.basic,
    "flows",
    "basic-data.csv",
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
    joinpath(@__DIR__, "..", tulipa_files_dir, "flow.csv"),
    joinpath(@__DIR__, "..", tulipa_files_dir, tulipa_file),
    TulipaEnergyModel.schemas.flows.rep_periods_partition,
    default_values,
)
