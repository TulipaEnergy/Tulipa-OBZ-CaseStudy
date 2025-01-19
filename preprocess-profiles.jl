# Define the path to the input files
input_profiles_file = joinpath(user_input_dir, "profiles.csv")
input_min_max_reservoir_profiles_file = joinpath(user_input_dir, "min-max-reservoir-levels.csv")

# Define the path to the output files
output_profiles_file = joinpath(tulipa_files_dir, "profiles-rep-periods.csv")
output_mapping_file = joinpath(tulipa_files_dir, "rep-periods-mapping.csv")
output_rp_file = joinpath(tulipa_files_dir, "rep-periods-data.csv")
output_timeframe_profiles_file = joinpath(tulipa_files_dir, "profiles-timeframe.csv")
output_timeframe_data_file = joinpath(tulipa_files_dir, "timeframe-data.csv")

# Read the input profiles
profiles_wide_format = CSV.read(input_profiles_file, DataFrame)
columns = setdiff(names(profiles_wide_format), ["year", "timestep"])
profiles_long_format =
    stack(profiles_wide_format, columns; variable_name = :profile_name, value_name = :value)

# Read the input min-max reservoir profiles
min_max_reservoir_profiles = CSV.read(input_min_max_reservoir_profiles_file, DataFrame)
columns = setdiff(names(min_max_reservoir_profiles), ["year", "timestep"])
min_max_reservoir_profiles_long_format =
    stack(min_max_reservoir_profiles, columns; variable_name = :profile_name, value_name = :value)

# If n_rp = 1 (full-year optimization) then append the min-max reservoir profiles to the profiles
if n_rp == 1
    profiles_long_format = vcat(profiles_long_format, min_max_reservoir_profiles_long_format)
end

# Run TulipaClustering
TulipaClustering.split_into_periods!(profiles_long_format; period_duration)
clustering_result =
    TulipaClustering.find_representative_periods(profiles_long_format, n_rp; method, distance)
TulipaClustering.fit_rep_period_weights!(
    clustering_result;
    weight_type,
    tol,
    niters,
    learning_rate,
    adaptive_grad,
)

# Save the profiles
clustered_profiles = clustering_result.profiles
clustered_profiles =
    select(clustered_profiles, [:profile_name, :year, :rep_period, :timestep, :value])
open(output_profiles_file, "w") do io
    println(io, join(["", "", "", "", "p.u."], ","))
end
CSV.write(output_profiles_file, clustered_profiles; append = true, writeheader = true)

# Save the mapping data
mapping = TulipaClustering.weight_matrix_to_df(clustering_result.weight_matrix)
mapping.year .= default_values["year"]
mapping = select(mapping, [:year, :period, :rep_period, :weight])
open(output_mapping_file, "w") do io
    println(io, join(["", "", "", "p.u."], ","))
end
CSV.write(output_mapping_file, mapping; append = true, writeheader = true)

# Save the representative period data
first_clustered_profile =
    filter(row -> row.profile_name == first(clustered_profiles.profile_name), clustered_profiles)
rp_data = combine(
    groupby(first_clustered_profile, [:year, :rep_period]),
    :timestep => length => :num_timesteps,
)
rp_data.resolution .= 1.0
open(output_rp_file, "w") do io
    println(io, join(["", "", "", ""], ","))
end
CSV.write(output_rp_file, rp_data; append = true, writeheader = true)

# Write extra files for n_rp > 1
if n_rp > 1
    TulipaClustering.split_into_periods!(min_max_reservoir_profiles_long_format; period_duration)
    # groupby profile_name, year, period aggregating the value column with mean
    grouped_min_max_reservoir_profiles_long_format = combine(
        groupby(min_max_reservoir_profiles_long_format, [:profile_name, :year, :period]),
        :value => Statistics.mean => :value,
    )
    open(output_timeframe_profiles_file, "w") do io
        println(io, join(["", "", "", "p.u."], ","))
    end
    CSV.write(
        output_timeframe_profiles_file,
        grouped_min_max_reservoir_profiles_long_format;
        append = true,
        writeheader = true,
    )
    first_profile = filter(
        row -> row.profile_name == first(profiles_long_format.profile_name),
        profiles_long_format,
    )
    timeframe_data =
        combine(groupby(first_profile, [:year, :period]), :timestep => length => :num_timesteps)
    open(output_timeframe_data_file, "w") do io
        println(io, join(["", "", ""], ","))
    end
    CSV.write(output_timeframe_data_file, timeframe_data; append = true, writeheader = true)
end