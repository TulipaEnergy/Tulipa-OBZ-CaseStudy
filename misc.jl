# Data Wrangling
using DataFrames
using CSV

input_dir = "tulipa-energy-model-files"

# create profile files
df = CSV.read(joinpath(input_dir, "profiles-rep-periods.csv"), DataFrame, header=2)
df_stacked = unstack(df, :profile_name, :value)
CSV.write("user-input-files/profiles-eu.csv", df_stacked)

# create assets files (non-year dependent)
df = CSV.read(joinpath(input_dir, "graph-assets-data.csv"), DataFrame, header=2)
df_hub = df[df.type.=="hub", [:name]]
df_hub.lat .= 0
df_hub.lon .= 0
df_consumer = df[df.type.=="consumer", [:name]]
df_consumer.lat .= 0
df_consumer.lon .= 0
df_producer = df[df.type.=="producer", [:name, :capacity]]
df_producer.lat .= 0
df_producer.lon .= 0
df_conversion = df[df.type.=="conversion", [:name, :capacity]]
df_conversion.lat .= 0
df_conversion.lon .= 0
df_storage = df[df.type.=="storage", [:name, :capacity, :capacity_storage_energy]]
df_storage.lat .= 0
df_storage.lon .= 0

CSV.write("user-input-files/assets-hub-basic-data.csv", df_hub)
CSV.write("user-input-files/assets-consumer-basic-data.csv", df_consumer)
CSV.write("user-input-files/assets-producer-basic-data.csv", df_producer)
CSV.write("user-input-files/assets-conversion-basic-data.csv", df_conversion)
CSV.write("user-input-files/assets-storage-basic-data.csv", df_storage)

# create assets files (year dependent)
df_year = CSV.read(joinpath(input_dir, "assets-data.csv"), DataFrame, header=2)
leftjoin!(df_year, df[:, [:name, :type]], on=:name)

df_hub_year = df_year[df_year.type.=="hub", [:name, :year]]
df_consumer_year = df_year[df_year.type.=="consumer", [:name, :year, :peak_demand]]
df_producer_year = df_year[df_year.type.=="producer", [:name, :year, :initial_units, :unit_commitment, :unit_commitment_method, :units_on_cost, :unit_commitment_integer, :min_operating_point, :ramping, :max_ramp_up, :max_ramp_down]]
df_conversion_year = df_year[df_year.type.=="conversion", [:name, :year, :initial_units, :unit_commitment, :unit_commitment_method, :units_on_cost, :unit_commitment_integer, :min_operating_point, :ramping, :max_ramp_up, :max_ramp_down]]
df_storage_year = df_year[df_year.type.=="storage", [:name, :year, :initial_units, :initial_storage_units, :initial_storage_level, :storage_inflows]]

CSV.write("user-input-files/assets-hub-yearly-data.csv", df_hub_year)
CSV.write("user-input-files/assets-consumer-yearly-data.csv", df_consumer_year)
CSV.write("user-input-files/assets-producer-yearly-data.csv", df_producer_year)
CSV.write("user-input-files/assets-conversion-yearly-data.csv", df_conversion_year)
CSV.write("user-input-files/assets-storage-yearly-data.csv", df_storage_year)

