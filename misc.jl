# Data Wrangling
using DataFrames
using CSV

input_dir = "tulipa-energy-model-files"

# create profile files
df = CSV.read(joinpath(input_dir, "profiles-rep-periods.csv"), DataFrame; header = 2)
df_stacked = unstack(df, :profile_name, :value)
CSV.write("user-input-files/profiles-eu.csv", df_stacked)

# create assets files (non-year dependent)
df = CSV.read(joinpath(input_dir, "graph-assets-data.csv"), DataFrame; header = 2)
df_hub = df[df.type.=="hub", [:name, :type]]
df_hub.lat .= 0
df_hub.lon .= 0
df_consumer = df[df.type.=="consumer", [:name, :type]]
df_consumer.lat .= 0
df_consumer.lon .= 0
df_producer = df[df.type.=="producer", [:name, :type, :capacity]]
df_producer.lat .= 0
df_producer.lon .= 0
df_conversion = df[df.type.=="conversion", [:name, :type, :capacity]]
df_conversion.lat .= 0
df_conversion.lon .= 0
df_storage = df[df.type.=="storage", [:name, :type, :capacity, :capacity_storage_energy]]
df_storage.lat .= 0
df_storage.lon .= 0

CSV.write("user-input-files/assets-hub-basic-data.csv", df_hub)
CSV.write("user-input-files/assets-consumer-basic-data.csv", df_consumer)
CSV.write("user-input-files/assets-producer-basic-data.csv", df_producer)
CSV.write("user-input-files/assets-conversion-basic-data.csv", df_conversion)
CSV.write("user-input-files/assets-storage-basic-data.csv", df_storage)

# create assets files (year dependent)
df_year = CSV.read(joinpath(input_dir, "assets-data.csv"), DataFrame; header = 2)
leftjoin!(df_year, df[:, [:name, :type]]; on = :name)

df_hub_year = df_year[df_year.type.=="hub", [:name, :year]]
df_consumer_year = df_year[df_year.type.=="consumer", [:name, :year, :peak_demand]]
df_producer_year = df_year[
    df_year.type.=="producer",
    [
        :name,
        :year,
        :initial_units,
        :unit_commitment,
        :unit_commitment_method,
        :units_on_cost,
        :unit_commitment_integer,
        :min_operating_point,
        :ramping,
        :max_ramp_up,
        :max_ramp_down,
    ],
]
df_conversion_year = df_year[
    df_year.type.=="conversion",
    [
        :name,
        :year,
        :initial_units,
        :unit_commitment,
        :unit_commitment_method,
        :units_on_cost,
        :unit_commitment_integer,
        :min_operating_point,
        :ramping,
        :max_ramp_up,
        :max_ramp_down,
    ],
]
df_storage_year = df_year[
    df_year.type.=="storage",
    [
        :name,
        :year,
        :initial_units,
        :initial_storage_units,
        :initial_storage_level,
        :storage_inflows,
    ],
]

CSV.write("user-input-files/assets-hub-yearly-data.csv", df_hub_year)
CSV.write("user-input-files/assets-consumer-yearly-data.csv", df_consumer_year)
CSV.write("user-input-files/assets-producer-yearly-data.csv", df_producer_year)
CSV.write("user-input-files/assets-conversion-yearly-data.csv", df_conversion_year)
CSV.write("user-input-files/assets-storage-yearly-data.csv", df_storage_year)

# create flow files (non-year dependent)
df = CSV.read(joinpath(input_dir, "graph-flows-data.csv"), DataFrame; header = 2)

df_assets_connections = df[df.is_transport.==false, [:from_asset, :to_asset, :carrier]]
df_trasport_assets =
    df[df.is_transport.==true, [:from_asset, :to_asset, :carrier, :is_transport, :capacity]]

CSV.write("user-input-files/flows-assets-connections-basic-data.csv", df_assets_connections)
CSV.write("user-input-files/flows-trasport-assets-basic-data.csv", df_trasport_assets)

# create flows files (year dependent)
df_year = CSV.read(joinpath(input_dir, "flows-data.csv"), DataFrame; header = 2)
leftjoin!(df_year, df[:, [:from_asset, :to_asset, :is_transport]]; on = [:from_asset, :to_asset])

df_assets_connections_year = df_year[
    df_year.is_transport.==false,
    [:from_asset, :to_asset, :year, :variable_cost, :efficiency],
]
df_transport_assets_year = df_year[
    df_year.is_transport.==true,
    [:from_asset, :to_asset, :year, :initial_export_units, :initial_import_units],
]

CSV.write("user-input-files/flows-assets-connections-yearly-data.csv", df_assets_connections_year)
CSV.write("user-input-files/flows-trasport-assets-yearly-data.csv", df_transport_assets_year)

# create the min storage level profiles
input_dir = "user-input-files"

# create profile files
df = CSV.read(joinpath(input_dir, "max-reservoir-levels.csv"), DataFrame)

_df = DataFrame(Dict(col => Vector{eltype(df[!, col])}() for col in names(df)))

time_step = 1
for row in eachrow(df)
    for _ in 1:row[:Hours]
        row.Hours = time_step
        time_step += 1
        push!(_df, row)
    end
end

CSV.write("user-input-files/max-reservoir-levels-profiles.csv", _df)