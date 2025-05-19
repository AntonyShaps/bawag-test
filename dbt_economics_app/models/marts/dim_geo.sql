
with all_geos as (

    -- Distinct geo_id from OECD
    select distinct geo_id
    from {{ ref('stg_oecd_timeseries') }}

    union

    -- Distinct geo_id from ECB
    select distinct geo_id
    from {{ ref('stg_ecb_timeseries') }}

    union

    -- Distinct geo_id from IMF
    select distinct geo_id
    from {{ ref('stg_imf_timeseries') }}

    union

    -- Distinct counterpart_geo_id from IMF (renamed as geo_id)
    select distinct counterpart_geo_id as geo_id
    from {{ ref('stg_imf_timeseries') }}
    where counterpart_geo_id is not null

),

dim_geo as (

    select
        geo_id,

        -- Extract type from prefix
        case
            when geo_id like 'country/%' then 'Country'
            when geo_id like 'continent/%' then 'Continent'
            when geo_id like 'countryGroup/%' then 'Country Group'
            else 'Ambigious Data'
        end as geo_type,

        -- Extract code (e.g. DEU, EU, OECD)
        split_part(geo_id, '/', 2) as geo_code,

        -- Placeholder columns (can be enriched via seed or join)
        null as geo_name,
        null as iso_alpha2,
        null as region_group

    from all_geos
)

select * from dim_geo
