{{ config(
    materialized='view'
) }}


with all_src as (

    -- OECD
    select
        'oecd' as src,
        frequency,
        null as geo_coverage,
        null as item_description,
        measure,
        measurement_type,
        seasonally_adjusted,
        null as source,
        variable,
        variable_name,
        unit
    from {{ ref('stg_oecd_attributes') }}

    union all

    -- ECB
    select
        'ecb' as src,
        frequency,
        geo_coverage,
        item_description,
        measure,
        measurement_type,
        seasonally_adjusted,
        source,
        variable,
        variable_name,
        unit
    from {{ ref('stg_ecb_attributes') }}

    union all

    -- IMF
    select
        'imf' as src,
        frequency,
        null as geo_coverage,
        null as item_description,
        measure,
        measurement_type,
        null as seasonally_adjusted,
        null as source,
        variable,
        variable_name,
        unit
    from {{ ref('stg_imf_attributes') }}

)


select * from all_src
