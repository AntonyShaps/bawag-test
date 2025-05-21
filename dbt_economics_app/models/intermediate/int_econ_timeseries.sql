{{ config(
    materialized = 'incremental',
    unique_key = 'pk',
    incremental_strategy = 'merge'
) }}


with all_src as (

    -- OECD
    select
        'oecd' as src,
        geo_id,
        null   as counterpart_geo_id,
        variable,
        variable_name,
        date,
        value,
        unit
    from {{ ref('stg_oecd_timeseries') }}

    union all

    -- ECB
    select
        'ecb' as src,
        geo_id,
        null   as counterpart_geo_id,
        variable,
        variable_name,
        date,
        value,
        unit
    from {{ ref('stg_ecb_timeseries') }}

    union all

    -- IMF
    select
        'imf' as src,
        geo_id,
        counterpart_geo_id,
        variable,
        variable_name,
        date,
        value,
        unit
    from {{ ref('stg_imf_timeseries') }}

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
          'geo_id', 'counterpart_geo_id', 'variable', 'date', 'src'
        ]) }} as pk,
        *
    from all_src
)

select * from final
