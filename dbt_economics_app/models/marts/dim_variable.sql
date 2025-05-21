
with used_variables as (
    select distinct variable
    from {{ ref('fct_econ_comparison') }}
)

select
    a.variable,
    a.variable_name,
    a.frequency,
    a.measure,
    a.measurement_type,
    a.seasonally_adjusted,
    a.unit,
    a.geo_coverage,
    a.item_description,
    a.source as data_source,
    a.src as source_system
from {{ ref('int_econ_attributes') }} a
join used_variables u
  on a.variable = u.variable
