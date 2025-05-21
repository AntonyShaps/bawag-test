with available_vars as (
    select distinct variable
    from {{ ref('int_econ_timeseries') }}
    where geo_id = 'country/AUT'
)

select
    t.*
from {{ ref('int_econ_timeseries') }} t
join available_vars v
  on t.variable = v.variable

