select
    *
from 
    {{ source('economics', 'fx_rates_timeseries')}}
