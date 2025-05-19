select distinct
    base_currency_id
from
    {{source('fx_rates','fx_rates_timeseries')}}
