{{ config(
    materialized = 'table',
    cluster_by = ['DATE', 'GEO_ID']
) }}

-- CDIS-only slice of FCT_ECON_COMPARISON
select
    *
from {{ ref('fct_econ_comparison') }}
where
      variable_name in (
          'Coordinated Direct Investment Survey: Outward Direct Investment Positions (Net) with Fellow Enterprises, Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Outward Equity Positions (Net), Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Outward Debt Instruments Positions (Net), Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Outward Debt Positions (Net): Resident Enterprises that are not Financial Intermediaries, Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Outward Debt Positions (Net): Resident Financial Intermediaries, Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Inward Direct Investment Positions (Net) with Fellow Enterprises, Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Inward Equity Positions (Net), Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Inward Debt Instruments Positions (Net), Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Inward Debt Positions (Net): Resident Enterprises that are not Financial Intermediaries, Derived | USD | Annual',
          'Coordinated Direct Investment Survey: Inward Debt Positions (Net): Resident Financial Intermediaries, Derived | USD | Annual'
      )
  and geo_id like 'country/%'
  and counterpart_geo_id like 'country/%'
  and date > '2019-12-31'

