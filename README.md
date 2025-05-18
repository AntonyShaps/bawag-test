# bawag-test

use role accountadmin;

create warehouse dbt_economics_wh with warehouse_size='x-small';
create database dbt_economics_db;
create role dbt_economics_role;
drop schema public;

show grants on warehouse dbt_economics_wh;

grant usage on warehouse dbt_economics_wh to role dbt_economics_role;
grant usage on database dbt_economics_db to role dbt_economics_role;
grant create schema on database dbt_economics_db to role dbt_economics_role;
grant role dbt_economics_role to user antonshapovalov111;

use role dbt_economics_role;

create schema dbt_economics_db.dbt_schema;
