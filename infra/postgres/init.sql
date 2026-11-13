-- Warehouse Postgres (not Airflow's metadata DB): dbt target + Iceberg catalog.
create database iceberg_catalog owner lakehouse;
\connect warehouse
create schema if not exists raw authorization lakehouse;
