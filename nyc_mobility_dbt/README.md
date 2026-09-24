# nyc_mobility_dbt, traffic advisory slice

dbt project for the traffic advisory source of the NYC Mobility pipeline. Part 2 of the
Week 9 homework: research one tool, apply it to one meaningful part, evaluate.

Scope: everything after ingestion. The Python scrape that lands the DOT page stays as a
Databricks notebook task. dbt starts at nyc_mobility.raw.traffic_advisory.

    models/staging/traffic_advisory/   source declaration, freshness, newest-scrape view
    models/clean/                      Type 2 history as an incremental merge model
    tests/                             the strict and warn checks that have no built-in test

Run order: dbt build. Sources are tested, the staging view builds, the incremental model
merges on closure_bk, then the tests run against the result.

Everything builds in your own dev schema (mine is dbt_kinah), not in nyc_mobility.clean, so
the notebook tables and the validation tables are never touched.

Before you run it: nyc_mobility.raw.traffic_advisory has to exist in your workspace. Run
src/01_raw/04_traffic_advisory_raw once if it does not.

dbt Cloud: set the project subdirectory to nyc_mobility_dbt, then switch the Studio branch to
test/dbt-traffic-advisory. Step by step setup is in the NYC_Mobility_pipeline 2.0 doc,
Engineer 4, subtab dbt Cloud setup, step by step.

Tested 24 Sep in dbt Cloud: dbt build 17 of 17 twice on the same scrape, counts unchanged.

This branch and test/dbt-green-taxi-clean both declare a source called raw. When the two
projects become one, those source blocks merge into a single file.
