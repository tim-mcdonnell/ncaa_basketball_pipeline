"""Dagster asset definitions for the ESPN dlt pipeline."""

import dlt
from dagster import AssetExecutionContext, StaticPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets

from dlt_sources.espn_source import espn_source

SEASON_YEARS = [
    str(year) for year in range(2025, 2003 - 1, -1)
]  # e.g., 2024 (for 23-24) to 2003 (for 02-03)
season_partitions = StaticPartitionsDefinition(SEASON_YEARS)

espn_dlt_pipeline_instance = dlt.pipeline(
    pipeline_name="ncaa_basketball_prod_pipeline",
    destination="duckdb",
    dataset_name="espn_ncaab_data",
)


@dlt_assets(
    dlt_source=espn_source(),
    dlt_pipeline=espn_dlt_pipeline_instance,
    name="espn_api_assets",
    group_name="espn_api",
    partitions_def=season_partitions,
)
def espn_data_load_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    """
    Dagster assets definition that uses the DagsterDltResource to run the dlt pipeline,
    partitioned by season. The 'dlt' parameter name matches the key used for
    DagsterDltResource in the Definitions object.
    """
    season_to_process = context.partition_key
    context.log.info(f"Starting dlt pipeline run for ESPN data, season: {season_to_process}")

    source_instance = espn_source(season_year_filter=season_to_process)

    yield from dlt.run(context=context, dlt_source=source_instance)

    context.log.info(f"dlt pipeline run for ESPN data, season: {season_to_process}, finished.")
