"""Dagster asset definitions for the ESPN dlt pipeline."""

import dlt
from dagster import AssetExecutionContext, StaticPartitionsDefinition
from dagster_dlt import DagsterDltResource, dlt_assets

from dlt_sources.espn_source import espn_mens_college_basketball_source

# Define season partitions
# The ESPN API often uses the year the season *ends* as the identifier for a season.
# For example, the 2022-2023 season is identified by "2023".
# Adjust this range and format as necessary for your specific API and needs.
SEASON_YEARS = [
    str(year) for year in range(2003, 2024 + 1)
]  # e.g., 2003 (for 02-03) to 2024 (for 23-24)
season_partitions = StaticPartitionsDefinition(SEASON_YEARS)

espn_dlt_pipeline_instance = dlt.pipeline(
    pipeline_name="ncaa_basketball_prod_pipeline",
    destination="duckdb",
    dataset_name="ncaa_basketball_prod_data",
)


@dlt_assets(
    dlt_source=espn_mens_college_basketball_source(),
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

    source_instance = espn_mens_college_basketball_source(season_year=season_to_process)

    yield from dlt.run(context=context, dlt_source=source_instance)

    context.log.info(f"dlt pipeline run for ESPN data, season: {season_to_process}, finished.")
