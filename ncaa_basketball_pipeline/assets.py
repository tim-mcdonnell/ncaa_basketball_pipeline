"""Dagster asset definitions for the ESPN dlt pipeline."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from dlt_sources.espn_source import espn_mens_college_basketball_source

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
)
def espn_data_load_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    """
    Dagster assets definition that uses the DagsterDltResource to run the dlt pipeline.
    The 'dlt' parameter name now matches the key used for DagsterDltResource
    in the Definitions object ("dlt").
    """
    context.log.info("Starting dlt pipeline run for ESPN data via Dagster.")

    yield from dlt.run(dlt_source=espn_mens_college_basketball_source(), context=context)
    context.log.info("dlt pipeline run for ESPN data finished.")
