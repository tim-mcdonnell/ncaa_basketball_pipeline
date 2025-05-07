# ncaa_basketball_pipeline/ncaa_basketball_pipeline/assets.py
"""Dagster asset definitions for the ESPN dlt pipeline."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

# Adjusted import path for the dlt source.
from dlt_sources.espn_source import espn_mens_college_basketball_source

# Define the dlt pipeline configuration that assets will use.
# The DuckDB path will be picked up from the DLT_DESTINATION__DUCKDB__CREDENTIALS__DATABASE env var.
espn_dlt_pipeline_instance = dlt.pipeline(
    pipeline_name="ncaa_basketball_prod_pipeline",
    destination="duckdb",
    dataset_name="ncaa_basketball_prod_data",  # This will be the schema in DuckDB
)


# Use the @dlt_assets decorator to create Dagster assets from the dlt source.
@dlt_assets(
    dlt_source=espn_mens_college_basketball_source(),  # Invoke the source function
    dlt_pipeline=espn_dlt_pipeline_instance,
    name="espn_api_assets",  # A friendly name for this group of Dagster assets
    group_name="espn_api",  # Group name for organizing assets in Dagster UI
)
def espn_data_load_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,  # CORRECTED: Parameter name changed to 'dlt'
):
    """
    Dagster assets definition that uses the DagsterDltResource to run the dlt pipeline.
    The 'dlt' parameter name now matches the key used for DagsterDltResource
    in the Definitions object ("dlt").
    """
    context.log.info("Starting dlt pipeline run for ESPN data via Dagster.")
    # CORRECTED: Using 'dlt' to call the run method
    yield from dlt.run(dlt_source=espn_mens_college_basketball_source(), context=context)
    context.log.info("dlt pipeline run for ESPN data finished.")


# Note: The Definitions object is no longer created here.
# It is now created in definitions.py.
