# ncaa_basketball_pipeline/ncaa_basketball_pipeline/definitions.py
"""
This module defines the main Definitions object for the NCAA Basketball Dagster project.
It brings together assets, resources, jobs, schedules, and sensors.
"""

from dagster import Definitions
from dagster_dlt import DagsterDltResource

# Import assets from the assets.py module
from .assets import espn_data_load_assets

# Define the resources required by the assets or other definitions
# The DagsterDltResource is needed for @dlt_assets to function.
# The key "dlt" matches the default parameter name in @dlt_assets.
RESOURCES = {
    "dlt": DagsterDltResource(),
}

# Create the main Definitions object for Dagster to load
defs = Definitions(
    assets=[espn_data_load_assets],  # List all assets to be included
    resources=RESOURCES,
    # You can also add jobs, schedules, sensors here, for example:
    # jobs=[my_job],
    # schedules=[my_schedule],
    # sensors=[my_sensor],
)
