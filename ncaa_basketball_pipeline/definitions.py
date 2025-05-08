"""
This module defines the main Definitions object for the NCAA Basketball Dagster project.
It brings together assets, resources, jobs, schedules, and sensors.
"""

from dagster import Definitions
from dagster_dlt import DagsterDltResource

from .assets import espn_data_load_assets

RESOURCES = {
    "dlt": DagsterDltResource(),
}

defs = Definitions(
    assets=[espn_data_load_assets],
    resources=RESOURCES,
)
