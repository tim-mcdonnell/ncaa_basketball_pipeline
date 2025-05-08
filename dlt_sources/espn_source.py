# dlt_sources/espn_source.py
"""dlt source definition for the ESPN API using RESTClient and consistent detail fetching."""

import logging
from collections.abc import Iterable
from typing import Any

import dlt
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dotenv import load_dotenv

API_LIMIT = 1000

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dlt.source(name="espn_source", max_table_nesting=0)
def espn_mens_college_basketball_source(
    base_url: str = dlt.config.value,
) -> Iterable[DltResource]:
    """
    Defines dlt resources for fetching NCAA Men's Basketball data from the ESPN API.

    Fetches season and season type details by listing references and then fetching details.

    Args:
        base_url (str): The base URL for the API, typically sourced from config/env.

    Returns:
        Iterable[DltResource]: An iterable containing the dlt resources (seasons, season_types).
    """
    # Check if base_url was successfully loaded from config/env
    if not base_url:
        logger.warning("Base URL not found in config/env. Using default.")
        # Default fallback if not provided in config
        base_url = (
            "http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
        )

    logger.info(f"Initializing ESPN source with base_url: {base_url}")

    # Client for endpoints that list items/refs and use page number pagination
    list_paginator = PageNumberPaginator(
        page_param="page", total_path="pageCount", base_page=1, stop_after_empty_page=True
    )
    list_client = RESTClient(  # Renamed client
        base_url=base_url, paginator=list_paginator, data_selector="items", headers={}
    )

    # Client for fetching single detail objects from absolute $ref URLs
    detail_client = RESTClient(base_url=None, headers={})  # Renamed client

    # --- Resources ---

    @dlt.resource(name="seasons", write_disposition="merge", primary_key="id")
    def seasons_resource() -> Iterable[dict[str, Any]]:
        """
        Fetches the list of season references and then fetches full details for each season.
        Yields the full season detail objects.
        """
        list_seasons_endpoint = "/seasons"
        api_params = {"limit": API_LIMIT}  # Use constant
        seasons_processed_count = 0
        logger.info(f"Fetching season references from {base_url}{list_seasons_endpoint}")

        try:
            # Paginate through the list of season $ref objects using list_client
            for page_of_refs in list_client.paginate(list_seasons_endpoint, params=api_params):
                if not page_of_refs:
                    continue

                for season_ref_item in page_of_refs:
                    detail_url = season_ref_item.get("$ref")
                    if not detail_url:
                        logger.warning(
                            f"Season reference item missing '$ref' key. Item: {season_ref_item}"
                        )
                        continue

                    # Fetch the actual season detail using detail_client
                    try:
                        response = detail_client.get(detail_url)
                        response.raise_for_status()
                        season_detail = response.json()

                        if season_detail.get("id") is not None:
                            yield season_detail
                            seasons_processed_count += 1
                        else:
                            logger.warning(
                                f"Fetched season detail from {detail_url} is missing 'id'."
                                f" Detail: {season_detail}"
                            )

                    except requests.exceptions.HTTPError as he:
                        response_text = he.response.text if he.response else "No response body"
                        logger.error(
                            f"HTTPError fetching season detail from {detail_url}: {he}. "
                            f"Response: {response_text}"
                        )
                    except Exception as ex:
                        logger.error(
                            f"Unexpected error fetching season detail from {detail_url}: {ex}"
                        )

            logger.info(
                f"Finished processing seasons resource, yielded details for "
                f"{seasons_processed_count} seasons."
            )

        except requests.exceptions.HTTPError as e:
            response_text = e.response.text if e.response else "No response body"
            logger.error(
                f"HTTPError listing season refs from {list_seasons_endpoint}: {e}. "
                f"Response: {response_text}"
            )
        except Exception as e:
            logger.error(f"Unexpected error listing season refs from {list_seasons_endpoint}: {e}")
            raise

    @dlt.transformer(
        name="season_types",
        data_from=seasons_resource,
        write_disposition="merge",
        primary_key=["id", "season_id_fk"],
    )
    def season_types_transformer(season_detail: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        For a given season detail object, fetches the list of season type references
        and then fetches the full details for each season type.
        Yields the full season type detail objects.
        """
        season_id = season_detail.get("id")
        if not season_id:
            logger.warning(
                f"Received season detail object without 'id'. "
                f"Skipping season types. Object: {season_detail}"
            )
            return

        logger.info(f"Processing season types for season_id '{season_id}'.")
        list_types_endpoint_relative = f"/seasons/{season_id}/types"
        api_params = {"limit": API_LIMIT}  # Use constant
        items_yielded_for_this_season = 0

        try:
            for page_of_refs in list_client.paginate(
                list_types_endpoint_relative, params=api_params
            ):
                if not page_of_refs:
                    continue

                for type_ref_item in page_of_refs:
                    detail_url = type_ref_item.get("$ref")
                    if not detail_url:
                        logger.warning(
                            f"Season type reference item for season_id '{season_id}' "
                            f"missing '$ref'. Item: {type_ref_item}"
                        )
                        continue

                    try:
                        response = detail_client.get(detail_url)
                        response.raise_for_status()
                        season_type_detail = response.json()
                        season_type_detail["season_id_fk"] = str(season_id)

                        detail_id = season_type_detail.get("id")
                        if detail_id is not None and str(detail_id).strip() != "":
                            yield season_type_detail
                            items_yielded_for_this_season += 1
                        else:
                            logger.warning(
                                f"Fetched season type detail for season_id '{season_id}' "
                                f"from {detail_url} is MISSING 'id' or 'id' is empty. "
                                f"Detail: {season_type_detail}"
                            )

                    except requests.exceptions.HTTPError as he:
                        response_text = he.response.text if he.response else "No response body"
                        logger.error(
                            f"HTTPError fetching season type detail from {detail_url} "
                            f"(for season_id '{season_id}'): {he}. Response: {response_text}"
                        )
                    except Exception as ex:
                        logger.error(
                            f"Unexpected error fetching season type detail from {detail_url} "
                            f"(for season_id '{season_id}'): {ex}"
                        )

            logger.info(
                f"Finished processing season types for season_id '{season_id}', "
                f"yielded {items_yielded_for_this_season} items."
            )

        except requests.exceptions.HTTPError as e:
            response_text = e.response.text if e.response else "No response body"
            logger.error(
                f"HTTPError listing season type refs for season_id '{season_id}': {e}. "
                f"Response: {response_text}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error listing season type refs for season_id '{season_id}': {e}"
            )

    @dlt.transformer(
        name="weeks",
        data_from=season_types_transformer,
        write_disposition="merge",
        primary_key=["id", "type_id_fk", "season_id_fk"],
    )
    def weeks_transformer(season_type_detail: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        For a given season type detail, fetches the list of week references
        and then fetches full details for each week.
        Yields the full week detail objects, augmented with foreign keys.
        """
        type_id = season_type_detail.get("id")
        season_id_fk = season_type_detail.get("season_id_fk")

        if not type_id or not season_id_fk:
            logger.warning(
                f"Received season type detail object without 'id' or 'season_id_fk'. "
                f"Skipping weeks. Object: {season_type_detail}"
            )
            return

        logger.info(f"Processing weeks for type_id '{type_id}' and season_id_fk '{season_id_fk}'.")
        list_weeks_endpoint_relative = f"/seasons/{season_id_fk}/types/{type_id}/weeks"
        api_params = {"limit": API_LIMIT}
        items_yielded_for_this_type = 0

        try:
            for page_of_refs in list_client.paginate(
                list_weeks_endpoint_relative, params=api_params
            ):
                if not page_of_refs:
                    continue

                for week_ref_item in page_of_refs:
                    detail_url = week_ref_item.get("$ref")
                    if not detail_url:
                        logger.warning(
                            f"Week reference item for type_id '{type_id}', "
                            f"season_id_fk '{season_id_fk}' missing '$ref'. "
                            f"Item: {week_ref_item}"
                        )
                        continue

                    try:
                        response = detail_client.get(detail_url)
                        response.raise_for_status()
                        week_detail = response.json()

                        # API uses 'number' for week identifier, we'll use 'id'
                        week_id_from_api = week_detail.pop("number", None)

                        if week_id_from_api is not None and str(week_id_from_api).strip() != "":
                            week_detail["id"] = str(week_id_from_api)
                            week_detail["type_id_fk"] = str(type_id)
                            week_detail["season_id_fk"] = str(season_id_fk)
                            yield week_detail
                            items_yielded_for_this_type += 1
                        else:
                            logger.warning(
                                f"Fetched week detail for type_id '{type_id}', "
                                f"season_id_fk '{season_id_fk}' from {detail_url} "
                                f"is MISSING 'number' (expected as 'id') or 'number' is empty. "
                                f"Detail: {week_detail}"
                            )

                    except requests.exceptions.HTTPError as he:
                        response_text = he.response.text if he.response else "No response body"
                        logger.error(
                            f"HTTPError fetching week detail from {detail_url} "
                            f"(for type_id '{type_id}', season_id_fk '{season_id_fk}'): {he}. "
                            f"Response: {response_text}"
                        )
                    except Exception as ex:
                        logger.error(
                            f"Unexpected error fetching week detail from {detail_url} "
                            f"(for type_id '{type_id}', season_id_fk '{season_id_fk}'): {ex}"
                        )

            logger.info(
                f"Finished processing weeks for type_id '{type_id}', "
                f"season_id_fk '{season_id_fk}', yielded {items_yielded_for_this_type} items."
            )

        except requests.exceptions.HTTPError as e:
            response_text = e.response.text if e.response else "No response body"
            logger.error(
                f"HTTPError listing week refs for type_id '{type_id}', "
                f"season_id_fk '{season_id_fk}': {e}. Response: {response_text}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error listing week refs for type_id '{type_id}', "
                f"season_id_fk '{season_id_fk}': {e}"
            )

    return seasons_resource, season_types_transformer, weeks_transformer


if __name__ == "__main__":
    load_dotenv()

    logger.info("Running dlt pipeline locally (standalone execution)...")

    pipeline = dlt.pipeline(
        pipeline_name="espn_api_standalone_test",
        destination="duckdb",
        dataset_name="espn_standalone_data",
        progress="enlighten",
    )

    source_instance = espn_mens_college_basketball_source()
    load_info = pipeline.run(source_instance)

    logger.info("\n--- Load Info ---")
    logger.info(load_info)
    logger.info("-----------------\n")

    if load_info.has_failed_jobs:
        logger.error("Pipeline run failed or had errors.")
    else:
        logger.info("Pipeline run completed successfully.")
