# dlt_sources/espn_source_poc_root_v2.py
"""
dlt source POC: Fetches ESPN API data starting from the league's root URL.
Demonstrates the "Lister + Detail Fetcher with @dlt.defer" pattern for
Seasons and Season Types.
"""

import logging
from collections.abc import Iterable
from typing import Any

import dlt
from dlt.common.typing import TDataItem
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests  # For exception handling
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# --- Configuration & Constants ---
API_LIMIT = 1000  # Max items per page for list endpoints

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --- Main Source Definition ---
@dlt.source(name="espn_source", max_table_nesting=0)
def espn_source(
    league_base_url: str = dlt.config.value,
    season_year_filter: str | None = None,
) -> Iterable[DltResource]:
    """
    Defines dlt resources for fetching NCAA Men's Basketball data from the ESPN API,
    starting with the league's root document and using Lister/Fetcher patterns.

    Args:
        league_base_url (str): The base URL for the specific league.
                                Example: "http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
                                This will be read from dlt.config.value
                                (e.g., env var SOURCES__ESPN_SOURCE__BASE_URL).
        season_year_filter (str | None): If provided, only this season will be processed.

    Returns:
        Iterable[DltResource]: An iterable containing the dlt resources.
    """
    if not league_base_url:
        league_base_url = (
            "http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
        )
        logger.warning(f"league_base_url not configured, using default: {league_base_url}")

    # Client for LISTING items from collection endpoints (e.g., a list of season $refs)
    # This client's base_url will effectively be ignored if full URLs are passed to paginate/get.
    # It's primarily for its paginator and data_selector.
    list_paginator = PageNumberPaginator(
        page_param="page", total_path="pageCount", base_page=1, stop_after_empty_page=True
    )
    list_client = RESTClient(base_url=None, paginator=list_paginator, data_selector="items")

    # Client for fetching single DETAIL objects from absolute $ref URLs.
    # base_url=None because $ref URLs are absolute.
    detail_client = RESTClient(base_url=None)  # No paginator needed for single detail fetches

    # --- League Root Information Resource ---
    @dlt.resource(name="league_info", write_disposition="replace", primary_key="id")
    def league_info_resource() -> Iterable[dict[str, Any]]:
        logger.info(f"Fetching league root information from: {league_base_url}")
        try:
            response = detail_client.get(league_base_url)
            response.raise_for_status()
            league_data = response.json()

            # Ensure a primary key for the league_info table
            if "id" not in league_data and "uid" in league_data:
                league_data["id"] = league_data["uid"]
            elif "id" not in league_data:
                league_data["id"] = league_base_url

            # Pass along the season_year_filter for downstream use
            if season_year_filter:
                league_data["_season_year_filter"] = season_year_filter
            else:
                league_data["_season_year_filter"] = None  # Ensure key exists

            yield league_data
            logger.info(
                f"Successfully fetched league root for {league_data.get('name', league_base_url)}."
            )
        except Exception as e:
            logger.error(f"Error fetching league root from {league_base_url}: {e}", exc_info=True)
            raise  # Stop pipeline if league root fails

    # --- Seasons Processing Chain ---

    @dlt.transformer(name="season_refs_lister", data_from=league_info_resource)
    def season_refs_lister_transformer(league_doc: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        If a season_year_filter is present, yields a direct $ref to that specific season.
        Otherwise, extracts the 'seasons.$ref' (collection URL) from the league document,
        paginates through it using list_client, and yields individual season $ref objects.
        Each yielded $ref object is augmented with league_id_fk.
        """
        current_season_filter = league_doc.get("_season_year_filter")
        league_id = league_doc.get("id")

        if current_season_filter:
            # Construct the direct URL for the specific season
            # Ensure league_base_url does not end with a slash if season_year_filter
            # starts with one, or vice-versa
            # A more robust way is to parse the seasons_collection_ref and replace parts,
            # but for this API, direct construction from league_base_url is likely fine.
            season_detail_url = f"{league_base_url}/seasons/{current_season_filter}"
            logger.info(
                f"Season filter '{current_season_filter}'. Yielding direct ref: {season_detail_url}"
            )
            yield {"$ref": season_detail_url, "league_id_fk": str(league_id)}
        else:
            seasons_collection_url = league_doc.get("seasons", {}).get("$ref")
            if not seasons_collection_url:
                logger.error(
                    f"League doc for '{league_id}' missing 'seasons.$ref'. Cannot list seasons."
                )
                return

            logger.info(f"Listing all season refs from collection: {seasons_collection_url}")
            try:
                # The seasons_collection_url is absolute, so list_client will use it directly.
                for season_ref_page in list_client.paginate(
                    seasons_collection_url, params={"limit": API_LIMIT}
                ):
                    for (
                        season_ref_item
                    ) in season_ref_page:  # season_ref_page is already the list of items
                        if "$ref" in season_ref_item:
                            season_ref_item_augmented = season_ref_item.copy()
                            season_ref_item_augmented["league_id_fk"] = str(league_id)
                            yield season_ref_item_augmented
                        else:
                            logger.warning(
                                f"Season ref item missing '$ref' key in page from "
                                f"{seasons_collection_url}. Item: {season_ref_item}"
                            )
            except Exception as e:
                logger.error(
                    f"Error listing season refs from {seasons_collection_url}: {e}", exc_info=True
                )

    @dlt.transformer(
        name="seasons",
        data_from=season_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def season_detail_fetcher_transformer(season_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full season details for an individual season $ref object.
        """
        detail_url = season_ref_item.get("$ref")
        league_id_fk = season_ref_item.get("league_id_fk")  # Get FK from lister

        if not detail_url:
            logger.warning(f"Season ref item missing '$ref'. Item: {season_ref_item}")
            return None

        logger.debug(f"Fetching season detail from: {detail_url}")
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            season_detail = response.json()

            api_season_year = season_detail.get("year")
            if api_season_year is not None:
                season_detail["id"] = str(api_season_year)  # Use API 'year' as 'id'
                if league_id_fk:  # Add the foreign key
                    season_detail["league_id_fk"] = str(league_id_fk)
                return season_detail
            else:
                logger.warning(
                    f"Fetched season detail from {detail_url} missing 'year'. "
                    f"Detail: {season_detail}"
                )
                return None
        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching season detail from {detail_url}: "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching season detail from {detail_url}: {e}", exc_info=True
            )
            return None

    # --- Season Types Processing Chain (dependent on season_details) ---

    @dlt.transformer(name="season_type_refs_lister", data_from=season_detail_fetcher_transformer)
    def season_type_refs_lister_transformer(
        season_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """
        Extracts the 'types.$ref' (collection URL for season types) from a season_detail object,
        paginates through it, and yields individual season type $ref objects.
        Each yielded $ref object is augmented with season_id_fk.
        """
        season_id = season_detail.get("id")  # This is the year, e.g., "2024"
        season_types_collection_url = season_detail.get("types", {}).get("$ref")

        if not season_id:
            logger.warning(
                f"Season detail missing 'id'. Skipping season types. Detail: {season_detail}"
            )
            return
        if not season_types_collection_url:
            logger.info(
                f"Season detail for season '{season_id}' missing 'types.$ref'. "
                f"No season types to list."
            )
            return

        logger.info(
            f"Listing season type refs for season '{season_id}' "
            f"from collection: {season_types_collection_url}"
        )
        try:
            for type_ref_page in list_client.paginate(
                season_types_collection_url, params={"limit": API_LIMIT}
            ):
                for type_ref_item in type_ref_page:
                    if "$ref" in type_ref_item:
                        type_ref_item_augmented = type_ref_item.copy()
                        type_ref_item_augmented["season_id_fk"] = str(season_id)
                        yield type_ref_item_augmented
                    else:
                        logger.warning(
                            f"Season type ref item missing '$ref' key in page "
                            f"from {season_types_collection_url}. Item: {type_ref_item}"
                        )
        except Exception as e:
            logger.error(
                f"Error listing season type refs for season '{season_id}' "
                f"from {season_types_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="season_types",
        data_from=season_type_refs_lister_transformer,
        write_disposition="merge",
        primary_key=["id", "season_id_fk"],
    )
    @dlt.defer
    def season_type_detail_fetcher_transformer(type_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full season type details for an individual season type $ref object.
        """
        detail_url = type_ref_item.get("$ref")
        season_id_fk = type_ref_item.get("season_id_fk")

        if not detail_url:
            logger.warning(f"Season type ref item missing '$ref'. Item: {type_ref_item}")
            return None
        if not season_id_fk:  # Should always be present from lister
            logger.warning(f"Season type ref item missing 'season_id_fk'. Item: {type_ref_item}")
            # Potentially skip or try to parse from URL if robustly possible
            return None

        logger.debug(f"Fetching season type detail for season '{season_id_fk}' from: {detail_url}")
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            type_detail = response.json()

            # API 'id' for type is the type's own ID (e.g., "1", "2")
            api_type_id = type_detail.get("id")
            if api_type_id is not None:
                type_detail["id"] = str(api_type_id)
                type_detail["season_id_fk"] = str(season_id_fk)  # Ensure FK is present
                return type_detail
            else:
                logger.warning(
                    f"Fetched season type detail from {detail_url} missing 'id'. "
                    f"Detail: {type_detail}"
                )
                return None
        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching season type detail from {detail_url} "
                f"(season_id_fk: {season_id_fk}): {he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching season type detail from {detail_url} "
                f"(season_id_fk: {season_id_fk}): {e}",
                exc_info=True,
            )
            return None

    # Define other resources and transformers here following the
    # "Lister + Detail Fetcher with @dlt.defer" pattern.

    return (
        league_info_resource,
        season_refs_lister_transformer,
        season_detail_fetcher_transformer,
        season_type_refs_lister_transformer,
        season_type_detail_fetcher_transformer,
        # ... add other listers/fetchers for Weeks, Events, etc.
    )


# --- Main execution for local testing ---
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logger.info("Running dlt POC pipeline (root fetch v2) locally...")

    pipeline = dlt.pipeline(
        pipeline_name="espn_local_test",
        destination="duckdb",
        dataset_name="espn_local",
    )

    source_instance = espn_source(
        # Test with a specific season filter:
        #     season_year_filter="2024"
    )

    load_info = pipeline.run(source_instance)

    logger.info("\n--- Load Info ---")
    logger.info(load_info)
    logger.info("-----------------\n")

    if load_info.has_failed_jobs:
        logger.error("Pipeline run failed or had errors.")
    else:
        logger.info("Pipeline run completed successfully.")
