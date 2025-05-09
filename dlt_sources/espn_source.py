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
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
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

            logger.debug(f"Listing all season refs from collection: {seasons_collection_url}")
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

        logger.debug(
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

    # --- Weeks Processing Chain (dependent on season_type_details) ---

    @dlt.transformer(name="week_refs_lister", data_from=season_type_detail_fetcher_transformer)
    def week_refs_lister_transformer(
        season_type_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """
        Extracts the 'weeks.$ref' (collection URL for weeks) from a season_type_detail object,
        paginates through it, and yields individual week $ref objects.
        Each yielded $ref object is augmented with season_id_fk and type_id_fk.
        """
        season_id_fk = season_type_detail.get("season_id_fk")
        type_id_fk = season_type_detail.get("id")  # This is the season type's ID
        weeks_collection_url = season_type_detail.get("weeks", {}).get("$ref")

        if not season_id_fk:
            logger.warning(
                f"Season type detail missing 'season_id_fk'. Skipping weeks. Detail: {season_type_detail}"
            )
            return
        if not type_id_fk:
            logger.warning(
                f"Season type detail missing 'id' (type_id_fk). Skipping weeks. Detail: {season_type_detail}"
            )
            return
        if not weeks_collection_url:
            logger.info(
                f"Season type detail for type '{type_id_fk}' in season '{season_id_fk}' "
                f"missing 'weeks.$ref'. No weeks to list."
            )
            return

        logger.debug(
            f"Listing week refs for type '{type_id_fk}', season '{season_id_fk}' "
            f"from collection: {weeks_collection_url}"
        )
        try:
            for week_ref_page in list_client.paginate(
                weeks_collection_url, params={"limit": API_LIMIT}
            ):
                for week_ref_item in week_ref_page:
                    if "$ref" in week_ref_item:
                        week_ref_item_augmented = week_ref_item.copy()
                        week_ref_item_augmented["season_id_fk"] = str(season_id_fk)
                        week_ref_item_augmented["type_id_fk"] = str(type_id_fk)
                        yield week_ref_item_augmented
                    else:
                        logger.warning(
                            f"Week ref item missing '$ref' key in page "
                            f"from {weeks_collection_url}. Item: {week_ref_item}"
                        )
        except Exception as e:
            logger.error(
                f"Error listing week refs for type '{type_id_fk}', season '{season_id_fk}' "
                f"from {weeks_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="weeks",
        data_from=week_refs_lister_transformer,
        write_disposition="merge",
        primary_key=["id", "type_id_fk", "season_id_fk"],
    )
    @dlt.defer
    def week_detail_fetcher_transformer(week_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full week details for an individual week $ref object.
        The API 'number' field is used as the primary key 'id'.
        """
        detail_url = week_ref_item.get("$ref")
        season_id_fk = week_ref_item.get("season_id_fk")
        type_id_fk = week_ref_item.get("type_id_fk")

        if not detail_url:
            logger.warning(f"Week ref item missing '$ref'. Item: {week_ref_item}")
            return None
        if not season_id_fk or not type_id_fk:
            logger.warning(
                f"Week ref item missing 'season_id_fk' or 'type_id_fk'. Item: {week_ref_item}"
            )
            return None

        logger.debug(
            f"Fetching week detail for type '{type_id_fk}', season '{season_id_fk}' from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            week_detail = response.json()

            api_week_number = week_detail.get("number")
            if api_week_number is not None:
                week_detail["id"] = str(api_week_number)  # Use API 'number' as 'id'
                week_detail["season_id_fk"] = str(season_id_fk)
                week_detail["type_id_fk"] = str(type_id_fk)
                return week_detail
            else:
                logger.warning(
                    f"Fetched week detail from {detail_url} missing 'number'. Detail: {week_detail}"
                )
                return None
        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching week detail from {detail_url} "
                f"(type_id_fk: {type_id_fk}, season_id_fk: {season_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching week detail from {detail_url} "
                f"(type_id_fk: {type_id_fk}, season_id_fk: {season_id_fk}): {e}",
                exc_info=True,
            )
            return None

    # --- Events (Games) Processing Chain (dependent on weeks) ---

    @dlt.transformer(name="event_refs_lister", data_from=week_detail_fetcher_transformer)
    def event_refs_lister_transformer(
        week_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """
        Constructs the events collection URL from week_detail's foreign keys and league_base_url,
        paginates through it, and yields individual event $ref objects.
        Each yielded $ref object is augmented with season_id_fk, type_id_fk, and week_id_fk.
        """
        season_id_fk = week_detail.get("season_id_fk")
        type_id_fk = week_detail.get("type_id_fk")
        week_id_fk = week_detail.get("id")  # This is the week's ID (number)

        if not all([season_id_fk, type_id_fk, week_id_fk]):
            logger.warning(
                f"Week detail missing one or more FKs (season_id_fk, type_id_fk, week_id_fk). "
                f"Skipping events. Detail: {week_detail}"
            )
            return

        # Construct the events collection URL
        # league_base_url is available from the espn_source function's scope
        events_collection_url = (
            f"{league_base_url}/seasons/{season_id_fk}/types/{type_id_fk}/weeks/{week_id_fk}/events"
        )

        logger.debug(
            f"Listing event refs for week '{week_id_fk}' (type '{type_id_fk}', season '{season_id_fk}') "
            f"from constructed collection URL: {events_collection_url}"
        )
        try:
            for event_ref_page in list_client.paginate(
                events_collection_url, params={"limit": API_LIMIT}
            ):
                for event_ref_item in event_ref_page:
                    if "$ref" in event_ref_item:
                        event_ref_item_augmented = event_ref_item.copy()
                        event_ref_item_augmented["season_id_fk"] = str(season_id_fk)
                        event_ref_item_augmented["type_id_fk"] = str(type_id_fk)
                        event_ref_item_augmented["week_id_fk"] = str(week_id_fk)
                        yield event_ref_item_augmented
                    else:
                        logger.warning(
                            f"Event ref item missing '$ref' key in page "
                            f"from {events_collection_url}. Item: {event_ref_item}"
                        )
        except requests.exceptions.HTTPError as he:
            # Check for 404 specifically, as some weeks might legitimately have no events
            # and the API might return 404 for an empty events collection.
            if he.response.status_code == 404:
                logger.info(
                    f"Received 404 Not Found when listing events for week '{week_id_fk}' "
                    f"(type '{type_id_fk}', season '{season_id_fk}') from {events_collection_url}. "
                    f"Assuming no events for this week. Error: {he.response.text if he.response else str(he)}"
                )
            else:
                logger.error(
                    f"HTTPError listing event refs for week '{week_id_fk}' (type '{type_id_fk}', season '{season_id_fk}') "
                    f"from {events_collection_url}: {he.response.text if he.response else str(he)}",
                    exc_info=True,
                )
        except Exception as e:
            logger.error(
                f"Error listing event refs for week '{week_id_fk}' (type '{type_id_fk}', season '{season_id_fk}') "
                f"from {events_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="events",  # This will be the table name for event details
        data_from=event_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def event_detail_fetcher_transformer(event_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full event (game) details for an individual event $ref object.
        The API 'id' for the event is used as the primary key.
        Propagates season_id_fk, type_id_fk, and week_id_fk.
        """
        detail_url = event_ref_item.get("$ref")
        season_id_fk = event_ref_item.get("season_id_fk")
        type_id_fk = event_ref_item.get("type_id_fk")
        week_id_fk = event_ref_item.get("week_id_fk")

        if not detail_url:
            logger.warning(f"Event ref item missing '$ref'. Item: {event_ref_item}")
            return None
        if not all([season_id_fk, type_id_fk, week_id_fk]):
            logger.warning(
                f"Event ref item missing one or more FKs (season_id_fk, type_id_fk, week_id_fk). "
                f"Item: {event_ref_item}"
            )
            return None

        logger.debug(
            f"Fetching event detail for week '{week_id_fk}' (type '{type_id_fk}', season '{season_id_fk}') "
            f"from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            event_detail = response.json()

            api_event_id = event_detail.get("id")
            if api_event_id is not None:
                event_detail["id"] = str(api_event_id)  # Ensure ID is string
                event_detail["season_id_fk"] = str(season_id_fk)
                event_detail["type_id_fk"] = str(type_id_fk)
                event_detail["week_id_fk"] = str(week_id_fk)
                # The event_detail object itself will be yielded and can be used
                # by downstream transformers to extract sub-resources (competitors, odds, etc.)
                return event_detail
            else:
                logger.warning(
                    f"Fetched event detail from {detail_url} missing 'id'. Detail: {event_detail}"
                )
                return None
        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching event detail from {detail_url} "
                f"(week_id_fk: {week_id_fk}, type_id_fk: {type_id_fk}, season_id_fk: {season_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching event detail from {detail_url} "
                f"(week_id_fk: {week_id_fk}, type_id_fk: {type_id_fk}, season_id_fk: {season_id_fk}): {e}",
                exc_info=True,
            )
            return None

    # --- Event Sub-Resources Processing Chain (dependent on event_detail_fetcher_transformer) ---

    @dlt.transformer(
        name="event_competitors",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=["id", "event_id_fk"],  # 'id' here is the competitor's team id
    )
    def event_competitors_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Extracts competitor details directly from the event_detail.competitions[0].competitors array.
        Each competitor item is augmented with event_id_fk.
        The 'id' field from the competitor data (team_id) and event_id_fk form the primary key.
        """
        event_id_fk = event_detail.get("id")
        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id' (event_id_fk). Cannot extract competitors. Detail: {event_detail}"
            )
            return None

        # Safely access competitions array and then competitors array
        competitions_list = event_detail.get("competitions")
        if (
            not competitions_list
            or not isinstance(competitions_list, list)
            or not competitions_list[0]
        ):
            logger.info(
                f"Event detail for event '{event_id_fk}' has no 'competitions' array or it's empty. "
                f"No competitors to extract."
            )
            return None

        competitors_list = competitions_list[0].get("competitors")
        if not competitors_list or not isinstance(competitors_list, list):
            logger.info(
                f"Event detail for event '{event_id_fk}' competition has no 'competitors' array. "
                f"No competitors to extract."
            )
            return None

        logger.debug(
            f"Extracting competitors for event_id: {event_id_fk}. "
            f"Found {len(competitors_list)} competitors."
        )
        for competitor_item in competitors_list:
            if not isinstance(competitor_item, dict) or "id" not in competitor_item:
                logger.warning(
                    f"Competitor item for event '{event_id_fk}' is malformed or missing 'id'. "
                    f"Item: {competitor_item}"
                )
                continue

            # Create a copy to avoid modifying the original event_detail in case it's reused
            competitor_record = competitor_item.copy()
            competitor_record["event_id_fk"] = str(event_id_fk)

            # Ensure the primary key field 'id' (team_id) is a string
            competitor_record["id"] = str(competitor_item["id"])

            # Pass along all other FKs from the event_detail for potential use by downstream transformers
            # that might process items yielded by this transformer's children.
            # (Though for event_competitors' direct children, usually event_id_fk and team_id are sufficient)
            if "season_id_fk" in event_detail:
                competitor_record["_season_id_fk_from_event"] = event_detail["season_id_fk"]
            if "type_id_fk" in event_detail:
                competitor_record["_type_id_fk_from_event"] = event_detail["type_id_fk"]
            if "week_id_fk" in event_detail:
                competitor_record["_week_id_fk_from_event"] = event_detail["week_id_fk"]

            yield competitor_record

    @dlt.transformer(
        name="event_scores",
        data_from=event_competitors_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk"],
    )
    @dlt.defer
    def event_scores_detail_fetcher_transformer(
        competitor_record: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches the final score for a team in an event using the competitor_record.score.$ref.
        """
        event_id_fk = competitor_record.get("event_id_fk")
        # The 'id' of the competitor_record is the team_id for that event
        team_id_fk = competitor_record.get("id")
        score_ref_url = competitor_record.get("score", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch score. Record: {competitor_record}"
            )
            return None

        if not score_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'score.$ref'. "
                f"No score to fetch."
            )
            return None

        logger.debug(
            f"Fetching event score for event '{event_id_fk}', team '{team_id_fk}' from: {score_ref_url}"
        )
        try:
            response = detail_client.get(score_ref_url)
            response.raise_for_status()
            score_detail = response.json()

            # Augment the score detail with necessary FKs
            score_detail_augmented = score_detail.copy()
            score_detail_augmented["event_id_fk"] = str(event_id_fk)
            score_detail_augmented["team_id_fk"] = str(team_id_fk)

            # The score detail itself might not have a unique 'id' other than the composite FKs relationship.
            # We rely on the composite primary key in dlt for merging.
            return score_detail_augmented

        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching event score from {score_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching event score from {score_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="event_linescores",
        data_from=event_competitors_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "period"],
    )
    @dlt.defer
    def event_linescores_transformer(
        competitor_record: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Fetches the list of linescore items (score per period) for a team in an event
        using the competitor_record.linescores.$ref.
        Yields one record per team per period.
        """
        event_id_fk = competitor_record.get("event_id_fk")
        team_id_fk = competitor_record.get("id")  # competitor's 'id' is team_id for the event
        linescores_ref_url = competitor_record.get("linescores", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch linescores. Record: {competitor_record}"
            )
            return None  # Or yield nothing: yield from []

        if not linescores_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'linescores.$ref'. "
                f"No linescores to fetch."
            )
            return None  # Or yield from []

        logger.debug(
            f"Fetching event linescores for event '{event_id_fk}', team '{team_id_fk}' from: {linescores_ref_url}"
        )
        try:
            response = detail_client.get(linescores_ref_url)
            response.raise_for_status()
            linescore_data = response.json()

            # The response might be a list directly, or a dict with an 'items' key
            linescore_items_list = []
            if isinstance(linescore_data, list):
                linescore_items_list = linescore_data
            elif (
                isinstance(linescore_data, dict)
                and "items" in linescore_data
                and isinstance(linescore_data["items"], list)
            ):
                linescore_items_list = linescore_data["items"]
            else:
                logger.warning(
                    f"Unexpected linescore_data format from {linescores_ref_url} for event '{event_id_fk}', "
                    f"team '{team_id_fk}'. Expected list or dict with 'items' list. Data: {linescore_data}"
                )
                # yield from [] # or return None

            processed_any = False
            for item in linescore_items_list:
                if not isinstance(item, dict) or "period" not in item:
                    logger.warning(
                        f"Linescore item from {linescores_ref_url} for event '{event_id_fk}', "
                        f"team '{team_id_fk}' is malformed or missing 'period'. Item: {item}"
                    )
                    continue

                linescore_item_augmented = item.copy()
                linescore_item_augmented["event_id_fk"] = str(event_id_fk)
                linescore_item_augmented["team_id_fk"] = str(team_id_fk)
                linescore_item_augmented["period"] = str(
                    item["period"]
                )  # Ensure period is string for PK consistency

                yield linescore_item_augmented
                processed_any = True

            if (
                not processed_any and not linescore_items_list
            ):  # Explicitly check if list was empty from API vs parse failure
                logger.debug(
                    f"No linescore items found in the response from {linescores_ref_url} "
                    f"for event '{event_id_fk}', team '{team_id_fk}'. API returned empty list."
                )

        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching event linescores from {linescores_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            # yield from [] # or return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event linescores from {linescores_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
            # yield from [] # or return None
        # If nothing is yielded implicitly, dlt handles it as no data for this input item.

    @dlt.transformer(  # This intermediate resource fetches raw data for team stats and player stat refs
        name="event_team_stats_raw_data",  # Not a final table, but a source for downstream processors
        data_from=event_competitors_transformer,
        # No primary_key or write_disposition needed if its output is purely intermediate
    )
    @dlt.defer
    def event_team_stats_raw_fetcher_transformer(
        competitor_record: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches the raw JSON data for a team's statistics in an event,
        which includes both aggregated team stats and refs to player stats.
        Yields the raw JSON augmented with event_id_fk and team_id_fk.
        """
        event_id_fk = competitor_record.get("event_id_fk")
        team_id_fk = competitor_record.get("id")  # competitor's 'id' is team_id
        stats_ref_url = competitor_record.get("statistics", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch team stats raw data. Record: {competitor_record}"
            )
            return None

        if not stats_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'statistics.$ref'. "
                f"No team stats raw data to fetch."
            )
            return None

        logger.debug(
            f"Fetching event team stats raw data for event '{event_id_fk}', team '{team_id_fk}' from: {stats_ref_url}"
        )
        try:
            response = detail_client.get(stats_ref_url)
            response.raise_for_status()
            raw_stats_data = response.json()

            # Augment the raw data with necessary FKs for downstream processors
            raw_stats_data_augmented = {
                "event_id_fk": str(event_id_fk),
                "team_id_fk": str(team_id_fk),
                "data": raw_stats_data,  # Keep the original fetched data nested
            }
            return raw_stats_data_augmented

        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching event team stats raw data from {stats_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching event team stats raw data from {stats_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="event_team_stats",
        data_from=event_team_stats_raw_fetcher_transformer,  # Takes from the raw data fetcher
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "stat_name"],
    )
    def event_team_stats_processor_transformer(
        augmented_raw_stats_data: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Processes the 'stats' portion of the raw team statistics data
        and yields tidy records for the event_team_stats table.
        """
        event_id_fk = augmented_raw_stats_data.get("event_id_fk")
        team_id_fk = augmented_raw_stats_data.get("team_id_fk")
        raw_data = augmented_raw_stats_data.get("data", {})

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Augmented raw stats data missing 'event_id_fk' or 'team_id_fk'. "
                f"Cannot process team stats. Data: {augmented_raw_stats_data}"
            )
            yield from []
            return

        # Team stats are usually in response.splits[0].stats
        # Need to handle cases where 'splits' or 'stats' may be missing or not in expected structure
        splits = raw_data.get("splits")
        if (
            not splits
            or not isinstance(splits, list)
            or not splits[0]
            or not isinstance(splits[0], dict)
        ):
            logger.debug(
                f"No 'splits' array or invalid format in raw team stats data for event '{event_id_fk}', "
                f"team '{team_id_fk}'. No team stats to process. Raw data: {raw_data}"
            )
            yield from []
            return

        team_stats_list = splits[0].get("stats")
        if not team_stats_list or not isinstance(team_stats_list, list):
            logger.debug(
                f"No 'stats' list in splits[0] for event '{event_id_fk}', team '{team_id_fk}'. "
                f"No team stats to process. Split data: {splits[0]}"
            )
            yield from []
            return

        # Optionally, get category name if available
        # category_name = splits[0].get("category", {}).get("name", "general") # Or derive from raw_data.name

        processed_any = False
        for stat_item in team_stats_list:
            if not isinstance(stat_item, dict) or "name" not in stat_item:
                logger.warning(
                    f"Team stat item for event '{event_id_fk}', team '{team_id_fk}' is malformed or "
                    f"missing 'name'. Item: {stat_item}"
                )
                continue

            stat_name = stat_item.get("name")
            # stat_display_name = stat_item.get("displayName")
            stat_value_str = stat_item.get(
                "displayValue", stat_item.get("value")
            )  # Prefer displayValue if exists

            tidy_stat_record = {
                "event_id_fk": str(event_id_fk),
                "team_id_fk": str(team_id_fk),
                "stat_name": str(stat_name),
                "stat_value": str(stat_value_str) if stat_value_str is not None else None,
                # "category": str(category_name), # If you decide to add category
                # "raw_label": stat_item.get("label") # Optional: for more detail
            }
            yield tidy_stat_record
            processed_any = True

        if not processed_any:
            logger.debug(
                f"Processed zero team stat items for event '{event_id_fk}', team '{team_id_fk}' "
                f"from raw data (list might have been empty or all items malformed)."
            )

    @dlt.transformer(  # This lister also takes from the raw team stats data
        name="event_player_stats_refs_lister",
        data_from=event_team_stats_raw_fetcher_transformer,
        # No primary_key or write_disposition as it yields refs, not a final table
    )
    def event_player_stats_refs_lister_transformer(
        augmented_raw_stats_data: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Processes the 'athletes' portion of the raw team statistics data (which contains
        player stat $refs) and yields augmented reference objects for fetching detailed player stats.
        """
        event_id_fk = augmented_raw_stats_data.get("event_id_fk")
        team_id_fk = augmented_raw_stats_data.get("team_id_fk")
        raw_data = augmented_raw_stats_data.get("data", {})

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Augmented raw stats data missing 'event_id_fk' or 'team_id_fk'. "
                f"Cannot list player stat refs. Data: {augmented_raw_stats_data}"
            )
            yield from []
            return

        # Player stat refs are usually in response.splits[0].athletes
        splits = raw_data.get("splits")
        if (
            not splits
            or not isinstance(splits, list)
            or not splits[0]
            or not isinstance(splits[0], dict)
        ):
            logger.debug(
                f"No 'splits' array or invalid format in raw team stats data for event '{event_id_fk}', "
                f"team '{team_id_fk}'. No player stat refs to list. Raw data: {raw_data}"
            )
            yield from []
            return

        athletes_list_with_refs = splits[0].get("athletes")
        if not athletes_list_with_refs or not isinstance(athletes_list_with_refs, list):
            logger.debug(
                f"No 'athletes' list in splits[0] for event '{event_id_fk}', team '{team_id_fk}'. "
                f"No player stat refs to list. Split data: {splits[0]}"
            )
            yield from []
            return

        processed_any = False
        for athlete_item in athletes_list_with_refs:
            if not isinstance(athlete_item, dict):
                logger.warning(
                    f"Athlete item for event '{event_id_fk}', team '{team_id_fk}' is malformed. Item: {athlete_item}"
                )
                continue

            athlete_id_obj = athlete_item.get("athlete", {})
            player_athlete_id = (
                athlete_id_obj.get("id") if isinstance(athlete_id_obj, dict) else None
            )

            player_stats_ref_obj = athlete_item.get("statistics", {})
            player_stats_ref_url = (
                player_stats_ref_obj.get("$ref") if isinstance(player_stats_ref_obj, dict) else None
            )

            if not player_athlete_id or not player_stats_ref_url:
                logger.warning(
                    f"Athlete item for event '{event_id_fk}', team '{team_id_fk}' missing 'athlete.id' or "
                    f"'statistics.$ref'. Item: {athlete_item}"
                )
                continue

            player_stat_ref_augmented = {
                "player_stats_ref_url": str(player_stats_ref_url),
                "event_id_fk": str(event_id_fk),
                "team_id_fk": str(team_id_fk),
                "athlete_id_fk": str(player_athlete_id),
            }
            yield player_stat_ref_augmented
            processed_any = True

        if not processed_any:
            logger.debug(
                f"Processed zero player stat refs for event '{event_id_fk}', team '{team_id_fk}' from "
                f"raw data (athletes list might have been empty or all items malformed)."
            )

    @dlt.transformer(
        name="event_player_stats",
        data_from=event_player_stats_refs_lister_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "athlete_id_fk", "stat_name"],
    )
    @dlt.defer
    def event_player_stats_detail_fetcher_transformer(
        player_stat_ref_item: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Fetches detailed player statistics for an event using the provided $ref,
        and unnests them into a tidy format (one row per stat).
        """
        detail_url = player_stat_ref_item.get("player_stats_ref_url")
        event_id_fk = player_stat_ref_item.get("event_id_fk")
        team_id_fk = player_stat_ref_item.get("team_id_fk")
        athlete_id_fk = player_stat_ref_item.get("athlete_id_fk")

        if not all([detail_url, event_id_fk, team_id_fk, athlete_id_fk]):
            logger.warning(
                f"Player stat ref item missing one or more required fields "
                f"('player_stats_ref_url', 'event_id_fk', 'team_id_fk', 'athlete_id_fk'). "
                f"Item: {player_stat_ref_item}"
            )
            yield from []  # Return an empty iterable
            return

        logger.debug(
            f"Fetching player stats for event '{event_id_fk}', team '{team_id_fk}', athlete '{athlete_id_fk}' "
            f"from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            player_stats_data = response.json()

            # Player stats often come in a structure like:
            # player_stats_data -> "splits" (list) -> "categories" (list) -> "stats" (list)
            # We need to unnest this.

            splits = player_stats_data.get("splits")
            if (
                not splits
                or not isinstance(splits, list)
                or not splits[0]
                or not isinstance(splits[0], dict)
            ):
                logger.debug(
                    f"No 'splits' array or invalid format in player stats data for event '{event_id_fk}', "
                    f"team '{team_id_fk}', athlete '{athlete_id_fk}'. URL: {detail_url}. Data: {player_stats_data}"
                )
                yield from []
                return

            # Assuming stats are in the first split, which is typical
            categories = splits[0].get("categories")
            if not categories or not isinstance(categories, list):
                logger.debug(
                    f"No 'categories' list in player stats splits[0] for event '{event_id_fk}', "
                    f"team '{team_id_fk}', athlete '{athlete_id_fk}'. URL: {detail_url}. Split data: {splits[0]}"
                )
                yield from []
                return

            processed_any_stat = False
            for category in categories:
                if not isinstance(category, dict):
                    logger.warning(f"Malformed category item: {category}. Skipping.")
                    continue

                # category_name = category.get("name", "unknown_category") # Optional: if needed for PK or context
                stats_list = category.get("stats")
                if not stats_list or not isinstance(stats_list, list):
                    logger.debug(
                        f"No 'stats' list in category '{category.get('name')}' for player stats. "
                        f"Event '{event_id_fk}', team '{team_id_fk}', athlete '{athlete_id_fk}'. Category: {category}"
                    )
                    continue

                for stat_item in stats_list:
                    if not isinstance(stat_item, dict) or "name" not in stat_item:
                        logger.warning(
                            f"Player stat item for event '{event_id_fk}', team '{team_id_fk}', athlete '{athlete_id_fk}' "
                            f"is malformed or missing 'name'. Item: {stat_item}"
                        )
                        continue

                    stat_name = stat_item.get("name")
                    stat_value_str = stat_item.get("displayValue", stat_item.get("value"))

                    tidy_stat_record = {
                        "event_id_fk": str(event_id_fk),
                        "team_id_fk": str(team_id_fk),
                        "athlete_id_fk": str(athlete_id_fk),
                        "stat_name": str(stat_name),
                        "stat_value": str(stat_value_str) if stat_value_str is not None else None,
                        # "category_name": str(category_name), # Uncomment if category is needed
                    }
                    yield tidy_stat_record
                    processed_any_stat = True

            if not processed_any_stat:
                logger.debug(
                    f"Processed zero player stat items for event '{event_id_fk}', team '{team_id_fk}', "
                    f"athlete '{athlete_id_fk}' from {detail_url}. Data might have been empty or malformed."
                )

        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching player stats from {detail_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}, athlete_id_fk: {athlete_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
            # yield from [] # Implicitly returns None, dlt handles this
        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing player stats from {detail_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}, athlete_id_fk: {athlete_id_fk}): {e}",
                exc_info=True,
            )
            # yield from [] # Implicitly returns None

    @dlt.transformer(
        name="event_leaders",
        data_from=event_competitors_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "category_name", "athlete_id_fk"],
    )
    @dlt.defer
    def event_leaders_detail_fetcher_transformer(
        competitor_record: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Fetches and unnests team leader data for various statistical categories for an event.
        Yields one record per player per leading category.
        """
        event_id_fk = competitor_record.get("event_id_fk")
        team_id_fk = competitor_record.get("id")  # competitor's 'id' is team_id for the event
        leaders_ref_url = competitor_record.get("leaders", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch event leaders. Record: {competitor_record}"
            )
            yield from []
            return

        if not leaders_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'leaders.$ref'. "
                f"No event leaders to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event leaders for event '{event_id_fk}', team '{team_id_fk}' from: {leaders_ref_url}"
        )
        try:
            response = detail_client.get(leaders_ref_url)
            response.raise_for_status()
            leaders_data = response.json()  # Expected to be a list of leader categories

            if not isinstance(leaders_data, list):
                logger.warning(
                    f"Unexpected leaders_data format from {leaders_ref_url} for event '{event_id_fk}', "
                    f"team '{team_id_fk}'. Expected list of categories. Data: {leaders_data}"
                )
                yield from []
                return

            processed_any_leader = False
            for category_data in leaders_data:
                if not isinstance(category_data, dict):
                    logger.warning(f"Malformed leader category data: {category_data}. Skipping.")
                    continue

                category_name = category_data.get(
                    "displayName", category_data.get("name", "unknown_category")
                )

                leader_entries = category_data.get("leaders")
                if not leader_entries or not isinstance(leader_entries, list):
                    logger.debug(
                        f"No 'leaders' list in category '{category_name}' for event '{event_id_fk}', "
                        f"team '{team_id_fk}'. Category data: {category_data}"
                    )
                    continue

                for leader_entry in leader_entries:
                    if not isinstance(leader_entry, dict):
                        logger.warning(f"Malformed leader entry: {leader_entry}. Skipping.")
                        continue

                    athlete_obj = leader_entry.get("athlete", {})
                    athlete_id_fk = athlete_obj.get("id") if isinstance(athlete_obj, dict) else None

                    if not athlete_id_fk:
                        logger.warning(
                            f"Leader entry for event '{event_id_fk}', team '{team_id_fk}', category '{category_name}' "
                            f"missing 'athlete.id'. Entry: {leader_entry}"
                        )
                        continue

                    # Extract common leader stats. The exact fields might vary slightly.
                    value = leader_entry.get("value")
                    display_value = leader_entry.get("displayValue")
                    # rank = leader_entry.get("rank") # If rank is available and desired

                    leader_record = {
                        "event_id_fk": str(event_id_fk),
                        "team_id_fk": str(team_id_fk),
                        "category_name": str(category_name),
                        "athlete_id_fk": str(athlete_id_fk),
                        "value": float(value)
                        if value is not None
                        else None,  # Attempt to cast to float
                        "display_value": str(display_value) if display_value is not None else None,
                        # "rank": int(rank) if rank is not None else None, # If rank is used
                    }
                    yield leader_record
                    processed_any_leader = True

            if not processed_any_leader:
                logger.debug(
                    f"Processed zero leader entries for event '{event_id_fk}', team '{team_id_fk}' from {leaders_ref_url}. "
                    f"Data might have been empty or all items malformed."
                )

        except requests.exceptions.HTTPError as he:
            logger.error(
                f"HTTPError fetching event leaders from {leaders_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): "
                f"{he.response.text if he.response else str(he)}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event leaders from {leaders_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
        # If nothing yielded, dlt handles it.

    # Define other resources and transformers here following the
    # "Lister + Detail Fetcher with @dlt.defer" pattern.

    return (
        league_info_resource,
        season_refs_lister_transformer,
        season_detail_fetcher_transformer,
        season_type_refs_lister_transformer,
        season_type_detail_fetcher_transformer,
        week_refs_lister_transformer,
        week_detail_fetcher_transformer,
        event_refs_lister_transformer,
        event_detail_fetcher_transformer,
        event_competitors_transformer,
        event_scores_detail_fetcher_transformer,
        event_linescores_transformer,
        event_team_stats_raw_fetcher_transformer,
        event_team_stats_processor_transformer,
        event_player_stats_refs_lister_transformer,
        event_player_stats_detail_fetcher_transformer,
        event_leaders_detail_fetcher_transformer,  # New fetcher for event leaders
        # ... add other listers/fetchers for Event sub-resources, etc.
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
        season_year_filter="2021"
    )

    load_info = pipeline.run(source_instance)

    logger.info("\n--- Load Info ---")
    logger.info(load_info)
    logger.info("-----------------\n")

    if load_info.has_failed_jobs:
        logger.error("Pipeline run failed or had errors.")
    else:
        logger.info("Pipeline run completed successfully.")
