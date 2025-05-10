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
        except Exception as e:
            logger.error(f"Error during API operation for {detail_url}: {e}", exc_info=True)
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
        except Exception as e:
            logger.error(
                f"Error during API operation for {detail_url} (season_id_fk: {season_id_fk}): {e}",
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
        except Exception as e:
            logger.error(
                f"Error during API operation for {detail_url} (type_id_fk: {type_id_fk}, season_id_fk: {season_id_fk}): {e}",
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

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event leaders from {leaders_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
        # If nothing yielded, dlt handles it.

    @dlt.transformer(
        name="event_roster",
        data_from=event_competitors_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "athlete_id_fk"],
    )
    @dlt.defer
    def event_roster_detail_fetcher_transformer(
        competitor_record: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Fetches the roster of players for a team in a specific game using the competitor_record.roster.$ref.
        Yields one record per player on the game roster.
        """
        event_id_fk = competitor_record.get("event_id_fk")
        team_id_fk = competitor_record.get("id")  # competitor's 'id' is team_id for the event
        roster_ref_url = competitor_record.get("roster", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch event roster. Record: {competitor_record}"
            )
            yield from []
            return

        if not roster_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'roster.$ref'. "
                f"No event roster to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event roster for event '{event_id_fk}', team '{team_id_fk}' from: {roster_ref_url}"
        )
        try:
            response = detail_client.get(roster_ref_url)
            response.raise_for_status()
            roster_data_response = response.json()

            # Rosters are often a list of entries, sometimes nested under 'entries' or 'items'
            roster_entries = []
            if isinstance(roster_data_response, list):
                roster_entries = roster_data_response
            elif isinstance(roster_data_response, dict):
                if "entries" in roster_data_response and isinstance(
                    roster_data_response["entries"], list
                ):
                    roster_entries = roster_data_response["entries"]
                elif "items" in roster_data_response and isinstance(
                    roster_data_response["items"], list
                ):  # Another common key
                    roster_entries = roster_data_response["items"]
                # Add other potential keys if discovered
                else:
                    logger.warning(
                        f"Roster data from {roster_ref_url} for event '{event_id_fk}', team '{team_id_fk}' "
                        f"is a dict but does not contain a recognized list key ('entries', 'items'). Data: {roster_data_response}"
                    )
            else:
                logger.warning(
                    f"Unexpected roster_data_response format from {roster_ref_url} for event '{event_id_fk}', "
                    f"team '{team_id_fk}'. Expected list or dict. Data: {roster_data_response}"
                )
                yield from []
                return

            processed_any_player = False
            for player_entry in roster_entries:
                if not isinstance(player_entry, dict):
                    logger.warning(f"Malformed player entry in roster: {player_entry}. Skipping.")
                    continue

                athlete_obj = player_entry.get("athlete", {})
                # The athlete ID is usually under athlete.$ref or athlete.id within the roster entry
                athlete_id_fk = None
                if isinstance(athlete_obj, dict):
                    athlete_id_fk = athlete_obj.get("id")
                    if (
                        not athlete_id_fk and "$ref" in athlete_obj
                    ):  # Fallback to parsing from athlete ref if ID not direct
                        try:
                            athlete_id_fk = athlete_obj["$ref"].split("/")[-1].split("?")[0]
                        except Exception:
                            logger.warning(
                                f"Could not parse athlete ID from $ref: {athlete_obj['$ref']}"
                            )

                if not athlete_id_fk:
                    logger.warning(
                        f"Player entry for event '{event_id_fk}', team '{team_id_fk}' missing 'athlete.id' or valid 'athlete.$ref'. "
                        f"Entry: {player_entry}"
                    )
                    continue

                roster_player_record = (
                    player_entry.copy()
                )  # Keep all original fields from the player entry
                roster_player_record["event_id_fk"] = str(event_id_fk)
                roster_player_record["team_id_fk"] = str(team_id_fk)
                roster_player_record["athlete_id_fk"] = str(athlete_id_fk)

                # Remove the original athlete object if we've extracted the ID, to avoid redundant nested dict.
                # Or, dlt can flatten it, but explicit removal can be cleaner.
                # if "athlete" in roster_player_record:
                #     del roster_player_record["athlete"]

                yield roster_player_record
                processed_any_player = True

            if not processed_any_player and not roster_entries:
                logger.debug(
                    f"No player entries found in the roster response from {roster_ref_url} "
                    f"for event '{event_id_fk}', team '{team_id_fk}'. API returned empty list or no parsable entries."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event roster from {roster_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
        # If nothing yielded, dlt handles it.

    @dlt.transformer(
        name="event_pregame_records",
        data_from=event_competitors_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "record_type", "stat_name"],
    )
    @dlt.defer
    def event_pregame_records_transformer(
        competitor_record: dict[str, Any],
    ) -> Iterable[TDataItem] | None:
        """
        Fetches and unnests pre-game team records (overall, home, away, etc.) for an event.
        Yields one record per statistic per record type (e.g., overall wins, overall losses).
        """
        event_id_fk = competitor_record.get("event_id_fk")
        team_id_fk = competitor_record.get("id")  # competitor's 'id' is team_id for the event
        records_ref_url = competitor_record.get("records", {}).get("$ref")

        if not all([event_id_fk, team_id_fk]):
            logger.warning(
                f"Competitor record missing 'event_id_fk' or 'id' (team_id_fk). "
                f"Cannot fetch pre-game records. Record: {competitor_record}"
            )
            yield from []
            return

        if not records_ref_url:
            logger.info(
                f"Competitor record for event '{event_id_fk}', team '{team_id_fk}' missing 'records.$ref'. "
                f"No pre-game records to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching pre-game records for event '{event_id_fk}', team '{team_id_fk}' from: {records_ref_url}"
        )
        try:
            response = detail_client.get(records_ref_url)
            response.raise_for_status()
            records_summary_list = (
                response.json()
            )  # Expected to be a list of record summary objects

            if not isinstance(records_summary_list, list):
                # Sometimes the API might return a dict with 'items' or 'entries'
                if isinstance(records_summary_list, dict):
                    if "items" in records_summary_list and isinstance(
                        records_summary_list.get("items"), list
                    ):
                        records_summary_list = records_summary_list["items"]
                    elif "entries" in records_summary_list and isinstance(
                        records_summary_list.get("entries"), list
                    ):
                        records_summary_list = records_summary_list["entries"]
                    else:
                        logger.warning(
                            f"Unexpected pre-game records data format from {records_ref_url} for event '{event_id_fk}', "
                            f"team '{team_id_fk}'. Expected list or dict with 'items'/'entries'. Data: {records_summary_list}"
                        )
                        yield from []
                        return
                else:
                    logger.warning(
                        f"Unexpected pre-game records data format from {records_ref_url} for event '{event_id_fk}', "
                        f"team '{team_id_fk}'. Expected list. Data: {records_summary_list}"
                    )
                    yield from []
                    return

            processed_any_record_stat = False
            for record_summary_item in records_summary_list:
                if not isinstance(record_summary_item, dict):
                    logger.warning(
                        f"Malformed record summary item: {record_summary_item}. Skipping."
                    )
                    continue

                record_type = record_summary_item.get(
                    "type", record_summary_item.get("name", "unknown_type")
                )
                record_summary_display = record_summary_item.get(
                    "summary", record_summary_item.get("displayValue")
                )

                stats_list = record_summary_item.get("stats")
                if not stats_list or not isinstance(stats_list, list):
                    logger.debug(
                        f"No 'stats' list in record summary for type '{record_type}', event '{event_id_fk}', "
                        f"team '{team_id_fk}'. Summary item: {record_summary_item}"
                    )
                    continue

                for stat_detail in stats_list:
                    if not isinstance(stat_detail, dict) or "name" not in stat_detail:
                        logger.warning(
                            f"Malformed stat detail in pre-game records: {stat_detail}. "
                            f"Type '{record_type}', event '{event_id_fk}', team '{team_id_fk}'. Skipping."
                        )
                        continue

                    stat_name = stat_detail.get("name")
                    # Value can be integer (e.g. wins) or float (e.g. win %)
                    stat_value = stat_detail.get("value", stat_detail.get("displayValue"))

                    record_stat_entry = {
                        "event_id_fk": str(event_id_fk),
                        "team_id_fk": str(team_id_fk),
                        "record_type": str(record_type),
                        "stat_name": str(stat_name),
                        "stat_value": str(stat_value)
                        if stat_value is not None
                        else None,  # Store as string for consistency
                        "record_summary_display": str(record_summary_display)
                        if record_summary_display is not None
                        else None,
                        # Optional: include original summary values if needed
                        # "original_summary": record_summary_item.get("summary"),
                        # "original_type_description": record_summary_item.get("description"),
                    }
                    yield record_stat_entry
                    processed_any_record_stat = True

            if not processed_any_record_stat and not records_summary_list:
                logger.debug(
                    f"No pre-game record stats found or processed for event '{event_id_fk}', team '{team_id_fk}' from {records_ref_url}. "
                    f"API might have returned empty list or all items were malformed."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing pre-game records from {records_ref_url} "
                f"(event_id_fk: {event_id_fk}, team_id_fk: {team_id_fk}): {e}",
                exc_info=True,
            )
        # If nothing yielded, dlt handles it.

    @dlt.transformer(
        name="event_status",
        data_from=event_detail_fetcher_transformer,  # Takes directly from event_detail
        write_disposition="merge",
        primary_key="event_id_fk",
    )
    @dlt.defer
    def event_status_detail_fetcher_transformer(event_detail: dict[str, Any]) -> TDataItem | None:
        """
        Fetches the current status for a game using event_detail.competitions[0].status.$ref.
        """
        event_id_fk = event_detail.get("id")
        status_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            status_ref_url = competitions[0].get("status", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch status. Detail: {event_detail}"
            )
            return None

        if not status_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].status.$ref'. No status to fetch."
            )
            return None

        logger.debug(f"Fetching event status for event '{event_id_fk}' from: {status_ref_url}")
        try:
            response = detail_client.get(status_ref_url)
            response.raise_for_status()
            status_data = response.json()

            status_data_augmented = status_data.copy()
            status_data_augmented["event_id_fk"] = str(event_id_fk)
            return status_data_augmented

        except Exception as e:
            logger.error(
                f"Unexpected error fetching event status from {status_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="event_situation",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key="event_id_fk",
    )
    @dlt.defer
    def event_situation_detail_fetcher_transformer(
        event_detail: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches live game situation details using event_detail.competitions[0].situation.$ref.
        """
        event_id_fk = event_detail.get("id")
        situation_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            situation_ref_url = competitions[0].get("situation", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch situation. Detail: {event_detail}"
            )
            return None

        if not situation_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].situation.$ref'. No situation to fetch."
            )
            return None

        logger.debug(
            f"Fetching event situation for event '{event_id_fk}' from: {situation_ref_url}"
        )
        try:
            response = detail_client.get(situation_ref_url)
            response.raise_for_status()
            situation_data = response.json()

            situation_data_augmented = situation_data.copy()
            situation_data_augmented["event_id_fk"] = str(event_id_fk)
            return situation_data_augmented

        except Exception as e:
            logger.error(
                f"Unexpected error fetching event situation from {situation_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="event_predictor",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key="event_id_fk",
    )
    @dlt.defer
    def event_predictor_detail_fetcher_transformer(
        event_detail: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches ESPN's win probability and predicted score using event_detail.competitions[0].predictor.$ref.
        """
        event_id_fk = event_detail.get("id")
        predictor_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            predictor_ref_url = competitions[0].get("predictor", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch predictor. Detail: {event_detail}"
            )
            return None

        if not predictor_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].predictor.$ref'. No predictor data to fetch."
            )
            return None

        logger.debug(
            f"Fetching event predictor data for event '{event_id_fk}' from: {predictor_ref_url}"
        )
        try:
            response = detail_client.get(predictor_ref_url)
            response.raise_for_status()
            predictor_data = response.json()

            predictor_data_augmented = predictor_data.copy()
            predictor_data_augmented["event_id_fk"] = str(event_id_fk)
            return predictor_data_augmented

        except Exception as e:
            logger.error(
                f"Unexpected error fetching event predictor_data from {predictor_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="event_odds",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "provider_id_fk"],
    )
    @dlt.defer
    def event_odds_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches betting odds for a game from event_detail.competitions[0].odds.$ref.
        Yields one record per odds provider.
        """
        event_id_fk = event_detail.get("id")
        odds_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            odds_ref_url = competitions[0].get("odds", {}).get("$ref")

        if not event_id_fk:
            logger.warning(f"Event detail missing 'id'. Cannot fetch odds. Detail: {event_detail}")
            yield from []
            return

        if not odds_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].odds.$ref'. No odds to fetch."
            )
            yield from []
            return

        logger.debug(f"Fetching event odds for event '{event_id_fk}' from: {odds_ref_url}")
        try:
            response = detail_client.get(odds_ref_url)
            response.raise_for_status()
            odds_data_list = response.json()

            if not isinstance(odds_data_list, list):
                # Check for common wrapper keys if not a direct list
                if isinstance(odds_data_list, dict):
                    if "items" in odds_data_list and isinstance(odds_data_list.get("items"), list):
                        odds_data_list = odds_data_list["items"]
                    elif "providers" in odds_data_list and isinstance(
                        odds_data_list.get("providers"), list
                    ):  # another possible key
                        odds_data_list = odds_data_list["providers"]
                    else:
                        logger.warning(
                            f"Unexpected odds data format from {odds_ref_url} for event '{event_id_fk}'. "
                            f"Expected list or dict with 'items'/'providers'. Data: {odds_data_list}"
                        )
                        yield from []
                        return
                else:
                    logger.warning(
                        f"Unexpected odds data format from {odds_ref_url} for event '{event_id_fk}'. Expected list. Data: {odds_data_list}"
                    )
                    yield from []
                    return

            processed_any = False
            for odds_item in odds_data_list:
                if not isinstance(odds_item, dict):
                    logger.warning(f"Malformed odds item: {odds_item}. Skipping.")
                    continue

                provider_obj = odds_item.get("provider", {})
                provider_id_fk = provider_obj.get("id") if isinstance(provider_obj, dict) else None

                if not provider_id_fk:
                    logger.warning(
                        f"Odds item for event '{event_id_fk}' missing 'provider.id'. Item: {odds_item}"
                    )
                    continue

                odds_record = odds_item.copy()
                odds_record["event_id_fk"] = str(event_id_fk)
                odds_record["provider_id_fk"] = str(provider_id_fk)

                # Potential TODO: Unnest 'details', 'overUnder', etc., or ensure dlt handles them appropriately.
                # For Bronze, keeping them as is, or as JSON strings if too nested, is acceptable.
                yield odds_record
                processed_any = True

            if not processed_any and not odds_data_list:
                logger.debug(f"No odds items found for event '{event_id_fk}' from {odds_ref_url}.")

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event odds from {odds_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="event_broadcasts",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=[
            "event_id_fk",
            "media_id_fk",
            "type",
        ],  # Using 'type' as part of PK for market/lang type
    )
    @dlt.defer
    def event_broadcasts_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches broadcast information for a game from event_detail.competitions[0].broadcasts.$ref.
        Yields one record per broadcast entry.
        """
        event_id_fk = event_detail.get("id")
        broadcasts_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            broadcasts_ref_url = competitions[0].get("broadcasts", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch broadcasts. Detail: {event_detail}"
            )
            yield from []
            return

        if not broadcasts_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].broadcasts.$ref'. No broadcasts to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event broadcasts for event '{event_id_fk}' from: {broadcasts_ref_url}"
        )
        try:
            response = detail_client.get(broadcasts_ref_url)
            response.raise_for_status()
            broadcast_data_list = response.json()

            if not isinstance(broadcast_data_list, list):
                # Check for common wrapper keys if not a direct list
                if (
                    isinstance(broadcast_data_list, dict)
                    and "items" in broadcast_data_list
                    and isinstance(broadcast_data_list.get("items"), list)
                ):
                    broadcast_data_list = broadcast_data_list["items"]
                else:
                    logger.warning(
                        f"Unexpected broadcast data format from {broadcasts_ref_url} for event '{event_id_fk}'. "
                        f"Expected list. Data: {broadcast_data_list}"
                    )
                    yield from []
                    return

            processed_any = False
            for broadcast_item in broadcast_data_list:
                if not isinstance(broadcast_item, dict):
                    logger.warning(f"Malformed broadcast item: {broadcast_item}. Skipping.")
                    continue

                media_obj = broadcast_item.get("media", {})
                media_id_fk = media_obj.get("id") if isinstance(media_obj, dict) else None

                # 'type' could be 'market.type' or 'type' or 'lang' - need to normalize for PK
                broadcast_type = broadcast_item.get("type", "unknown")  # Default type
                if isinstance(broadcast_item.get("market"), dict):  # More specific market type
                    broadcast_type = broadcast_item["market"].get("type", broadcast_type)

                if not media_id_fk:
                    logger.warning(
                        f"Broadcast item for event '{event_id_fk}' missing 'media.id'. Item: {broadcast_item}"
                    )
                    continue

                broadcast_record = broadcast_item.copy()
                broadcast_record["event_id_fk"] = str(event_id_fk)
                broadcast_record["media_id_fk"] = str(media_id_fk)
                broadcast_record["type"] = str(broadcast_type)  # Ensure type is string for PK

                yield broadcast_record
                processed_any = True

            if not processed_any and not broadcast_data_list:
                logger.debug(
                    f"No broadcast items found for event '{event_id_fk}' from {broadcasts_ref_url}."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event broadcasts from {broadcasts_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="event_probabilities",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=[
            "event_id_fk",
            "play_id",
        ],  # Using play_id as suggested, ensure it stringifies playId from API
    )
    @dlt.defer
    def event_probabilities_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches time-series win probability data from event_detail.competitions[0].probabilities.$ref.
        Yields one record per probability entry.
        """
        event_id_fk = event_detail.get("id")
        probabilities_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            probabilities_ref_url = competitions[0].get("probabilities", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch probabilities. Detail: {event_detail}"
            )
            yield from []
            return

        if not probabilities_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].probabilities.$ref'. No probabilities to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event probabilities for event '{event_id_fk}' from: {probabilities_ref_url}"
        )
        try:
            response = detail_client.get(probabilities_ref_url)
            response.raise_for_status()
            probabilities_data_list = response.json()

            if not isinstance(probabilities_data_list, list):
                # Check for common wrapper keys if not a direct list
                if (
                    isinstance(probabilities_data_list, dict)
                    and "items" in probabilities_data_list
                    and isinstance(probabilities_data_list.get("items"), list)
                ):
                    probabilities_data_list = probabilities_data_list["items"]
                else:
                    logger.warning(
                        f"Unexpected probabilities data format from {probabilities_ref_url} for event '{event_id_fk}'. "
                        f"Expected list. Data: {probabilities_data_list}"
                    )
                    yield from []
                    return

            processed_any = False
            for prob_item in probabilities_data_list:
                if (
                    not isinstance(prob_item, dict) or "playId" not in prob_item
                ):  # playId is often the key element
                    logger.warning(
                        f"Malformed probability item or missing 'playId': {prob_item}. Skipping."
                    )
                    continue

                prob_record = prob_item.copy()
                prob_record["event_id_fk"] = str(event_id_fk)
                # Ensure the playId used for PK is a string
                prob_record["play_id"] = str(
                    prob_item["playId"]
                )  # Rename for dlt schema, use as PK part

                # Remove original playId if renamed, to avoid confusion, or let dlt handle it.
                # if "playId" in prob_record and "play_id" in prob_record:
                #    del prob_record["playId"]

                yield prob_record
                processed_any = True

            if not processed_any and not probabilities_data_list:
                logger.debug(
                    f"No probability items found for event '{event_id_fk}' from {probabilities_ref_url}."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event probabilities from {probabilities_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="event_powerindex_stats",  # Tidy format
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "team_id_fk", "stat_name"],
    )
    @dlt.defer
    def event_powerindex_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches team power index ratings (BPI/FPI) for the game from event_detail.competitions[0].powerindex.$ref.
        Yields tidy stats (one row per stat per team).
        """
        event_id_fk = event_detail.get("id")
        powerindex_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            powerindex_ref_url = competitions[0].get("powerindex", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch power index. Detail: {event_detail}"
            )
            yield from []
            return

        if not powerindex_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].powerindex.$ref'. No power index to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event power index for event '{event_id_fk}' from: {powerindex_ref_url}"
        )
        try:
            response = detail_client.get(powerindex_ref_url)
            response.raise_for_status()
            powerindex_data_list = response.json()  # Usually a list of 2 items (one per team)

            if not isinstance(powerindex_data_list, list):
                logger.warning(
                    f"Unexpected power index data format from {powerindex_ref_url} for event '{event_id_fk}'. "
                    f"Expected list. Data: {powerindex_data_list}"
                )
                yield from []
                return

            processed_any_stat = False
            for team_pi_data in powerindex_data_list:
                if not isinstance(team_pi_data, dict):
                    logger.warning(f"Malformed team power index data: {team_pi_data}. Skipping.")
                    continue

                team_obj = team_pi_data.get("team", {})
                team_id_fk = team_obj.get("id") if isinstance(team_obj, dict) else None

                if not team_id_fk:
                    logger.warning(
                        f"Team power index data for event '{event_id_fk}' missing 'team.id'. Data: {team_pi_data}"
                    )
                    continue

                stats_list = team_pi_data.get("stats")
                if not stats_list or not isinstance(stats_list, list):
                    logger.debug(
                        f"No 'stats' list in power index data for team '{team_id_fk}', event '{event_id_fk}'. Item: {team_pi_data}"
                    )
                    continue

                for stat_item in stats_list:
                    if not isinstance(stat_item, dict) or "name" not in stat_item:
                        logger.warning(f"Malformed power index stat item: {stat_item}. Skipping.")
                        continue

                    stat_name = stat_item.get("name")
                    stat_value = stat_item.get("value", stat_item.get("displayValue"))

                    pi_stat_record = {
                        "event_id_fk": str(event_id_fk),
                        "team_id_fk": str(team_id_fk),
                        "stat_name": str(stat_name),
                        "stat_value": str(stat_value) if stat_value is not None else None,
                    }
                    yield pi_stat_record
                    processed_any_stat = True

            if not processed_any_stat and not powerindex_data_list:
                logger.debug(
                    f"No power index stats found for event '{event_id_fk}' "
                    f"from {powerindex_ref_url}."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event power index "
                f"from {powerindex_ref_url} (event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="event_officials",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "official_id"],
    )
    @dlt.defer
    def event_officials_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches officials assigned to the game from event_detail.competitions[0].officials.$ref.
        Yields one record per official.
        """
        event_id_fk = event_detail.get("id")
        officials_ref_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            officials_ref_url = competitions[0].get("officials", {}).get("$ref")

        if not event_id_fk:
            logger.warning(
                f"Event detail missing 'id'. Cannot fetch officials. Detail: {event_detail}"
            )
            yield from []
            return

        if not officials_ref_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].officials.$ref'. "
                f"No officials to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event officials for event '{event_id_fk}' from: {officials_ref_url}"
        )
        try:
            response = detail_client.get(officials_ref_url)
            response.raise_for_status()
            officials_data_list = response.json()

            if not isinstance(officials_data_list, list):
                if (
                    isinstance(officials_data_list, dict)
                    and "items" in officials_data_list
                    and isinstance(officials_data_list.get("items"), list)
                ):
                    officials_data_list = officials_data_list["items"]
                else:
                    logger.warning(
                        f"Unexpected officials data format from {officials_ref_url} "
                        f"for event '{event_id_fk}'. "
                        f"Expected list. Data: {officials_data_list}"
                    )
                    yield from []
                    return

            processed_any = False
            for official_item in officials_data_list:
                if not isinstance(official_item, dict):
                    logger.warning(f"Malformed official item: {official_item}. Skipping.")
                    continue

                # Official ID might be directly in 'id' or nested under 'official.id'
                official_id = official_item.get("id")
                if not official_id:
                    official_obj = official_item.get("official", {})
                    if isinstance(official_obj, dict):
                        official_id = official_obj.get("id")

                if not official_id:
                    logger.warning(
                        f"Official item for event '{event_id_fk}' missing 'id' or 'official.id'. "
                        f"Item: {official_item}"
                    )
                    continue

                official_record = official_item.copy()
                official_record["event_id_fk"] = str(event_id_fk)
                official_record["official_id"] = str(
                    official_id
                )  # Use extracted/normalized ID for PK

                # If original 'id' was at top level and we used it, ensure no conflict
                # if 'official_id' is the PK column.
                # Or, structure it carefully. For now, this should be okay
                # if 'official_id' is the PK column.

                yield official_record
                processed_any = True

            if not processed_any and not officials_data_list:
                logger.debug(
                    f"No official items found for event '{event_id_fk}' from {officials_ref_url}."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event officials from {officials_ref_url} "
                f"(event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="event_plays",
        data_from=event_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=["event_id_fk", "id"],
    )
    @dlt.defer
    def event_plays_lister_transformer(event_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Fetches paginated play-by-play data for an event from
        event_detail.competitions[0].plays.$ref.
        Uses list_client to handle pagination.
        Yields one record per play.
        """
        event_id_fk = event_detail.get("id")
        plays_collection_url = None
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            plays_collection_url = competitions[0].get("plays", {}).get("$ref")

        if not event_id_fk:
            logger.warning(f"Event detail missing 'id'. Cannot fetch plays. Detail: {event_detail}")
            yield from []
            return

        if not plays_collection_url:
            logger.info(
                f"Event detail for '{event_id_fk}' missing 'competitions[0].plays.$ref'. "
                f"No plays to fetch."
            )
            yield from []
            return

        logger.debug(
            f"Fetching event plays for event '{event_id_fk}' from paginated URL: "
            f"{plays_collection_url}"
        )
        try:
            processed_any_play = False
            # list_client.paginate will handle iterating through all pages
            for play_page in list_client.paginate(
                plays_collection_url, params={"limit": API_LIMIT}
            ):
                # play_page is already the list of items due to data_selector="items" in list_client
                for play_item in play_page:
                    if not isinstance(play_item, dict) or "id" not in play_item:
                        logger.warning(
                            f"Malformed play item or missing 'id' for event '{event_id_fk}'. "
                            f"Item: {play_item}. Skipping."
                        )
                        continue

                    play_record = play_item.copy()
                    play_record["event_id_fk"] = str(event_id_fk)
                    play_record["id"] = str(
                        play_item["id"]
                    )  # Ensure play's own ID is string for PK

                    # Complex fields like 'participants' will be handled by dlt
                    # (e.g., as JSON strings or nested tables if max_nesting was > 0)
                    # For Bronze layer with max_table_nesting=0, they'll likely be JSON strings.
                    yield play_record
                    processed_any_play = True

            if (
                not processed_any_play
            ):  # This check might be redundant if paginate yields nothing on empty list
                logger.debug(
                    f"No play-by-play items found or processed for event '{event_id_fk}' "
                    f"from {plays_collection_url}. "
                    f"API might have returned empty list or all items were malformed."
                )

        except Exception as e:
            logger.error(
                f"Unexpected error fetching/processing event plays from {plays_collection_url} "
                f"(event_id_fk: {event_id_fk}): {e}",
                exc_info=True,
            )
        # If nothing yielded, dlt handles it.

    # --- Master / Dimension Tables ---

    @dlt.transformer(name="team_refs_lister", data_from=season_detail_fetcher_transformer)
    def team_refs_lister_transformer(season_detail: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        Extracts the 'teams.$ref' (collection URL for teams) from a season_detail object,
        paginates through it, and yields individual team $ref objects, augmented with season_id_fk.
        """
        season_id = season_detail.get("id")  # This is the season year, e.g., "2024"
        teams_collection_url = season_detail.get("teams", {}).get("$ref")

        if not season_id:
            logger.warning(
                f"Season detail missing 'id'. Skipping team refs listing. Detail: {season_detail}"
            )
            return  # yield from []

        if not teams_collection_url:
            logger.info(
                f"Season detail for season '{season_id}' missing 'teams.$ref'. No teams to list for this season."
            )
            return  # yield from []

        logger.debug(
            f"Listing team refs for season '{season_id}' from collection: {teams_collection_url}"
        )
        try:
            for team_ref_page in list_client.paginate(
                teams_collection_url, params={"limit": API_LIMIT}
            ):
                for team_ref_item in team_ref_page:
                    if "$ref" in team_ref_item:
                        team_ref_item_augmented = team_ref_item.copy()
                        team_ref_item_augmented["season_id_fk"] = str(season_id)
                        yield team_ref_item_augmented
                    else:
                        logger.warning(
                            f"Team ref item missing '$ref' key in page "
                            f"from {teams_collection_url} for season '{season_id}'. Item: {team_ref_item}"
                        )

        except Exception as e:
            logger.error(
                f"Error listing team refs for season '{season_id}' from {teams_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="teams",  # This will be the table name for team details
        data_from=team_refs_lister_transformer,
        write_disposition="merge",
        primary_key=["id", "season_id_fk"],  # Team data is specific to a season in this context
    )
    @dlt.defer
    def team_detail_fetcher_transformer(team_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full team details for an individual team $ref object, augmenting with season_id_fk.
        """
        detail_url = team_ref_item.get("$ref")
        season_id_fk = team_ref_item.get("season_id_fk")

        if not detail_url:
            logger.warning(f"Team ref item missing '$ref'. Item: {team_ref_item}")
            return None
        if not season_id_fk:  # Should always be present from lister
            logger.warning(f"Team ref item missing 'season_id_fk'. Item: {team_ref_item}")
            # Could try to parse from URL, but safer to expect it from lister
            return None

        logger.debug(f"Fetching team detail for season '{season_id_fk}' from: {detail_url}")
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            team_detail = response.json()

            api_team_id = team_detail.get("id")
            if api_team_id is not None:
                team_detail["id"] = str(api_team_id)  # Ensure team's own ID is string
                team_detail["season_id_fk"] = str(
                    season_id_fk
                )  # Add/ensure FK is present and string

                # Potential: If team detail contains $refs to venue, coach, etc.,
                # those refs could be yielded here for separate "master detail fetch_transformerners"
                # For now, just yielding the team_detail.

                return team_detail
            else:
                logger.warning(
                    f"Fetched team detail from {detail_url} (season '{season_id_fk}') "
                    f"missing 'id'. Detail: {team_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching team detail from {detail_url} "
                f"(season_id_fk: {season_id_fk}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="athlete_refs_lister",
        data_from=season_detail_fetcher_transformer,  # Lister takes from season details
    )
    def athlete_refs_lister_transformer(season_detail: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        Extracts 'athletes.$ref' from season_detail, paginates, and yields athlete $ref objects,
        augmented with season_id_fk (for discovery context).
        """
        season_id = season_detail.get("id")  # Season year
        athletes_collection_url = season_detail.get("athletes", {}).get("$ref")

        if not season_id:
            logger.warning(
                f"Season detail missing 'id'. Skipping athlete refs listing. "
                f"Detail: {season_detail}"
            )
            return  # yield from []

        if not athletes_collection_url:
            logger.info(
                f"Season detail for season '{season_id}' missing 'athletes.$ref'. "
                f"No athletes to list for this season."
            )
            return  # yield from []

        logger.debug(
            f"Listing athlete refs for season '{season_id}' from collection: "
            f"{athletes_collection_url}"
        )
        try:
            for athlete_ref_page in list_client.paginate(
                athletes_collection_url, params={"limit": API_LIMIT}
            ):
                for athlete_ref_item in athlete_ref_page:
                    if "$ref" in athlete_ref_item:
                        athlete_ref_item_augmented = athlete_ref_item.copy()
                        # This season_id_fk is for context of *where* this athlete ref was found
                        athlete_ref_item_augmented["discovery_season_id_fk"] = str(season_id)
                        yield athlete_ref_item_augmented
                    else:
                        logger.warning(
                            f"Athlete ref item missing '$ref' key in page "
                            f"from {athletes_collection_url} for season '{season_id}'. "
                            f"Item: {athlete_ref_item}"
                        )

        except Exception as e:
            logger.error(
                f"Error listing athlete refs for season '{season_id}' "
                f"from {athletes_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="athletes",  # Table for master athlete details
        data_from=athlete_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",  # Assuming athlete 'id' is globally unique for master data
    )
    @dlt.defer
    def athlete_detail_fetcher_transformer(athlete_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full athlete details for an individual athlete $ref object.
        Augments with discovery_season_id_fk.
        """
        detail_url = athlete_ref_item.get("$ref")
        # This FK indicates the season context in which this athlete record was found/fetched.
        # Not part of the athlete's own master PK unless IDs are not globally unique.
        discovery_season_id_fk = athlete_ref_item.get("discovery_season_id_fk")

        if not detail_url:
            logger.warning(f"Athlete ref item missing '$ref'. Item: {athlete_ref_item}")
            return None

        # discovery_season_id_fk is good for context but not strictly essential for fetching
        # if URL is absolute
        # if not discovery_season_id_fk:
        #     logger.warning(f"Athlete ref item missing 'discovery_season_id_fk'.
        #     Item: {athlete_ref_item}")
        # Decide if this is critical enough to return None

        logger.debug(
            f"Fetching athlete detail (discovery season '{discovery_season_id_fk}') "
            f"from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            athlete_detail = response.json()

            api_athlete_id = athlete_detail.get("id")
            if api_athlete_id is not None:
                athlete_detail["id"] = str(
                    api_athlete_id
                )  # Ensure athlete's own ID is string for PK

                if discovery_season_id_fk:  # Add the discovery context FK
                    athlete_detail["discovery_season_id_fk"] = str(discovery_season_id_fk)

                # Athlete detail might contain $refs for position, possibly headshot, etc.
                # These could be new sources for other master tables or detail fetchers.
                # e.g., athlete_detail.get("position", {}).get("$ref") -> position_detail_fetcher

                return athlete_detail
            else:
                logger.warning(
                    f"Fetched athlete detail from {detail_url} "
                    f"(discovery season '{discovery_season_id_fk}') missing 'id'. "
                    f"Detail: {athlete_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching athlete detail from {detail_url} "
                f"(discovery_season_id_fk: {discovery_season_id_fk}): {e}",
                exc_info=True,
            )
            return None

    # --- Opportunistic Master Data: Venue Refs Extractors ---
    @dlt.transformer(name="team_venue_ref_extractor", data_from=team_detail_fetcher_transformer)
    def team_venue_ref_extractor_transformer(
        team_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """Extracts venue $ref from team_detail if present."""
        venue_ref_url = team_detail.get("venue", {}).get("$ref")
        team_id = team_detail.get("id")  # For logging context

        if venue_ref_url:
            logger.debug(f"Team '{team_id}' has venue ref: {venue_ref_url}")
            yield {"venue_ref_url": venue_ref_url, "_source_discovery": f"team_{team_id}"}
        # No yield if not found

    @dlt.transformer(name="event_venue_ref_extractor", data_from=event_detail_fetcher_transformer)
    def event_venue_ref_extractor_transformer(
        event_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """Extracts venue $ref from event_detail if present."""
        venue_ref_url = None
        event_id = event_detail.get("id")  # For logging context
        competitions = event_detail.get("competitions")
        if competitions and isinstance(competitions, list) and competitions[0]:
            venue_ref_url = competitions[0].get("venue", {}).get("$ref")

        if venue_ref_url:
            logger.debug(f"Event '{event_id}' has venue ref: {venue_ref_url}")
            yield {"venue_ref_url": venue_ref_url, "_source_discovery": f"event_{event_id}"}
        # No yield if not found

    # --- Opportunistic Master Data: Position Refs Extractor ---
    @dlt.transformer(
        name="athlete_position_ref_extractor", data_from=athlete_detail_fetcher_transformer
    )
    def athlete_position_ref_extractor_transformer(
        athlete_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """Extracts position $ref from athlete_detail if present."""
        position_ref_url = athlete_detail.get("position", {}).get("$ref")
        athlete_id = athlete_detail.get("id")  # For logging context

        if position_ref_url:
            logger.debug(f"Athlete '{athlete_id}' has position ref: {position_ref_url}")
            yield {
                "position_ref_url": position_ref_url,
                "_source_discovery": f"athlete_{athlete_id}",
            }
        # No yield if not found

    # --- Master Data Detail Fetchers ---

    @dlt.transformer(
        name="venues",
        data_from=team_venue_ref_extractor_transformer | event_venue_ref_extractor_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def venue_detail_fetcher_transformer(venue_ref_container: dict[str, Any]) -> TDataItem | None:
        """Fetches venue details from a $ref URL."""
        detail_url = venue_ref_container.get("venue_ref_url")
        source_discovery_info = venue_ref_container.get("_source_discovery", "unknown")

        if not detail_url:
            logger.warning(
                f"Venue ref container missing 'venue_ref_url'. Item: {venue_ref_container}"
            )
            return None

        logger.debug(
            f"Fetching venue detail (discovered via '{source_discovery_info}') from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            venue_detail = response.json()

            api_venue_id = venue_detail.get("id")
            if api_venue_id is not None:
                venue_detail["id"] = str(api_venue_id)
                return venue_detail
            else:
                logger.warning(
                    f"Fetched venue detail from {detail_url} (source: {source_discovery_info}) missing 'id'. Detail: {venue_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching venue detail from {detail_url} (source: {source_discovery_info}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="positions",
        data_from=athlete_position_ref_extractor_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def position_detail_fetcher_transformer(
        position_ref_container: dict[str, Any],
    ) -> TDataItem | None:
        """Fetches position details from a $ref URL."""
        detail_url = position_ref_container.get("position_ref_url")
        source_discovery_info = position_ref_container.get("_source_discovery", "unknown")

        if not detail_url:
            logger.warning(
                f"Position ref container missing 'position_ref_url'. Item: {position_ref_container}"
            )
            return None

        logger.debug(
            f"Fetching position detail (discovered via '{source_discovery_info}') from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            position_detail = response.json()

            api_position_id = position_detail.get("id")
            if api_position_id is not None:
                position_detail["id"] = str(api_position_id)
                return position_detail
            else:
                logger.warning(
                    f"Fetched position detail from {detail_url} (source: {source_discovery_info}) missing 'id'. Detail: {position_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching position detail from {detail_url} (source: {source_discovery_info}): {e}",
                exc_info=True,
            )
            return None

    # --- Opportunistic Master Data: Provider Refs Extractor (from Event Odds) ---
    @dlt.transformer(name="odds_provider_ref_extractor", data_from=event_odds_transformer)
    def odds_provider_ref_extractor_transformer(
        odds_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """Extracts provider $ref from an odds_record if present."""
        provider_obj = odds_record.get("provider", {})
        provider_ref_url = provider_obj.get("$ref") if isinstance(provider_obj, dict) else None
        event_id_fk = odds_record.get("event_id_fk")  # For logging
        provider_id_fk = odds_record.get("provider_id_fk")  # For logging

        if provider_ref_url:
            logger.debug(
                f"Event odds record for event '{event_id_fk}', provider '{provider_id_fk}' has provider detail ref: {provider_ref_url}"
            )
            yield {
                "provider_ref_url": provider_ref_url,
                "_source_discovery": f"event_odds_ev{event_id_fk}_p{provider_id_fk}",
            }
        elif provider_id_fk:
            logger.debug(
                f"Event odds record for event '{event_id_fk}', provider '{provider_id_fk}' missing 'provider.$ref'. Cannot extract provider detail."
            )
        # No yield if not found

    # --- Opportunistic Master Data: Media Refs Extractor (from Event Broadcasts) ---
    @dlt.transformer(name="broadcast_media_ref_extractor", data_from=event_broadcasts_transformer)
    def broadcast_media_ref_extractor_transformer(
        broadcast_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """Extracts media $ref from a broadcast_record if present."""
        media_obj = broadcast_record.get("media", {})
        media_ref_url = media_obj.get("$ref") if isinstance(media_obj, dict) else None
        event_id_fk = broadcast_record.get("event_id_fk")  # For logging
        media_id_fk = broadcast_record.get("media_id_fk")  # For logging

        if media_ref_url:
            logger.debug(
                f"Event broadcast record for event '{event_id_fk}', media '{media_id_fk}' has media detail ref: {media_ref_url}"
            )
            yield {
                "media_ref_url": media_ref_url,
                "_source_discovery": f"event_broadcast_ev{event_id_fk}_m{media_id_fk}",
            }
        elif media_id_fk:
            logger.debug(
                f"Event broadcast record for event '{event_id_fk}', media '{media_id_fk}' missing 'media.$ref'. Cannot extract media detail."
            )
        # No yield if not found

    # --- Master Data Detail Fetchers (Continued) ---

    @dlt.transformer(
        name="providers",  # Master table for odds providers
        data_from=odds_provider_ref_extractor_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def provider_detail_fetcher_transformer(
        provider_ref_container: dict[str, Any],
    ) -> TDataItem | None:
        """Fetches odds provider details from a $ref URL."""
        detail_url = provider_ref_container.get("provider_ref_url")
        source_discovery_info = provider_ref_container.get(
            "_source_discovery", "unknown_odds_event"
        )

        if not detail_url:
            logger.warning(
                f"Provider ref container missing 'provider_ref_url'. Item: {provider_ref_container}"
            )
            return None

        logger.debug(
            f"Fetching provider detail (discovered via '{source_discovery_info}') from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            provider_detail = response.json()

            api_provider_id = provider_detail.get("id")
            if api_provider_id is not None:
                provider_detail["id"] = str(api_provider_id)
                return provider_detail
            else:
                logger.warning(
                    f"Fetched provider detail from {detail_url} (source: {source_discovery_info}) missing 'id'. Detail: {provider_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching provider detail from {detail_url} (source: {source_discovery_info}): {e}",
                exc_info=True,
            )
            return None

    @dlt.transformer(
        name="media",  # Master table for broadcast media outlets
        data_from=broadcast_media_ref_extractor_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def media_detail_fetcher_transformer(media_ref_container: dict[str, Any]) -> TDataItem | None:
        """Fetches media outlet details from a $ref URL."""
        detail_url = media_ref_container.get("media_ref_url")
        source_discovery_info = media_ref_container.get(
            "_source_discovery", "unknown_broadcast_event"
        )

        if not detail_url:
            logger.warning(
                f"Media ref container missing 'media_ref_url'. Item: {media_ref_container}"
            )
            return None

        logger.debug(
            f"Fetching media detail (discovered via '{source_discovery_info}') from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            media_detail = response.json()

            api_media_id = media_detail.get("id")
            if api_media_id is not None:
                media_detail["id"] = str(api_media_id)
                return media_detail
            else:
                logger.warning(
                    f"Fetched media detail from {detail_url} (source: {source_discovery_info}) missing 'id'. Detail: {media_detail}"
                )
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error fetching media detail from {detail_url} (source: {source_discovery_info}): {e}",
                exc_info=True,
            )
            return None

    # --- Coaches (Master & Seasonal Assignments) ---

    @dlt.transformer(  # This resource yields data for the 'coach_team_assignments' table
        name="coach_team_assignments",
        data_from=team_detail_fetcher_transformer,
        write_disposition="merge",
        primary_key=[
            "id",
            "team_id_fk",
            "season_id_fk",
        ],  # 'id' here is the coach's id for the assignment
    )
    def coach_team_assignments_resource(team_detail: dict[str, Any]) -> Iterable[TDataItem] | None:
        """
        Lists coach assignments for a given team and season.
        Each yielded item represents a coach's role for that team/season and includes a ref to master coach data.
        """
        team_id_fk = team_detail.get("id")
        season_id_fk = team_detail.get("season_id_fk")
        coaches_collection_url = team_detail.get("coaches", {}).get("$ref")

        if not all([team_id_fk, season_id_fk]):
            logger.warning(
                f"Team detail missing 'id' (team_id_fk) or 'season_id_fk'. Cannot list coach assignments. Detail: {team_detail}"
            )
            yield from []
            return

        if not coaches_collection_url:
            logger.info(
                f"Team detail for team '{team_id_fk}', season '{season_id_fk}' missing 'coaches.$ref'. No coach assignments to list."
            )
            yield from []
            return

        logger.debug(
            f"Listing coach assignments for team '{team_id_fk}', season '{season_id_fk}' from: {coaches_collection_url}"
        )
        try:
            for coach_assignment_page in list_client.paginate(
                coaches_collection_url,
                params={"limit": API_LIMIT},  # Usually very few coaches per team
            ):
                for item in coach_assignment_page:
                    if (
                        not isinstance(item, dict) or "id" not in item
                    ):  # 'id' here is the coach's id
                        logger.warning(
                            f"Malformed coach assignment item for team '{team_id_fk}', season '{season_id_fk}'. Item: {item}"
                        )
                        continue

                    assignment_record = item.copy()
                    assignment_record["id"] = str(item["id"])  # Ensure coach's ID is string for PK
                    assignment_record["team_id_fk"] = str(team_id_fk)
                    assignment_record["season_id_fk"] = str(season_id_fk)

                    # The 'coach' field with its '$ref' to master data is already in 'item'
                    # Example: item = {'$ref': 'seasonal_coach_url', 'id': 'coach123', 'type': 'HC', 'coach': {'$ref': 'master_coach_url'}}
                    yield assignment_record

        except Exception as e:
            logger.error(
                f"Unexpected error listing coach assignments for team '{team_id_fk}', season '{season_id_fk}' from {coaches_collection_url}: {e}",
                exc_info=True,
            )
        # Implicitly yields nothing if loop doesn't run or error

    @dlt.transformer(
        name="coach_master_ref_extractor",  # Intermediate step, does not create a dlt table itself
        data_from=coach_team_assignments_resource,  # Consumes items from the assignments resource
    )
    def coach_master_ref_extractor_transformer(
        coach_assignment_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]] | None:
        """
        Extracts the master coach $ref URL and coach ID from a coach_assignment_record.
        """
        master_coach_ref_url = coach_assignment_record.get("coach", {}).get("$ref")
        # 'id' of coach_assignment_record is the coach's ID for that assignment
        coach_id_for_master = coach_assignment_record.get("id")

        team_id_fk = coach_assignment_record.get("team_id_fk", "unknown_team")
        season_id_fk = coach_assignment_record.get("season_id_fk", "unknown_season")

        if master_coach_ref_url and coach_id_for_master:
            logger.debug(
                f"Extracted master coach ref '{master_coach_ref_url}' for coach '{coach_id_for_master}' "
                f"(from assignment team '{team_id_fk}', season '{season_id_fk}')"
            )
            yield {
                "coach_ref_url": master_coach_ref_url,
                "coach_id_for_master": str(coach_id_for_master),  # Ensure it's a string
                "_source_discovery_assignment": f"team_{team_id_fk}_season_{season_id_fk}_coach_{coach_id_for_master}",
            }
        else:
            logger.debug(
                f"Coach assignment record for coach '{coach_id_for_master}' (team '{team_id_fk}', season '{season_id_fk}') "
                f"missing 'coach.$ref' or its own 'id'. Record: {coach_assignment_record}"
            )
        # No yield if refs/IDs are missing

    @dlt.transformer(  # This resource yields data for the 'coaches' master table
        name="coaches",
        data_from=coach_master_ref_extractor_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def coaches_resource(coach_master_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches master coach details using the $ref URL.
        The 'id' for the coaches table comes from 'coach_id_for_master' in the input item.
        """
        detail_url = coach_master_ref_item.get("coach_ref_url")
        # This ID should be used as the PK for the master coaches table
        expected_coach_id = coach_master_ref_item.get("coach_id_for_master")
        source_discovery_info = coach_master_ref_item.get(
            "_source_discovery_assignment", "unknown_assignment"
        )

        if not detail_url or not expected_coach_id:
            logger.warning(
                f"Coach master ref item missing 'coach_ref_url' or 'coach_id_for_master'. Item: {coach_master_ref_item}"
            )
            return None

        logger.debug(
            f"Fetching master coach detail for ID '{expected_coach_id}' (discovered via '{source_discovery_info}') from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            coach_master_detail = response.json()

            api_coach_id = coach_master_detail.get("id")
            if api_coach_id is not None:
                # Ensure the ID from the API matches the one we expect and use it for the record.
                # The `expected_coach_id` is derived from the assignment data, which should be authoritative for linking.
                coach_master_detail["id"] = str(expected_coach_id)
                if str(api_coach_id) != str(expected_coach_id):
                    logger.warning(
                        f"Mismatch between expected coach ID ('{expected_coach_id}') and API coach ID ('{api_coach_id}') "
                        f"from {detail_url}. Using expected ID for master record."
                    )
                return coach_master_detail
            else:
                logger.warning(
                    f"Fetched master coach detail from {detail_url} (expected ID '{expected_coach_id}', source: {source_discovery_info}) missing 'id' in response. Detail: {coach_master_detail}"
                )
                # Still try to save it using the expected_coach_id if the rest of the data is valuable
                coach_master_detail["id"] = str(expected_coach_id)
                return coach_master_detail

        except Exception as e:
            logger.error(
                f"Error during API operation for {detail_url} (master coach ID '{expected_coach_id}', source: {source_discovery_info}): {e}",
                exc_info=True,
            )
            return None

    # --- Franchises (Master) ---

    @dlt.transformer(
        name="franchise_refs_lister",
        data_from=league_info_resource,  # Depends on league_info to ensure league_base_url is resolved
    )
    def franchise_refs_lister_transformer(league_doc: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        Lists all franchise $ref objects from the /franchises endpoint relative to the league_base_url.
        The league_base_url is passed implicitly via the espn_source function's scope.
        """
        # league_base_url is available from the espn_source function's scope
        # Ensure it doesn't end with a slash if we are appending one.
        # Or, more robustly, get it from league_doc if it was passed through,
        # but for this API, direct construction from league_base_url is likely fine.

        # The root league_base_url itself might already point to the base for franchises,
        # or franchises might be a sub-path. For this API, it's typically:
        # http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/franchises
        # is NOT standard. The /franchises endpoint is usually at the sports level or a global level.
        # Let's assume /franchises is a top-level path that might need to be constructed
        # relative to a more general API root if league_base_url is too specific.
        # However, the example indicates a direct /franchises. Let's assume it's relative to the provided league_base_url
        # or that all $refs are absolute anyway.
        # For now, the current setup assumes that $refs provided by the API are absolute.
        # This lister will list from a collection endpoint under league_doc if one exists,
        # OR if there's a known global /franchises endpoint.
        # The API Endpoint Inventory points to /franchises as a top level ref list.
        # This implies it might be listed under the main league doc, or as a known fixed path.

        # A common pattern would be league_doc["franchises"]["$ref"]
        # Let's check if the league_doc even has a franchise ref.
        # If not, we'll assume a constructed path based on league_base_url.

        franchises_collection_url = league_doc.get("franchises", {}).get("$ref")

        if not franchises_collection_url:
            # Fallback: try to construct it if the API pattern is known and consistent
            # For ESPN, franchise data might be linked from team data or other entities,
            # or listed globally. The inventory just says "/franchises".
            # Let's assume for this API, it's relative to the *sports root*, not the league root.
            # e.g. http://sports.core.api.espn.com/v2/sports/basketball/franchises
            # This is a tricky one without an explicit $ref in the league doc or a clear example.
            # For now, let's make it simpler and assume it's available from league_base_url + "/franchises"
            # if not directly referenced in league_doc.
            # However, the typical pattern for this API is that such lists are linked.
            # If `league_doc["franchises"]["$ref"]` isn't there, this resource might not be discoverable this way.

            # For NCAA basketball, "franchises" might not be a primary concept like in pro sports.
            # Let's assume the inventory means a general franchises endpoint if accessible from the base.
            # The API inventory says: Endpoint Path (List of Refs): /franchises
            # This implies either league_base_url + /franchises or that they are absolute in some other listing.
            # Given our current structure starts from league_base_url, appending /franchises is the most direct.

            # If franchises are discovered opportunistically (e.g. from team details),
            # then we would have ref_extractors similar to venues/positions.
            # Since the API inventory lists it as a direct discoverable list, we'll try that.

            # Using the main league_base_url that was passed into espn_source()
            constructed_franchises_url = f"{league_base_url.rstrip('/')}/franchises"
            logger.info(
                f"League doc for '{league_doc.get('id')}' did not directly reference a franchises collection. "
                f"Attempting to list franchises from constructed URL: {constructed_franchises_url}"
            )
            franchises_collection_url = constructed_franchises_url  # Use the constructed one

        if not franchises_collection_url:
            logger.error(
                f"Could not determine franchise collection URL. League doc: {league_doc.get('id')}"
            )
            return

        logger.debug(f"Listing all franchise refs from collection: {franchises_collection_url}")
        try:
            for franchise_ref_page in list_client.paginate(
                franchises_collection_url, params={"limit": API_LIMIT}
            ):
                for franchise_ref_item in franchise_ref_page:
                    if "$ref" in franchise_ref_item:
                        # No specific FK needed from league_doc for master franchises
                        yield franchise_ref_item
                    else:
                        logger.warning(
                            f"Franchise ref item missing '$ref' key in page "
                            f"from {franchises_collection_url}. Item: {franchise_ref_item}"
                        )
        except Exception as e:
            logger.error(
                f"Error listing franchise refs from {franchises_collection_url}: {e}", exc_info=True
            )

    @dlt.transformer(
        name="franchises",
        data_from=franchise_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def franchises_resource(franchise_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches full franchise details for an individual franchise $ref object.
        """
        detail_url = franchise_ref_item.get("$ref")

        if not detail_url:
            logger.warning(f"Franchise ref item missing '$ref'. Item: {franchise_ref_item}")
            return None

        logger.debug(f"Fetching franchise detail from: {detail_url}")
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            franchise_detail = response.json()

            api_franchise_id = franchise_detail.get("id")
            if api_franchise_id is not None:
                franchise_detail["id"] = str(api_franchise_id)  # Ensure ID is string for PK
                return franchise_detail
            else:
                logger.warning(
                    f"Fetched franchise detail from {detail_url} missing 'id'. Detail: {franchise_detail}"
                )
                return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching franchise detail from {detail_url}: {e}", exc_info=True
            )
            return None

    # --- Awards (Master & Seasonal) ---

    # Part 1: Master Award Definitions
    @dlt.transformer(name="award_master_refs_lister", data_from=league_info_resource)
    def award_master_refs_lister_transformer(
        league_doc: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """
        Lists all master award definition $ref objects from the /awards endpoint
        relative to the league_base_url.
        """
        # league_base_url is available from the espn_source function's scope
        master_awards_collection_url = league_doc.get("awards", {}).get("$ref")

        if not master_awards_collection_url:
            # Attempt to construct if not directly in league_doc (less common for global master lists)
            # For this API, /awards is usually a top-level endpoint for the sport.
            # We might need a more robust way to get the true "sports" base URL if league_base_url is too deep.
            # For now, assume it might be at league_base_url + /awards or absolute in the $ref.
            # If the $ref itself in league_doc is absolute, this construction is not needed.
            # Given inventory says "/awards", it suggests it might be relative to the league or sports root.
            logger.info(
                f"League doc '{league_doc.get('id')}' did not contain 'awards.$ref'. "
                f"Attempting general awards list from league_base_url: {league_base_url.rstrip('/')}/awards"
            )
            master_awards_collection_url = f"{league_base_url.rstrip('/')}/awards"
            # This might still 404 if awards are only listed seasonally or not at all for this league.

        if not master_awards_collection_url:  # Still no URL
            logger.warning(
                "Could not determine master awards collection URL. Skipping master awards."
            )
            return

        logger.debug(
            f"Listing all master award refs from collection: {master_awards_collection_url}"
        )
        try:
            for award_ref_page in list_client.paginate(
                master_awards_collection_url, params={"limit": API_LIMIT}
            ):
                for award_ref_item in award_ref_page:
                    if "$ref" in award_ref_item:
                        yield award_ref_item
                    else:
                        logger.warning(
                            f"Master award ref item missing '$ref' key in page "
                            f"from {master_awards_collection_url}. Item: {award_ref_item}"
                        )
        except Exception as e:
            logger.error(
                f"Error listing master award refs from {master_awards_collection_url}: {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="awards_master",
        data_from=award_master_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def award_master_detail_fetcher_transformer(
        award_master_ref_item: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches full master award details for an individual award $ref object.
        """
        detail_url = award_master_ref_item.get("$ref")

        if not detail_url:
            logger.warning(f"Master award ref item missing '$ref'. Item: {award_master_ref_item}")
            return None

        logger.debug(f"Fetching master award detail from: {detail_url}")
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            award_master_detail = response.json()

            api_award_id = award_master_detail.get("id")
            if api_award_id is not None:
                award_master_detail["id"] = str(api_award_id)
                return award_master_detail
            else:
                logger.warning(
                    f"Fetched master award detail from {detail_url} missing 'id'. Detail: {award_master_detail}"
                )
                return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching master award detail from {detail_url}: {e}",
                exc_info=True,
            )
            return None

    # Part 2: Seasonal Award Instances
    @dlt.transformer(
        name="season_award_instance_refs_lister", data_from=season_detail_fetcher_transformer
    )
    def season_award_instance_refs_lister_transformer(
        season_detail: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """
        Lists seasonal award instance $ref objects from season_detail.awards.$ref,
        augmented with season_id_fk.
        """
        season_id_fk = season_detail.get("id")
        seasonal_awards_collection_url = season_detail.get("awards", {}).get("$ref")

        if not season_id_fk:
            logger.warning(
                f"Season detail missing 'id'. Skipping seasonal awards. Detail: {season_detail}"
            )
            return

        if not seasonal_awards_collection_url:
            logger.info(
                f"Season detail for season '{season_id_fk}' missing 'awards.$ref'. No seasonal awards to list."
            )
            return

        logger.debug(
            f"Listing seasonal award instance refs for season '{season_id_fk}' from: {seasonal_awards_collection_url}"
        )
        try:
            for award_instance_ref_page in list_client.paginate(
                seasonal_awards_collection_url, params={"limit": API_LIMIT}
            ):
                for item in award_instance_ref_page:
                    if "$ref" in item:
                        ref_item_augmented = item.copy()
                        ref_item_augmented["season_id_fk"] = str(season_id_fk)
                        yield ref_item_augmented
                    else:
                        logger.warning(
                            f"Seasonal award instance ref item missing '$ref' key for season '{season_id_fk}'. Item: {item}"
                        )
        except Exception as e:
            logger.error(
                f"Error listing seasonal award instance refs for season '{season_id_fk}': {e}",
                exc_info=True,
            )

    @dlt.transformer(
        name="awards_seasonal",
        data_from=season_award_instance_refs_lister_transformer,
        write_disposition="merge",
        primary_key=["id", "season_id_fk"],
    )
    @dlt.defer
    def season_award_instance_detail_fetcher_transformer(
        seasonal_award_ref_item: dict[str, Any],
    ) -> TDataItem | None:
        """
        Fetches full seasonal award instance details.
        Extracts recipient and master award type if available.
        """
        detail_url = seasonal_award_ref_item.get("$ref")
        season_id_fk = seasonal_award_ref_item.get("season_id_fk")

        if not detail_url or not season_id_fk:
            logger.warning(
                f"Seasonal award ref item missing '$ref' or 'season_id_fk'. Item: {seasonal_award_ref_item}"
            )
            return None

        logger.debug(
            f"Fetching seasonal award instance detail for season '{season_id_fk}' from: {detail_url}"
        )
        try:
            response = detail_client.get(detail_url)
            response.raise_for_status()
            seasonal_award_detail = response.json()

            instance_id = seasonal_award_detail.get("id")
            if instance_id is None:
                logger.warning(
                    f"Fetched seasonal award instance from {detail_url} (season '{season_id_fk}') missing 'id'. Detail: {seasonal_award_detail}"
                )
                return None  # Instance ID is crucial for PK

            processed_detail = seasonal_award_detail.copy()
            processed_detail["id"] = str(instance_id)
            processed_detail["season_id_fk"] = str(season_id_fk)

            # Extract master award ID
            award_type_obj = seasonal_award_detail.get("type", {})
            if isinstance(award_type_obj, dict) and "id" in award_type_obj:
                processed_detail["award_master_id_fk"] = str(award_type_obj["id"])
            elif isinstance(award_type_obj, dict) and "$ref" in award_type_obj:
                # Try to parse ID from $ref if direct ID is not there
                try:
                    processed_detail["award_master_id_fk"] = (
                        award_type_obj["$ref"].split("/")[-1].split("?")[0]
                    )
                except Exception:
                    logger.warning(
                        f"Could not parse award_master_id_fk from type.$ref: {award_type_obj['$ref']}"
                    )

            # Extract recipient ID (can be athlete or team)
            recipient_obj = seasonal_award_detail.get("recipient", {})
            if isinstance(recipient_obj, dict):
                athlete_recipient_obj = recipient_obj.get("athlete", {})
                if isinstance(athlete_recipient_obj, dict) and "id" in athlete_recipient_obj:
                    processed_detail["recipient_athlete_id_fk"] = str(athlete_recipient_obj["id"])

                team_recipient_obj = recipient_obj.get("team", {})
                if isinstance(team_recipient_obj, dict) and "id" in team_recipient_obj:
                    processed_detail["recipient_team_id_fk"] = str(team_recipient_obj["id"])

            return processed_detail

        except Exception as e:
            logger.error(
                f"Unexpected error fetching seasonal award instance from {detail_url} (season '{season_id_fk}'): {e}",
                exc_info=True,
            )
            return None

    # Define other resources and transformers here following the
    # "Lister + Detail Fetcher with @dlt.defer" pattern.

    return (
        league_info_resource,
        season_detail_fetcher_transformer,
        event_refs_lister_transformer,
        event_detail_fetcher_transformer,
        event_competitors_transformer,
        event_scores_detail_fetcher_transformer,
        event_linescores_transformer,
        event_team_stats_processor_transformer,
        event_player_stats_refs_lister_transformer,
        event_player_stats_detail_fetcher_transformer,
        event_leaders_detail_fetcher_transformer,
        event_roster_detail_fetcher_transformer,
        event_pregame_records_transformer,
        event_status_detail_fetcher_transformer,
        event_situation_detail_fetcher_transformer,
        event_odds_transformer,
        event_broadcasts_transformer,
        event_predictor_detail_fetcher_transformer,
        event_probabilities_transformer,
        event_powerindex_transformer,
        event_officials_transformer,
        event_plays_lister_transformer,
        # Master / Dimension Tables
        team_refs_lister_transformer,
        team_detail_fetcher_transformer,
        athlete_refs_lister_transformer,
        athlete_detail_fetcher_transformer,
        # Opportunistic Master Data
        team_venue_ref_extractor_transformer,
        event_venue_ref_extractor_transformer,
        venue_detail_fetcher_transformer,
        athlete_position_ref_extractor_transformer,
        position_detail_fetcher_transformer,
        # Provider ref extractor & detail fetcher (from event_odds)
        odds_provider_ref_extractor_transformer,
        provider_detail_fetcher_transformer,
        # Media ref extractor & detail fetcher (from event_broadcasts)
        broadcast_media_ref_extractor_transformer,
        media_detail_fetcher_transformer,
        # Coaches (Assignments & Master)
        coach_team_assignments_resource,
        coach_master_ref_extractor_transformer,
        coaches_resource,
        # Franchises
        franchise_refs_lister_transformer,
        franchises_resource,
        # Awards (Master & Seasonal)
        award_master_refs_lister_transformer,
        award_master_detail_fetcher_transformer,
        season_award_instance_refs_lister_transformer,
        season_award_instance_detail_fetcher_transformer,
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
