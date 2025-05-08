"""
dlt source : Fetches ESPN API data starting from the league's root URL.
Demonstrates fetching league info and then branching to seasons.
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
def espn_mens_college_basketball_source_(
    base_url: str = dlt.config.value,  # e.g., http://.../leagues/mens-college-basketball
    season_year_filter: str | None = None,  # Optional: To filter for a specific season
) -> Iterable[DltResource]:
    """
    Defines dlt resources for fetching NCAA Men's Basketball data from the ESPN API,
    starting with the league's root document.

    Args:
        base_url: str = dlt.config.value (str): The base URL for the specific api.
        season_year_filter (Optional[str]): If provided, only this season will be processed.

    Returns:
        Iterable[DltResource]: An iterable containing the dlt resources.
    """

    # Client for endpoints that list items/refs and use page number pagination
    # This client will be used when its base_url is set to a collection endpoint
    # (e.g., a seasons collection URL)
    list_paginator = PageNumberPaginator(
        page_param="page", total_path="pageCount", base_page=1, stop_after_empty_page=True
    )

    # Detail Client: For fetching single detail objects from absolute $ref URLs.
    # base_url=None because $ref URLs are absolute.
    detail_client = RESTClient(base_url=None, headers={})

    # --- League Root Information Resource ---
    @dlt.resource(name="league_info", write_disposition="replace", primary_key="id")
    def league_info_resource() -> Iterable[dict[str, Any]]:
        """
        Fetches the main information document for the league from the base_url.
        This document contains links ($refs) to other top-level collections like seasons, teams, etc.
        """
        logger.info(f"Fetching league root information from: {base_url}")
        try:
            response = detail_client.get(base_url)  # Use detail_client as URL is absolute
            response.raise_for_status()
            league_data = response.json()

            # The league data itself might have an 'id' or 'uid' that can serve as a primary key.
            # Example: league_data['id'] = league_data.get('id', base_url) # Ensure an ID
            if "id" not in league_data and "uid" in league_data:  # Use uid if id is missing
                league_data["id"] = league_data["uid"]
            elif "id" not in league_data:  # Fallback if no clear ID
                league_data["id"] = base_url  # Not ideal, but ensures a PK

            # Add the season_year_filter to the yielded item if it exists,
            # so downstream transformers can use it.
            if season_year_filter:
                league_data["_season_year_filter"] = season_year_filter

            yield league_data
            logger.info(
                f"Successfully fetched and yielded league root information for {league_data.get('name', base_url)}."
            )
        except Exception as e:
            logger.error(
                f"Error fetching league root information from {base_url}: {e}", exc_info=True
            )
            # Optionally, raise to stop the pipeline or yield nothing
            # raise

    # --- Seasons Processing Chain ---

    @dlt.transformer(
        name="season_refs_lister",
        data_from=league_info_resource,  # Takes data from the league_info_resource
    )
    def season_refs_lister_transformer(league_doc: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """
        Extracts the $ref to the seasons collection from the league document.
        If a season_year_filter is present, it constructs a direct $ref to that season.
        Otherwise, it prepares to list all season $refs from the seasons collection URL.
        Yields item(s) containing the URL to fetch season(s) and any necessary context.
        """
        current_season_year_filter = league_doc.get("_season_year_filter")

        if current_season_year_filter:
            # If a specific season is requested, construct its direct $ref
            # The 'seasons' $ref in the league doc might point to the collection,
            # but we need the base for constructing a specific season URL.
            # Assuming the league_doc.$ref or a similar field gives the league's own base URL.
            # Or, more simply, use the original base_url to construct.

            # Example: league_doc['season']['$ref'] gives current season.
            # league_doc['seasons']['$ref'] gives collection of all seasons.
            # We need to be careful how we get the base for /seasons/{year}

            # Let's assume the base_url is the correct base for /seasons/{year}
            # This is a simplification; a robust solution might parse league_doc['seasons']['$ref']
            # to get the true base of the seasons collection if it differs.
            season_detail_url = f"{base_url}/seasons/{current_season_year_filter}"

            logger.info(
                f"Specific season filter '{current_season_year_filter}' provided. Preparing to fetch: {season_detail_url}"
            )
            yield {
                "$ref": season_detail_url,
                "type": "specific_season_detail",
                # Pass along the original league ID if needed for FKs later
                "league_id_fk": league_doc.get("id"),
            }
        else:
            # No specific season, so get the $ref for the entire seasons collection
            seasons_collection_ref = league_doc.get("seasons", {}).get("$ref")
            if not seasons_collection_ref:
                logger.error(
                    f"League document for {league_doc.get('id')} is missing 'seasons.$ref'. Cannot list seasons."
                )
                return

            logger.info(
                f"No specific season filter. Preparing to list all season refs from: {seasons_collection_ref}"
            )
            yield {
                "$ref": seasons_collection_ref,
                "type": "season_collection_list",
                "league_id_fk": league_doc.get("id"),
            }

    @dlt.transformer(
        name="season_details",  # This will be the table name for season details
        data_from=season_refs_lister_transformer,
        write_disposition="merge",
        primary_key="id",
    )
    @dlt.defer
    def season_detail_fetcher_transformer(season_ref_item: dict[str, Any]) -> TDataItem | None:
        """
        Fetches season details.
        If item type is 'specific_season_detail', fetches that one season.
        If item type is 'season_collection_list', it should ideally be further processed by
        another lister that paginates through the collection.
        For this , if it's a collection, we'll assume it's a $ref to a list that
        detail_client can fetch (if the list isn't too large and doesn't need pagination itself).
        A more robust version would have another lister here for the collection.

        This  simplifies: if it's a collection ref, it assumes it's the URL to list all season refs.
        And then it would fetch each one. This part needs refinement for true pagination of season list.
        """
        detail_url = season_ref_item.get("$ref")
        item_type = season_ref_item.get("type")
        league_id_fk = season_ref_item.get("league_id_fk")

        if not detail_url:
            logger.warning(f"Season reference item missing '$ref'. Item: {season_ref_item}")
            return None

        if item_type == "specific_season_detail":
            logger.info(f"Fetching specific season detail from: {detail_url}")
            try:
                response = detail_client.get(detail_url)
                response.raise_for_status()
                season_detail = response.json()

                # Ensure 'id' (from API 'year') and add league_id_fk
                season_api_year = season_detail.get("year")
                if season_api_year is not None:
                    season_detail["id"] = str(season_api_year)
                    if league_id_fk:
                        season_detail["league_id_fk"] = str(league_id_fk)
                    return season_detail  # Use return for @dlt.defer with single item
                else:
                    logger.warning(
                        f"Fetched season detail from {detail_url} missing 'year'. Detail: {season_detail}"
                    )
                    return None
            except Exception as e:
                logger.error(
                    f"Error fetching specific season detail from {detail_url}: {e}", exc_info=True
                )
                return None

        elif item_type == "season_collection_list":
            # This is where you'd use a list_client to paginate through all season $refs
            # For  simplicity, this part is conceptual.
            # A real implementation would use a list_client with the detail_url as its base
            # or pass this URL to another transformer that IS a lister.
            logger.info(f"Concept: Would list all season refs from {detail_url} and fetch each.")
            logger.warning(
                " Limitation: Full pagination of season list from collection URL not implemented here. Only specific season fetching is complete."
            )
            # To make this work in a limited way for the , if the season_collection_ref
            # itself was a list of $refs (which it isn't, it's a ref to a paginated endpoint),
            # you could iterate here. But it's not.
            # So, this path won't yield data in this simplified  unless season_year_filter is used.

            # Correct approach:
            # 1. season_refs_lister_transformer yields the collection URL.
            # 2. A *new* transformer, say `all_season_refs_paginator_transformer(collection_url_item)`,
            #    would take this, initialize a list_client with collection_url as base,
            #    paginate through "/items" (or similar, if the collection URL is the base for items),
            #    and yield individual season $refs.
            # 3. `season_detail_fetcher_transformer` (this one) would then consume those individual $refs.
            # This  skips step 2 for brevity if no season_year_filter.
            return None  # Placeholder for full collection listing logic

    # --- Placeholder for other top-level transformers ---
    # Example: If league_doc contained a direct $ref to "all_teams_ever"
    # @dlt.transformer(name="all_teams_lister", data_from=league_info_resource)
    # def all_teams_lister_transformer(league_doc: dict[str, Any]):
    #     teams_ref = league_doc.get("all_teams_ever", {}).get("$ref")
    #     if teams_ref:
    #         # yield {"$ref": teams_ref, "type": "team_collection_list", ...}
    #         pass

    # @dlt.transformer(name="team_details", data_from=all_teams_lister_transformer)
    # @dlt.defer
    # def all_team_detail_fetcher_transformer(team_ref_item: dict[str, Any]):
    #     # fetch team details
    #     pass

    # The source yields all defined resources/transformers that dlt should run.
    # Order matters for dependencies if not explicitly linked by data_from in transformers.
    # Here, transformers are explicitly linked.
    return (
        league_info_resource,
        season_refs_lister_transformer,
        season_detail_fetcher_transformer,
        # ... add other top-level listers/fetchers here, e.g., for franchises, awards_root, etc.
        # each taking data_from=league_info_resource
    )


# --- Main execution for local testing ---
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    logger.info("Running dlt  pipeline (root fetch) locally...")

    pipeline = dlt.pipeline(
        pipeline_name="espn_source_pipeline",
        destination="duckdb",
        dataset_name="bronze",
    )

    # Run for a specific season
    source_instance = espn_mens_college_basketball_source_(season_year_filter="2024")

    # Run for all seasons
    # source_instance = espn_mens_college_basketball_source_()

    load_info = pipeline.run(source_instance)

    logger.info("\n--- Load Info ---")
    logger.info(load_info)
    logger.info("-----------------\n")

    if load_info.has_failed_jobs:
        logger.error("Pipeline run failed or had errors.")
    else:
        logger.info("Pipeline run completed successfully.")
