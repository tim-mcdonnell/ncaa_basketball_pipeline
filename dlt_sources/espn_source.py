# dlt_sources/espn_source.py
"""dlt source definition for the ESPN API using RESTClient."""

import re
from collections.abc import Iterable
from pathlib import Path  # Added for finding .env
from typing import Any

import dlt
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# --- (Helper function and source definition remain the same as previous version) ---


# --- Helper Function (Updated with Regex) ---
def extract_id_from_ref(ref_url: str) -> str | None:
    if not ref_url:
        return None
    try:
        path_part = ref_url.split("?")[0]
        cleaned_path = path_part.rstrip("/")
        match = re.search(r"/([0-9]+)$", cleaned_path)
        if match:
            return match.group(1)
        else:
            segments = cleaned_path.split("/")
            last_segment = segments[-1] if segments else None
            if last_segment:
                print(
                    f"EXTRACT_ID_WARNING: Regex /([0-9]+)$ did not find numeric ID in '{cleaned_path}'. Falling back to: '{last_segment}'. Original: '{ref_url}'"
                )
                return last_segment
            else:
                print(
                    f"EXTRACT_ID_WARNING: Could not extract ID from '{cleaned_path}'. Original: '{ref_url}'"
                )
                return None
    except Exception as e:
        print(f"EXTRACT_ID_ERROR: Error for ref_url '{ref_url}': {e}")
        return None


# --- dlt Source Definition ---
@dlt.source(name="espn_api_source", max_table_nesting=0)
def espn_mens_college_basketball_source(
    base_url: str = dlt.config.value,  # Get base_url from config/env: SOURCES__ESPN_API_SOURCE__BASE_URL
) -> Iterable[DltResource]:
    # Check if base_url was successfully loaded from config/env
    if not base_url:
        print("WARNING: Base URL not found in config/env. Using default.")
        base_url = "http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"  # Default fallback

    print(f"DEBUG: Using base_url: {base_url}")

    items_list_paginator = PageNumberPaginator(
        page_param="page", total_path="pageCount", base_page=1, stop_after_empty_page=True
    )
    client_for_items_list = RESTClient(
        base_url=base_url, paginator=items_list_paginator, data_selector="items", headers={}
    )
    client_for_details = RESTClient(
        base_url=None, headers={}
    )  # base_url=None for absolute $ref URLs

    @dlt.resource(name="list_seasons", write_disposition="replace", primary_key="id")
    def list_seasons_resource() -> Iterable[dict[str, Any]]:
        endpoint = "/seasons"
        api_params = {"limit": 50}
        items_yielded_total = 0
        try:
            for page_data in client_for_items_list.paginate(endpoint, params=api_params):
                if not page_data:
                    continue
                for item_ref_obj in page_data:
                    ref_url = item_ref_obj.get("$ref")
                    season_id = extract_id_from_ref(ref_url)
                    if season_id:
                        yield {"id": season_id, "season_ref": ref_url}
                        items_yielded_total += 1
                    else:
                        print(f"LSR_WARNING: Could not extract season_id from $ref: {ref_url}")
            print(
                f"LSR_INFO: Finished list_seasons_resource, yielded {items_yielded_total} seasons."
            )
        except Exception as e:
            print(f"LSR_ERROR: Unexpected error in list_seasons_resource: {e}")
            raise

    @dlt.transformer(
        name="season_details",
        data_from=list_seasons_resource,
        write_disposition="merge",
        primary_key="id",
    )
    def season_details_transformer(season_item: dict[str, Any]) -> dict[str, Any] | None:
        season_id = season_item.get("id")
        if not season_id:
            print(f"SDT_WARNING: Missing season_id in item: {season_item}. Skipping.")
            return None
        endpoint_relative = f"/seasons/{season_id}"
        try:
            response = client_for_items_list.get(endpoint_relative)
            response.raise_for_status()
            season_data = response.json()
            season_data["id"] = str(season_id)
            return season_data
        except requests.exceptions.HTTPError as e:
            print(
                f"SDT_ERROR: HTTPError for season {season_id}: {e}. Response: {e.response.text if e.response else 'No response body'}"
            )
            return None
        except Exception as e:
            print(f"SDT_ERROR: Unexpected error for season {season_id}: {e}")
            return None

    @dlt.transformer(
        name="list_season_types", data_from=list_seasons_resource, write_disposition="replace"
    )
    def list_season_types_transformer(season_item: dict[str, Any]) -> Iterable[dict[str, Any]]:
        season_id = season_item.get("id")
        print(f"LSTT_ENTRY: Processing season_id '{season_id}' for season types.")
        if not season_id:
            print(f"LSTT_WARNING: Missing or invalid season_id in item: {season_item}. Skipping.")
            return
        list_types_endpoint_relative = f"/seasons/{season_id}/types"
        api_params = {"limit": 50}
        items_yielded_for_this_season_id = 0
        pages_processed_for_this_season_id = 0
        try:
            print(
                f"LSTT_DEBUG: Attempting client.paginate for season_id '{season_id}', list endpoint: {base_url}{list_types_endpoint_relative}"
            )
            for page_of_refs in client_for_items_list.paginate(
                list_types_endpoint_relative, params=api_params
            ):
                pages_processed_for_this_season_id += 1
                if not page_of_refs:
                    print(
                        f"LSTT_INFO: Received an empty page of refs for season_id '{season_id}', page {pages_processed_for_this_season_id}."
                    )
                    continue
                print(
                    f"LSTT_DEBUG: Received page {pages_processed_for_this_season_id} with {len(page_of_refs)} refs for season_id '{season_id}'."
                )
                for ref_item in page_of_refs:
                    detail_url = ref_item.get("$ref")
                    if not detail_url:
                        print(
                            f"LSTT_WARNING: Ref item for season_id '{season_id}' is missing '$ref' key. Item: {ref_item}"
                        )
                        continue
                    # print(f"LSTT_DETAIL_FETCH: Fetching season type detail from: {detail_url} (for season_id '{season_id}')") # Can be verbose
                    try:
                        response = client_for_details.get(detail_url)
                        response.raise_for_status()
                        season_type_detail = response.json()
                        season_type_detail["season_id_fk"] = str(season_id)
                        detail_id = season_type_detail.get("id")
                        if detail_id is not None and str(detail_id).strip() != "":
                            # print(f"LSTT_YIELDING: Season type detail with id '{detail_id}' for season_id '{season_id}'.") # Can be verbose
                            yield season_type_detail
                            items_yielded_for_this_season_id += 1
                        else:
                            print(
                                f"LSTT_CRITICAL_SKIP_DETAIL: Fetched season type detail for season_id '{season_id}' from {detail_url} is MISSING 'id' or 'id' is empty. Detail: {season_type_detail}"
                            )
                    except requests.exceptions.HTTPError as he:
                        print(
                            f"LSTT_HTTP_ERROR_DETAIL: Fetching detail from {detail_url} (for season_id '{season_id}'): {he}. Response: {he.response.text if he.response else 'No response body'}"
                        )
                    except Exception as ex:
                        print(
                            f"LSTT_UNEXPECTED_ERROR_DETAIL: Fetching detail from {detail_url} (for season_id '{season_id}'): {ex}"
                        )
            if pages_processed_for_this_season_id == 0:
                print(
                    f"LSTT_INFO: client.paginate yielded NO pages at all for season_id '{season_id}'. Endpoint: {base_url}{list_types_endpoint_relative}"
                )
            print(
                f"LSTT_SUMMARY: For season_id '{season_id}', processed {pages_processed_for_this_season_id} pages, yielded {items_yielded_for_this_season_id} valid items."
            )
        except requests.exceptions.HTTPError as e:
            print(
                f"LSTT_HTTP_ERROR_LIST: Listing types for season_id '{season_id}': {e}. Response: {e.response.text if e.response else 'No response body'}"
            )
        except Exception as e:
            print(f"LSTT_UNEXPECTED_ERROR_LIST: Listing types for season_id '{season_id}': {e}")

    return list_seasons_resource, season_details_transformer, list_season_types_transformer


# --- Standalone Execution Block (Updated) ---
if __name__ == "__main__":
    # Attempt to load .env file from the parent directory (project root)
    try:
        from dotenv import load_dotenv

        # Go up one level from the current script's directory to find the project root
        project_dir = Path(__file__).resolve().parent.parent
        dotenv_path = project_dir / ".env"
        if dotenv_path.exists():
            print(f"Loading .env file from: {dotenv_path}")
            load_dotenv(dotenv_path=dotenv_path, override=True)
        else:
            print(f"Warning: .env file not found at {dotenv_path}")
    except ImportError:
        print("Warning: python-dotenv not installed. Cannot load .env file automatically.")
    except Exception as e:
        print(f"Error loading .env file: {e}")

    # Define pipeline configuration for standalone run
    # It will try to use env vars first (loaded above), then defaults if not set.
    pipeline_name = "espn_api_standalone_test"
    dataset_name = "espn_standalone_data"
    # Destination 'duckdb' will automatically look for
    # DLT_DESTINATION__DUCKDB__CREDENTIALS__DATABASE in env vars.

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",  # Reads path from DLT_DESTINATION__DUCKDB__CREDENTIALS__DATABASE env var
        dataset_name=dataset_name,
    )

    # Verify the database path dlt intends to use
    try:
        # Accessing the resolved credentials can vary slightly by dlt version
        # This attempts a common way
        resolved_creds = pipeline.destination.config_params
        db_path = (
            resolved_creds.database
            if resolved_creds and hasattr(resolved_creds, "database")
            else None
        )
        if db_path:
            # Ensure the parent directory exists
            db_parent_dir = Path(db_path).parent
            print(f"Ensuring directory exists: {db_parent_dir}")
            db_parent_dir.mkdir(parents=True, exist_ok=True)
            print(f"Standalone run will use DuckDB path: {db_path}")
        else:
            print("Warning: Could not resolve DuckDB path from dlt configuration/environment.")
            print("dlt might default to an in-memory DB or use a default file path.")
    except Exception as e:
        print(f"Error trying to determine DuckDB path: {e}")

    print(
        f"Running dlt pipeline locally for source '{pipeline.pipeline_name}' -> dataset '{pipeline.dataset_name}'..."
    )
    # The source function now reads its base_url from config/env
    source_instance = espn_mens_college_basketball_source()
    load_info = pipeline.run(source_instance)
    print("\n--- Load Info ---")
    print(load_info)
    print("-----------------\n")

    # Inspect the data directly after local run
    if load_info.has_failed_jobs:
        print("Pipeline run failed or had errors.")
    else:
        print("Pipeline run successful. Inspecting data...")
        import duckdb

        # Use the same path resolution logic as above
        db_path_inspect = None
        try:
            resolved_creds_inspect = pipeline.destination.configuration_credentials()
            db_path_inspect = (
                resolved_creds_inspect.database
                if resolved_creds_inspect and hasattr(resolved_creds_inspect, "database")
                else None
            )
        except Exception:
            print("Could not get DB path from destination object for inspection.")

        if db_path_inspect:
            if not Path(db_path_inspect).exists():
                print(f"ERROR: DuckDB file not found at the expected path: {db_path_inspect}")
            else:
                try:
                    conn = duckdb.connect(database=db_path_inspect, read_only=True)
                    print(f"\nConnected to DuckDB at {db_path_inspect}")
                    print(f"Tables created by dlt in dataset '{pipeline.dataset_name}':")
                    tables = conn.execute(
                        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{pipeline.dataset_name}';"
                    ).fetchall()
                    if tables:
                        for table_tuple in tables:
                            table_name = table_tuple[0]
                            print(f"\n--- Table: {pipeline.dataset_name}.{table_name} ---")
                            print(
                                conn.sql(
                                    f'SELECT * FROM "{pipeline.dataset_name}"."{table_name}" LIMIT 3;'
                                ).df()
                            )
                    else:
                        print(f"No tables found in the dataset '{pipeline.dataset_name}'.")
                    conn.close()
                except Exception as e:
                    print(f"Error connecting/querying DuckDB at '{db_path_inspect}': {e}")
        else:
            print(
                "Could not determine DuckDB database path from pipeline configuration for local inspection."
            )
