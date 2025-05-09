# Requirements for Refactoring `espn_source.py`

**Version:** 1.0 **Date:** 2025-05-08 **Project:** ESPN NCAA Men's Basketball Data Pipeline **Author:** [Your Name/Team]

## 1. Objective

The primary objective of this refactoring task is to overhaul the existing `espn_source.py` file to create a robust,
scalable, and maintainable `dlt` (Data Load Tool) source definition. This refactored source will comprehensively extract
data from the ESPN NCAA Men's College Basketball API, aligning with the detailed structure and patterns identified in
the "ESPN API Endpoint Inventory for `dlt` Source (v2)" document.

The refactored source should:

- Cover all relevant endpoints as cataloged in the inventory.
- Implement a logical data flow mirroring the API's hierarchy.
- Employ best practices for `dlt` resource and transformer design.
- Incorporate efficient concurrency for fetching detailed data.
- Ensure robust error handling and logging.
- Maintain clear foreign key relationships for relational integrity in the destination (DuckDB).

## 2. Key Reference Documents & Resources

The developer **must** familiarize themselves with and continuously reference the following:

1. **ESPN API Endpoint Inventory for `dlt` Source (v2):** (ID: `espn-api-inventory-v2`) This is the primary guide for
   understanding API endpoints, data structures, `$ref` patterns, suggested primary/foreign keys, and the overall data
   discovery flow.
2. **Existing `espn_source.py`:** Provides a starting point and context for the current implementation approach.
3. **`repomix-output-tim-mcdonnell-ncaa_basketball_pipeline-2.xml`:** Contains sample API responses for every discovered
   endpoint. Use this to verify data structures and field names.
4. **`dlt` Documentation:** (Provided as `dlt_documentation.xml`) For understanding `dlt` concepts, decorators
   (`@dlt.source`, `@dlt.resource`, `@dlt.transformer`), client usage, pagination, and state management.
5. **`assets.py` (Dagster Integration):** To understand how the `dlt` source is invoked and partitioned within the
   Dagster orchestration framework. Note that the `espn_source.py` file itself should remain focused on `dlt` logic.

## 3. Core Architectural Principles

The refactored source must adhere to the following architectural principles:

- **Hierarchical Structure:** The `dlt` resources and transformers should mirror the API's hierarchical data flow
  (Seasons -> Types -> Weeks -> Events -> Event Details -> Master Data) as outlined in the API Inventory.
- **`$ref` Handling:**
  - List endpoints will typically yield items containing `$ref` URLs.
  - Child transformers will consume these `$ref`s (or the parent data item containing them) to fetch detailed data.
- **Client Configuration:**
  - **List Client:** A `dlt.sources.helpers.rest_client.RESTClient` instance configured with the `base_url`, a
    `PageNumberPaginator` (page_param="page", total_path="pageCount", base_page=1, stop_after_empty_page=True), and
    `data_selector="items"` (or appropriate selector) for endpoints that list `$ref` items.
  - **Detail Client:** A separate `RESTClient` instance with `base_url=None` (or the root API URL if all `$ref`s are
    relative, though they appear absolute) for fetching details from the absolute `$ref` URLs.
- **Modularity:** Each `dlt` resource and transformer should have a clear, single responsibility (e.g., fetching
  seasons, transforming season data to fetch types, fetching event details).

## 4. Specific Implementation Requirements

### 4.1. Endpoint Coverage & Data Flow

- Implement `dlt` resources and transformers to cover all endpoints listed in the "Core Resources" and "Event
  Sub-Resources" sections of the API Inventory.
- Implement `dlt` resources/transformers for key "Master / Dimension Tables" (Teams, Athletes, Venues, Positions). The
  strategy for discovering/fetching these (e.g., dedicated resources vs. triggered by `$ref`s from event data) should
  align with the inventory's suggestions.
- The flow of data between transformers must follow the parent-child relationships defined in the inventory (e.g.,
  `seasons_resource` yields data for `season_types_transformer`, which yields for `weeks_transformer`, etc.).

### 4.2. `dlt` Source, Resources, and Transformers

- The main source function should be decorated with `@dlt.source(name="espn_source", max_table_nesting=0)`.
- It should accept `season_year` as an optional parameter to allow processing for specific seasons (as currently
  implemented for Dagster partitioning).
- Use `@dlt.resource` for functions that initiate data fetching (e.g., fetching the initial list of seasons).
- Use `@dlt.transformer` for functions that process data yielded by a parent resource/transformer and fetch further
  related data.
- Transformers should explicitly define their `data_from` parameter pointing to the parent resource.

### 4.3. Data Yielding and Foreign Keys

- Transformers fetching detailed data from `$ref` URLs must yield complete data records for their respective tables.
- **Crucially, ensure all necessary foreign key fields (suffixed with `_fk`) are added to the yielded data items.**
  These foreign keys link child records back to their parent tables (e.g., an event record should have `week_id_fk`,
  `type_id_fk`, `season_id_fk`).
- All ID fields (primary and foreign keys) should be stored as **strings**.
- Pass down parent identifiers through the transformer chain to construct these foreign keys.

### 4.4. Concurrency and Configuration

- **Primary Concurrency Strategy:** Employ `@dlt.defer` for transformers that perform I/O-bound operations (primarily
  API calls for detail fetching) for each item received from a parent. This allows `dlt` to manage a global thread pool
  for concurrent execution.
  - The size of this global pool is configurable via the environment variable `DLT_RUNTIME__PARALLEL_POOL_MAX_WORKERS`
    (e.g., `DLT_RUNTIME__PARALLEL_POOL_MAX_WORKERS=20`) in the `.env` file.
- **Refactoring `events_transformer`:** The existing `events_transformer` (which uses a `ThreadPoolExecutor`) will be
  refactored into:
  - `event_refs_lister_transformer`: Lists event references for a week. Could be `@dlt.defer`-ed if listing refs for a
    single week is itself a slow, multi-page operation.
  - `event_detail_fetcher_transformer`: Fetches details for each event reference using `@dlt.defer`. The internal
    `ThreadPoolExecutor` in the original `events_transformer` will be removed.
- **Custom `ThreadPoolExecutor` (Use Sparingly):** For specific cases where a transformer needs to manage a batch of its
  own concurrent tasks internally _before_ yielding items to a subsequent `@dlt.defer`-ed transformer (e.g., if a list
  endpoint itself is very slow and yields many items that then need immediate, batched pre-processing), a custom
  `ThreadPoolExecutor` might be considered.
  - If used, its `max_workers` should be configurable via an environment variable, e.g.,
    `DLT_SOURCES__ESPN_SOURCE__MAX_SPECIFIC_WORKERS=5`, accessed in the code via
    `dlt.config.get("sources.espn_source.max_specific_workers", 5)`. This allows tuning specific parts of the pipeline
    if necessary.
- **Error Handling:** Wrap all I/O operations within `try...except` blocks to handle failures gracefully, log errors,
  and allow the pipeline to continue with other items.
- **Environment Variables:** All `dlt`-related concurrency configurations will be managed via environment variables in
  the `.env` file, which `dlt` and `python-dotenv` will pick up.

### 4.5. Pagination

- Utilize the `PageNumberPaginator` for list endpoints that are paginated, as configured in the "List Client".
- Use `limit=API_LIMIT` (e.g., `API_LIMIT = 1000`) in API requests to list endpoints to minimize the number of calls.

### 4.6. Error Handling and Logging

- Implement comprehensive `try...except` blocks around all API calls (both list and detail fetches).
- Log errors clearly, including the URL/endpoint being accessed, any relevant IDs (season, event, etc.), and the error
  message. Use the standard Python `logging` module.
- For HTTP errors, log the status code and response body if available.
- Distinguish between transient errors (which `dlt`'s `requests` helper might retry) and permanent errors.
- If a detail fetch for a specific item fails after retries, log the failure and continue processing other items. Do not
  let a single item failure stop an entire batch.

### 4.7. Configuration and Constants

- Define constants for `API_LIMIT`.
- The `base_url` is passed into the source function from `dlt.config.value` (set via `DLT_BASE_URL` in `.env` if not
  using the default passed by `dlt.config.value` directly from a secrets/config file structure that `dlt` normally uses.
  For `.env` driven configuration, ensure `base_url: str = dlt.config.value` in the source function signature correctly
  picks up `DLT_BASE_URL` or a more specific config like `DLT_SOURCES__ESPN_SOURCE__BASE_URL`).
- Concurrency parameters (e.g., `DLT_RUNTIME__PARALLEL_POOL_MAX_WORKERS`, and any custom max_workers like
  `DLT_SOURCES__ESPN_SOURCE__MAX_CONCURRENT_EVENT_FETCHES` if still used for a specific `ThreadPoolExecutor`, or new
  ones like `DLT_SOURCES__ESPN_SOURCE__MAX_SPECIFIC_WORKERS`) will be managed via environment variables in the `.env`
  file and accessed via `dlt.config.get("path.to.setting", default_value)` or automatically used by `dlt` for its own
  settings.
- Ensure the `base_url` is correctly utilized by the list client.

### 4.8. Code Quality

- Write clean, readable, and well-commented Python code.
- Add docstrings to all functions and classes explaining their purpose, arguments, and what they yield/return.
- Structure the file logically, grouping related resources and transformers.
- Adhere to PEP 8 Python style guidelines.

## 5. Dagster Integration Context

- The `espn_source.py` file defines the `dlt` source logic.
- Dagster (`assets.py`) is responsible for:
  - Orchestrating the pipeline runs.
  - Managing partitioning (currently by `season_year`). The `espn_mens_college_basketball_source` function will receive
    the `season_year` partition key.
  - Configuring the `dlt` pipeline (destination, dataset name).
- The refactoring should focus on the `dlt` source definition. No changes to `assets.py` are required unless a change in
  source function signature necessitates it.

## 6. Testing

- Thoroughly test the refactored source:
  - Test individual resources/transformers in isolation where possible (e.g., by providing mock parent data).
  - Test the end-to-end pipeline for a single season.
  - Verify that all expected tables are created in DuckDB.
  - Verify that data is correctly populated and foreign key relationships are intact.
  - Check for any unintended duplication of data.
  - Monitor logs for errors or unexpected behavior.

## 7. Non-Goals for This Refactoring Iteration (Focus Areas)

- **Complex Incremental Logic:** While `dlt` supports incremental loading, the primary goal of this refactor is to build
  out the full, comprehensive extraction for all specified endpoints. Basic state management for resuming after
  interruption is good, but advanced logic for delta loads based on timestamps can be a subsequent enhancement if not
  easily integrated now.
- **Major Changes to Dagster Partitioning Strategy:** The current season-based partitioning is sufficient.

By following these requirements and leveraging the provided documentation, the refactored `espn_source.py` will be a
significant improvement, enabling reliable and comprehensive data ingestion from the ESPN API.
