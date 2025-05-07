# Product Requirements Document: NCAA Basketball Data Pipeline - Bronze Layer with Dagster and dlt

**Version:** 2.4
**Date:** May 7, 2025
**Status:** Draft

# 1. Overview

This project aims to build a robust and scalable data pipeline focused on the Bronze layer of NCAA Men's College Basketball data. The primary objective is to efficiently fetch comprehensive data spanning potentially 20+ years from the publicly available ESPN REST API. This will be achieved by leveraging dlt (data load tool) for data extraction and loading, deeply integrated and orchestrated as software-defined assets within the Dagster framework using the dagster-dlt library. The data will be persistently stored in a DuckDB database named `ncaa_basketball.duckdb`, specifically within a `bronze` schema.

This PRD mandates a specific architectural approach where dlt sources are defined as Python functions and then materialized as Dagster assets via the `@dlt_assets` decorator. Configuration and secrets management will adhere to the practices recommended for the dagster-dlt integration, primarily using environment variables and direct parameterization within Dagster asset definitions, rather than standalone dlt project files.

# 2. Goals

- **Establish a Scalable Bronze Layer:** Implement an efficient process for ingesting raw NCAA Men's College Basketball data from the ESPN API into the `bronze` schema of a `ncaa_basketball.duckdb` database using dlt orchestrated by Dagster.
- **Leverage dlt for Extraction and Loading:** Utilize dlt's Python-native capabilities for defining data sources (`@dlt.source`, `@dlt.resource`), handling API interaction, schema inference, and loading.
- **Prescriptive Dagster-dlt Integration:** Strictly use the `@dlt_assets` decorator from `dagster-dlt` for defining assets. Configuration of dlt pipelines (name, destination, dataset) will occur within these asset definitions.
- **Centralized Configuration via Dagster:** Manage dlt source configurations (e.g., API URLs) and secrets (if any were needed) through Dagster's environment variable management, accessible within dlt source code via `dlt.config.value` and `dlt.secrets.value`.
- **Architect for Maintainability and Clarity:** Define a clear project structure aligned with `dagster-dlt` best practices.
- **Ensure Idempotency and Data Integrity:** Design ingestion for idempotency using dlt's features.
- **Support Backfilling & Incremental Loads:** Utilize Dagster's partitioning with dlt sources designed to accept partition keys.
- **Provide Observability:** Use the Dagster UI (Dagit) for monitoring.

# 3. Non-Goals

- Implementing Silver or Gold data layers (data cleaning, transformation, aggregation) within the scope of this specific PRD.
- Building a real-time streaming pipeline.
- Developing a user-facing application beyond the Dagster UI.
- Implementing complex machine learning models based directly on Bronze layer data.
- Using standalone dlt project structures (e.g., `.dlt` folder, `config.toml`, `secrets.toml` at the project root for pipeline execution). All dlt operations will be orchestrated via Dagster.

# 4. Core Architecture: Prescriptive dagster-dlt Integration

The architecture mandates using the `dagster-dlt` library for a tight integration between Dagster and dlt. dlt sources will be Python modules, and their materialization and configuration as assets will be handled exclusively through Dagster.

## 4.1. dlt Source Definition

- **Location:** dlt source logic will reside in Python files within a dedicated directory, e.g., `ncaa_basketball_pipeline/dlt_sources/`.
- **Structure:** Each source will be defined using `@dlt.source` and will contain one or more `@dlt.resource` functions. These functions will encapsulate the logic for fetching data from specific ESPN API endpoints.
  - API interactions (requests, pagination, `$ref` handling) will be coded within these resource functions.
  - Source-specific configurations (e.g., base API URL) should be accessed within the `@dlt.source` function using `dlt.config.value` (e.g., `espn_api_base_url: str = dlt.config.value`). These values are expected to be set via environment variables that Dagster makes available.
  - Secrets (though not required for this specific public ESPN API) would be accessed using `dlt.secrets.value` (e.g., `api_key: str = dlt.secrets.value`).

## 4.2. Dagster Asset Definition with `@dlt_assets`

- **Location:** Dagster asset definitions using dlt will be in a Dagster-specific module, e.g., `ncaa_basketball_pipeline/dagster_bronze_layer/assets.py`.
- **`@dlt_assets` Decorator:** This is the sole method for defining Dagster assets from dlt sources.
  - It takes a `dlt_source` parameter, which will be an imported instance of a `@dlt.source`-decorated function (e.g., `dlt_source=espn_api_source()`).
  - It takes a `dlt_pipeline` parameter, which is an instance of `dlt.pipeline(...)` configured directly in the decorator. This is where `pipeline_name`, `dataset_name` (targeting the `bronze` schema), and `destination` ("duckdb") are explicitly set.

Example in `dagster_bronze_layer/assets.py`:
```python
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt # dlt core library
from ..dlt_sources.espn_api_source import espn_ncaa_basketball_source # Assuming source defined here

@dlt_assets(
    dlt_source=espn_ncaa_basketball_source(), # Invoking the @dlt.source function
    dlt_pipeline=dlt.pipeline(
        pipeline_name="espn_bronze_pipeline",
        dataset_name="bronze", # This will be the schema in DuckDB
        destination="duckdb",
        progress="log" # Or other dlt progress indicators
    ),
    name="bronze_espn_assets", # Name for the Dagster multi-asset
    group_name="bronze_layer"
)
def espn_bronze_assets_definition(context: AssetExecutionContext, dlt_resource: DagsterDltResource):
    yield from dlt_resource.run(context=context)
```

- **`DagsterDltResource`**: A `DagsterDltResource` instance must be defined in the Dagster `definitions.py` file (e.g., at `ncaa_basketball_pipeline/dagster_bronze_layer/definitions.py`) and provided to the `Definitions` object. This resource is then injected into the function decorated by `@dlt_assets` (e.g., `dlt_resource: DagsterDltResource` argument) and used to execute the dlt pipeline run (`yield from dlt_resource.run(...)`).

## 4.3. Configuration and Secrets Management (Prescriptive)

- **No `.dlt` Project Folder:** The project will not contain a `.dlt/` folder at its root.
- **No `config.toml` or `secrets.toml`:** Standalone dlt `config.toml` and `secrets.toml` files are not used for pipeline configuration when integrated with Dagster in this manner.
- **Configuration via Environment Variables:**
  - All dlt-level configurations (for sources, destinations, etc.) that are not directly parameterized in `dlt.pipeline(...)` will be managed via environment variables. Dagster is responsible for making these environment variables available to the execution environment.
  - dlt automatically picks up environment variables following specific naming conventions:
    - For sources: `SOURCES__<SOURCE_NAME>__<CONFIG_KEY>` (e.g., `SOURCES__ESPN_API_SOURCE__BASE_URL="http://..."`). The `<SOURCE_NAME>` should match how dlt internally recognizes the source, often derived from the function name or explicit naming.
    - For destinations: `DESTINATION__<DESTINATION_NAME>__CREDENTIALS__<KEY>` (e.g., `DESTINATION__DUCKDB__CREDENTIALS__DATABASE="ncaa_basketball.duckdb"`).
  - These environment variables will be accessed within the `@dlt.source` Python code using `dlt.config.value` for general configurations and `dlt.secrets.value` for sensitive values.
- **Pipeline Definition in Code:** Core dlt pipeline settings (`pipeline_name`, `dataset_name`, `destination`) are explicitly defined in Python code within the `@dlt_assets` decorator's `dlt_pipeline` argument, as shown in section 4.2.

## 4.4. File and Folder Structure Strategy (Prescriptive)

```
ncaa_basketball_pipeline/
├── dlt_sources/              # Directory for dlt source definitions
│   ├── __init__.py
│   └── espn_api_source.py    # Defines @dlt.source for ESPN API
│
├── dagster_bronze_layer/     # Dagster-specific code for the Bronze layer
│   ├── __init__.py
│   ├── assets.py             # Defines @dlt_assets using sources from dlt_sources
│   └── definitions.py        # Dagster Definitions object, including DagsterDltResource
│
├── notebooks/                # Jupyter notebooks for exploration
│
├── tests/                    # Unit and integration tests
│   ├── dlt_sources/
│   └── dagster_bronze_layer/
│
├── .env                      # Optional: For local development environment variables
├── dagster.yaml              # Dagster instance configuration
├── pyproject.toml            # Python project configuration (dependencies: dagster, dagster-dlt, dlt)
└── README.md                 # Project overview
```

This structure separates dlt source logic from Dagster asset definitions, promoting clarity. It explicitly omits a top-level `.dlt` folder.

## 4.5. Data Storage (Bronze Layer in DuckDB)

- **Database:** `ncaa_basketball.duckdb`.
- **Schema:** All data loaded by dlt will reside within the `bronze` schema. This is configured via the `dataset_name="bronze"` parameter in `dlt.pipeline(...)`.

# 5. Conceptual Data Model (Bronze Layer)

While dlt will dynamically infer and manage schemas, the Bronze layer is expected to contain tables corresponding to the main entities from the ESPN API. These tables will store data as close to the raw source format as possible. Key conceptual entities (which will likely translate to tables or groups of related tables) include:

- **Seasons:** Details for each basketball season (e.g., `bronze.seasons`).
- **Season Types:** Types within a season (e.g., regular, postseason) (e.g., `bronze.season_types`).
- **Weeks:** Weekly breakdown within a season type (e.g., `bronze.weeks`).
- **Events (Games):** Core game information, including participating teams, date, time, venue references (e.g., `bronze.events`).
- **Competitors:** Details about teams participating in an event (e.g., `bronze.event_competitors`).
- **Scores:** Final scores and potentially period-specific scores (linescores) for each competitor in an event (e.g., `bronze.scores`, `bronze.linescores`).
- **Statistics (Team & Player):** Aggregated team statistics and individual player statistics per game, often broken down by statistical categories (e.g., `bronze.team_statistics`, `bronze.player_statistics`).
- **Rosters:** Player rosters for each team in a specific game (e.g., `bronze.rosters`).
- **Play-by-Play:** Detailed log of game events (e.g., `bronze.plays`).
- **Venues:** Information about game locations (e.g., `bronze.venues`).
- **Team Details:** General team information (e.g., `bronze.teams`).
- **Records:** Team win/loss records at various points (e.g., `bronze.records`).
- **Leaders:** Top player performances in key statistical categories per game (e.g., `bronze.leaders`).
- (Other entities as discovered and deemed valuable from the API inventory, e.g., Odds, Situation, Status)

Each table will have primary keys, often composite, derived from the API structure (e.g., `event_id`, `team_id`, `season_id`) to ensure data integrity and support idempotent loads. dlt's load IDs (`_dlt_load_id`, `_dlt_id`) will also be present for lineage and debugging.

# 6. Data Freshness and Latency

- **Historical Data:** The initial load will be a backfill of all available historical seasons (approx. 20+ years). This is a one-time, intensive operation.
- **Ongoing Updates:** During active NCAA Men's College Basketball seasons, data for events should be updated daily. The goal is to have data for completed games available in the Bronze layer within 24 hours of game completion.
- **Non-Active Periods:** During the off-season, pipeline runs may be less frequent (e.g., weekly) to check for any rare updates or ensure system health, unless specific static data updates are expected.
- **Scheduling:** Dagster schedules will be configured to achieve these freshness targets (e.g., a daily schedule during the season).

# 7. Testing Strategy (High-Level)

A multi-layered testing approach will be crucial for ensuring the reliability and accuracy of the Bronze layer:

- **dlt Source/Resource Tests (Unit Tests):**
  - Mock API responses to test the parsing logic within dlt source functions.
  - Verify correct handling of pagination, `$ref` link processing, and extraction of key fields.
  - Test edge cases and potential error conditions from the API.
- **dlt Pipeline Tests (Integration Tests via Dagster Assets):**
  - Materialize `@dlt_assets` against a controlled subset of live (or cached static) API data.
  - Verify that data is correctly loaded into a test DuckDB instance with the expected schema (table names, key columns present in the `bronze` schema).
  - Check primary key constraints and write dispositions (merge/replace) are working as intended.
- **Dagster Asset Tests:**
  - Ensure that `@dlt_assets` definitions correctly invoke the dlt pipelines.
  - Verify that Dagster correctly tracks materializations and metadata.
- **Data Validation (Post-Load):**
  - Implement basic data validation checks, potentially as downstream Dagster assets.
  - Examples: Check for non-null primary keys, approximate row counts for known entities, date range sanity checks.
  - Verify that dlt's schema evolution handles API changes gracefully or alerts if breaking changes occur.

# 8. Key Risks and Mitigation Strategies

- **API Changes/Instability:**
  - **Risk:** The ESPN API is not officially versioned for public stability; endpoints or data structures could change without notice, breaking the pipeline.
  - **Mitigation:**
    - Implement robust error handling and alerting within dlt sources and Dagster assets.
    - dlt's schema evolution capabilities can handle non-breaking changes.
    - Regularly monitor pipeline runs. Have a process for quickly identifying and adapting to breaking API changes.
    - Maintain the API Endpoint Inventory document.
- **Rate Limiting:**
  - **Risk:** Aggressive data fetching, especially during backfills, might trigger unannounced rate limits.
  - **Mitigation:**
    - Utilize dlt's built-in retry and backoff mechanisms (configurable if needed).
    - Make request delays configurable in dlt sources if necessary.
    - Design backfill strategies to process data in manageable chunks (e.g., season by season with potential delays).
    - Monitor for 429 errors.
- **Unexpected Data Structures / Edge Cases:**
  - **Risk:** API might return data in formats not encountered during initial development, leading to parsing errors.
  - **Mitigation:**
    - Thorough exploratory data analysis during dlt source development.
    - Implement flexible parsing logic and comprehensive error logging for individual items.
    - dlt's ability to load variant records can help capture unexpected structures for later analysis.
- **Scalability for Backfills:**
  - **Risk:** The initial backfill of 20+ years of data could be time-consuming or resource-intensive.
  - **Mitigation:**
    - Leverage Dagster's partitioning for breaking down the backfill into smaller, manageable jobs (e.g., by season).
    - Optimize dlt resource implementation for efficient data fetching and processing.
    - Monitor resource usage during initial large backfills.
- **Complexity of `$ref` Traversal:**
  - **Risk:** Deeply nested or circular `$ref` links could lead to overly complex or inefficient data fetching.
  - **Mitigation:**
    - Carefully design dlt resources to manage the depth of `$ref` following.
    - Implement safeguards against excessive recursion if necessary.
    - Prioritize fetching essential linked data.

# 9. Partitioning and Scheduling

Dagster's partitioning will be applied to `@dlt_assets` to manage historical backfills (e.g., by season) and incremental loads (e.g., daily for new game data based on event dates).

dlt sources will be designed to accept parameters (e.g., season, date range) from Dagster partitions to fetch specific data slices. This is achieved by passing the `context.partition_key` to the `dlt_source` function when calling `dlt_resource.run()` within the `@dlt_assets` decorated function.

Example in `dagster_bronze_layer/assets.py`:
```python
from dagster import AssetExecutionContext, DailyPartitionsDefinition, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from typing import Optional # For type hinting in the source
# Assuming espn_ncaa_basketball_source is in dlt_sources/espn_api_source.py
# and is defined like: @dlt.source def espn_ncaa_basketball_source(partition_date: Optional[str] = None, ...):
from ..dlt_sources.espn_api_source import espn_ncaa_basketball_source

daily_partitions = DailyPartitionsDefinition(start_date="2023-11-01")

@dlt_assets(
    dlt_pipeline=dlt.pipeline(
        pipeline_name="espn_bronze_partitioned_pipeline",
        dataset_name="bronze",
        destination="duckdb"
    ),
    partitions_def=daily_partitions,
    name="bronze_espn_partitioned_assets",
    group_name="bronze_layer"
    # dlt_source is provided in the run method for partitioned assets
)
def espn_bronze_partitioned_assets_definition(context: AssetExecutionContext, dlt_resource: DagsterDltResource):
    partition_key_str = context.partition_key
    # Pass the partition key to the source function
    yield from dlt_resource.run(
        context=context,
        dlt_source=espn_ncaa_basketball_source(partition_date=partition_key_str)
    )
```

Dagster schedules will trigger the materialization of these partitioned assets according to the freshness requirements outlined in Section 6.

# 10. Idempotency and Data Integrity

dlt pipelines will use appropriate `write_disposition` (e.g., "merge" or "replace") and defined `primary_keys` for each resource (defined within the `@dlt.resource` decorators in the Python source files) to ensure idempotency.

Dagster will manage the orchestration of these idempotent assets.

# 11. Observability & Monitoring

The Dagster UI (Dagit) will provide primary monitoring for Bronze layer assets, including run history, logs, and materialization metadata.

Alerting will be configured in Dagster for pipeline failures or significant data anomalies detected by validation steps.

# 12. Future Considerations (Beyond Bronze Layer)

While this PRD is strictly focused on establishing the Bronze layer, the overall architectural vision anticipates subsequent Silver and Gold layers, all orchestrated by Dagster and utilizing DuckDB as the primary data store for consistency and simplicity in this project. Data lineage across all layers is a paramount concern and a key reason for choosing Dagster for end-to-end orchestration.

## Silver Layer (Cleansed and Normalized Data):

- **Objective:** Transform raw Bronze data from `bronze` schema in DuckDB into cleaned, normalized, and integrated datasets in a `silver` schema within the same DuckDB database.
- **Potential Technologies:**
  - Transformations will be defined as Dagster assets.
  - Python (with Pandas/Polars for in-memory processing) and/or direct SQL transformations executed by `dagster-duckdb` against DuckDB are primary candidates.
  - `dagster-spark` could be considered if data volumes unexpectedly outgrow DuckDB's single-node capabilities, but the initial plan is to remain within the DuckDB ecosystem.
- **Storage:** Tables within the `silver` schema in `ncaa_basketball.duckdb`.

## Gold Layer (Business-Ready Data & Features):

- **Objective:** Create aggregated, de-normalized datasets for business intelligence, reporting, and features for machine learning models, stored in a `gold` schema in DuckDB.
- **Potential Technologies:**
  - `dbt` (via `dagster-dbt`): Strongly considered for SQL-based transformations, aggregations, and business logic, with dbt models managed as Dagster assets, reading from the `silver` schema and writing to the `gold` schema in DuckDB.
  - Python-based Feature Engineering: Using libraries like Scikit-learn, Feature-engine, or potentially Featuretools within Dagster assets for creating ML features, reading from `silver` or `gold` DuckDB tables.
- **Storage:** Tables within the `gold` schema in `ncaa_basketball.duckdb`.

## Data Lineage and Automation:

- A core principle is to leverage Dagster's asset graph to maintain end-to-end data lineage from Bronze through Silver and Gold schemas in DuckDB, and into any downstream ML models or BI dashboards.
- Dagster's scheduling and sensor capabilities will be used to automate refreshes and updates across layers based on changes in upstream dependencies.

## Iterative PRD Development:

This PRD will be revisited and potentially new, layer-specific PRDs will be authored as the project progresses and requirements for Silver and Gold layers are further refined.
