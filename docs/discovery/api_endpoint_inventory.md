# ESPN API Endpoint Inventory for `dlt` Source (v2)

**Version:** 2.0
**Date:** 2025-05-08
**Purpose:** This document catalogs the ESPN API endpoints for the NCAA Men's College Basketball league (`mens-college-basketball`) and provides detailed guidance for implementing a `dlt` (Data Load Tool) source to ingest this data, based on the patterns observed in `espn_source.py` and comprehensive API discovery.

**Base URL:** `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball`

---

## General `dlt` Implementation Guidelines

Based on `espn_source.py` and API structure:

1.  **`$ref` Handling:** The API extensively uses `$ref` fields.
    * Parent `dlt` resources/transformers will typically list these references (often under an `"items"` key).
    * For fetching details from these `$ref`s, a two-transformer pattern is often preferred:
        * A "Lister" transformer: Takes a parent ID/object, makes the paginated API call to list child `$ref`s, and yields *each individual child `$ref` object* (augmented with foreign keys).
        * A "Detail Fetcher" transformer: Takes an individual child `$ref` object from the "Lister", is decorated with `@dlt.defer`, and fetches the details from the absolute URL in the `$ref`.
2.  **Client Configuration:**
    * **List Client:** `RESTClient` with `base_url`, `PageNumberPaginator`, and `data_selector="items"`.
    * **Detail Client:** `RESTClient` with `base_url=None` for absolute `$ref` URLs.
3.  **Primary Keys & Foreign Keys:**
    * Composite primary keys are common.
    * Establish clear foreign key relationships (`_fk` suffix).
    * Store all ID fields as **strings**.
4.  **Pagination:** List endpoints are paginated. Use `limit=API_LIMIT` (e.g., 1000). The "Lister" transformer (see point 1) will handle iterating through pages of `$ref`s.
5.  **Data Structure:** Use `max_table_nesting=0` in `@dlt.source`.
6.  **Concurrency:**
    * **Prefer `@dlt.defer`:** For any transformer that takes a single input item (like a `$ref` object) and needs to make an API call to fetch its details, use the `@dlt.defer` decorator. This allows `dlt` to manage a thread pool for these operations.
    * **Manual `ThreadPoolExecutor`:** May still be useful in specific, complex scenarios within a single transformer if `@dlt.defer` is not a natural fit, but aim to break down tasks to leverage `@dlt.defer` where possible.
7.  **Error Handling:** Robust `try...except` blocks within all API call logic, especially in `@dlt.defer`-decorated functions. Log errors and allow the pipeline to continue with other items.
8.  **Incremental Loading:** Utilize `dlt` state for incremental loads (e.g., last fetched date, processed event IDs).
9.  **API Path Structure Anomaly:** Reminder: `event_id` often serves as `competition_id` in event sub-resource paths.
10. **Resource Naming:** Descriptive names (e.g., `seasons_resource`, `season_types_lister_transformer`, `season_type_detail_fetcher_transformer`).

---

## Concurrency Management with `dlt` and Environment Variables

When implementing `dlt` sources, especially those involving numerous API calls, managing concurrency is vital for performance. `dlt` provides several mechanisms for this:

1.  **`@dlt.defer` Decorator:**
    *   This is the preferred method for transformers that perform I/O operations (like an API call) for each item they process from a parent resource.
    *   `dlt` manages a global thread pool to execute these deferred functions concurrently.
    *   The size of this global pool can be configured using the environment variable `DLT_RUNTIME__PARALLEL_POOL_MAX_WORKERS`. For example:
        `DLT_RUNTIME__PARALLEL_POOL_MAX_WORKERS=20`
    *   If this is not set, `dlt` uses a default (often related to `os.cpu_count()`).

2.  **Custom `ThreadPoolExecutor`:**
    *   For more complex scenarios where a single transformer needs to manage a batch of concurrent operations internally (e.g., fetching all pages of a sub-list before yielding combined results, or processing a batch of URLs gathered within one transformer's iteration), a `ThreadPoolExecutor` can be used directly.
    *   The `max_workers` for such custom executors should also be configurable. This can be achieved using `dlt.config.get()` within the source code, which can read from environment variables. For example, if a setting is named `sources.espn_source.max_concurrent_detail_fetches`, it can be set via the environment variable:
        `DLT_SOURCES__ESPN_SOURCE__MAX_CONCURRENT_DETAIL_FETCHES=15`
    *   It's generally advisable to favor `@dlt.defer` where possible to allow `dlt` to manage the overall concurrency, preventing issues like overly nested thread pools.

3.  **Configuration in `.env`:**
    *   Since this project is orchestrated by Dagster and uses a `.env` file for configuration (loaded by `python-dotenv`), all `dlt` configuration parameters should be set as environment variables in the `.env` file.
    *   `dlt` automatically picks up environment variables prefixed with `DLT_` and structured according to its configuration hierarchy (e.g., `PIPELINES__MY_PIPELINE__DESTINATION` maps to `pipelines.my_pipeline.destination` in a config file).

Careful tuning of these concurrency settings is necessary to balance performance with API rate limits and system resources.

---

## Core Resources (Sequential Discovery Flow)

This section follows the typical data discovery path, starting from seasons and drilling down to event details.

### 1. Seasons

-   **`dlt` Resource Name:** `seasons_resource`
-   **Description:** Fetches the list of available seasons and their details. Can be filtered to a specific `season_year`.
-   **Endpoint Path (List):** `/seasons`
-   **Endpoint Path (Detail):** `/seasons/{season_id}` (from `$ref` in list response)
-   **Request Parameters (List):** `limit` (query, e.g., 1000)
-   **Key Fields (Detail):**
    -   `year` (map to `id`, primary key)
    -   `startDate`, `endDate`
    -   `displayName`
    -   `types.$ref` (URL to list Season Types for this season)
    -   `rankings.$ref`
    -   `powerIndexes.$ref`
    -   `powerIndexLeaders.$ref`
    -   `athletes.$ref`
    -   `awards.$ref`
    -   `futures.$ref`
    -   `leaders.$ref`
-   **Primary Key (`dlt`):** `id` (string, derived from API `year`)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `seasons_example.json` (list), `seasons_-season-id_example.json` (detail)
-   **Concurrency Strategy:** If fetching *all* seasons (not a specific `season_year`), the loop fetching details for each season `$ref` is sequential. If the number of seasons is large and fetching each is slow, this resource could potentially be refactored to list all `$ref`s first, then use a separate `@dlt.defer`-ed transformer to fetch details. For now, with potentially 20-30+ seasons, it might show some slowdown but is less critical than event-level concurrency. If a single `season_year` is provided, it's a single detail fetch, so concurrency isn't a concern for that path.
-   **Implementation Notes:**
    -   Acts as the entry point if `season_year` is not provided.
    -   Fetches the list of season `$ref`s.
    -   Yields detailed season data fetched from each `$ref`.
    -   The `year` field (e.g., 2024) is the natural primary key.


### 2. Season Types

-   **`dlt` Resource Name:** `season_types_transformer`
-   **Parent Resource:** `seasons_resource`
-   **Description:** For each season, fetches the different types within it (e.g., Preseason, Regular Season, Postseason).
-   **Endpoint Path (List):** `/seasons/{season_id}/types` (from `season_detail.types.$ref` or constructed)
-   **Endpoint Path (Detail):** (from `$ref` in the list response)
-   **Request Parameters (List):** `limit` (query)
-   **Key Fields (Detail):**
    -   `id` (e.g., "1", "2", "3", "4")
    -   `name`, `abbreviation`
    -   `year`, `startDate`, `endDate`
    -   `hasStandings`, `hasGroups` (booleans)
    -   `weeks.$ref` (URL to list Weeks for this season type)
    -   `groups.$ref` (URL to list Groups/Conferences for this season type)
    -   `leaders.$ref` (URL to list Leaders for this season type)
-   **Primary Key (`dlt`):** `id`, `season_id_fk` (composite)
-   **Foreign Keys (`dlt`):** `season_id_fk` (references `seasons.id`)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `seasons_-season-id-_types_example.json` (list), `seasons_-season-id-_types_-type-id_example.json` (detail)
-   **Concurrency Strategy:** Recommended to use `@dlt.defer`. Each call processes one season and makes a few (e.g., 2-4) API calls for type details. `@dlt.defer` will allow different seasons' type details to be fetched concurrently.
-   **Implementation Notes:**
    -   Transforms data yielded by `seasons_resource`.
    -   Fetches the list of type `$ref`s for a season, then fetches details for each type.
    -   `season_id_fk` is the `id` (year) from the parent season.

### 3. Weeks

-   **`dlt` Resource Name:** `weeks_transformer`
-   **Parent Resource:** `season_types_transformer`
-   **Description:** For each season type, fetches the weeks within it.
-   **Endpoint Path (List):** `/seasons/{season_id}/types/{type_id}/weeks` (from `season_type_detail.weeks.$ref` or constructed)
-   **Endpoint Path (Detail):** (from `$ref` in the list response)
-   **Request Parameters (List):** `limit` (query)
-   **Key Fields (Detail):**
    -   `number` (map to `id` in `dlt`)
    -   `startDate`, `endDate`, `text`
    -   `events.$ref` (URL to list Events for this week)
    -   `rankings.$ref` (URL to list Rankings for this week)
-   **Primary Key (`dlt`):** `id`, `type_id_fk`, `season_id_fk` (composite)
-   **Foreign Keys (`dlt`):**
    -   `type_id_fk` (references `season_types.id`)
    -   `season_id_fk` (references `seasons.id`)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `seasons_-season-id-_types_-type-id-_weeks_example.json` (list), `seasons_-season-id-_types_-type-id-_weeks_-week-id_example.json` (detail)
-   **Concurrency Strategy:** Recommended to use `@dlt.defer`. Each call processes one season type and makes multiple (e.g., ~20) API calls for week details. `@dlt.defer` allows different season types' week details to be fetched concurrently.
-   **Implementation Notes:**
    -   Transforms data yielded by `season_types_transformer`.
    -   Fetches the list of week `$ref`s for a season type, then fetches details for each week.
    -   The API's `number` field maps to the `id` column.
    -   Pass `type_id_fk` and `season_id_fk` down from the parent.

### 4. Events (Games)

-   **`dlt` Resource Name:** `events_transformer`
-   **Parent Resource:** `weeks_transformer`
-   **Description:** For each week, fetches the events (games) occurring within it.
-   **Endpoint Path (List):** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/events` (from `week_detail.events.$ref` or constructed). Also accessible via `/events?dates=YYYYMMDD&groups=...&seasontype=...`
-   **Endpoint Path (Detail):** `/events/{event_id}` (from `$ref` in list response)
-   **Request Parameters (List):** `limit` (query). Can also filter by `dates`, `groups`, `seasontype` if using `/events`.
-   **Key Fields (Detail - `events` table):**
    -   `id` (primary key)
    -   `uid`, `date`, `name`, `shortName`, `timeValid`
    -   `season.$ref` (parse `season_year`)
    -   `seasonType.$ref` (parse `type_id`)
    -   `week.$ref` (parse `week_number`)
    -   `competitions` (array, usually 1 item):
        -   `id` (competition ID, often same as `event_id`)
        -   `date`, `attendance`, `neutralSite`, `conferenceCompetition`
        -   `venue.$ref` (parse `venue_id`)
        -   `status.$ref` (URL for status sub-resource)
        -   (Many boolean flags: `boxscoreAvailable`, `playByPlayAvailable`, etc.)
        -   (Many `$ref` links to sub-resources: `odds`, `broadcasts`, `plays`, `predictor`, etc.)
-   **Primary Key (`dlt`):** `id` (string, from API `id`)
-   **Foreign Keys (`dlt`):**
    -   `week_id_fk` (references `weeks.id`, derived from `week.$ref` or parent context)
    -   `type_id_fk` (references `season_types.id`, derived from `seasonType.$ref` or parent context)
    -   `season_id_fk` (references `seasons.id`, derived from `season.$ref` or parent context)
    -   `venue_id_fk` (references `venues.id`, parsed from `competitions[0].venue.$ref`)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_example.json` (list), `events_-event-id_example.json` (detail)
-   **Concurrency Strategy:** Recommended to use `@dlt.defer`. Each call processes one week event list and makes multiple (e.g., ~300) API calls for event details. `@dlt.defer` allows different week's event details to be fetched concurrently.
-   **Implementation Notes:**
    -   Transforms data yielded by `weeks_transformer`.
    -   Fetches the list of event `$ref`s for a week, then fetches details concurrently.
    -   The main event data goes into the `events` table.
    -   The `competitions` array data (especially competitors) needs careful handling. It might be best to yield this competition data separately for processing by child transformers (like `event_competitors_transformer`).
    -   Store the various `$ref` URLs from the competition object to be used by child transformers.

---

## Event Sub-Resources (Game Details)

These transformers process data related to a specific event, typically yielded by `events_transformer`.

### 5. Event Competitors

-   **`dlt` Resource Name:** `event_competitors_transformer`
-   **Parent Resource:** `events_transformer` (processing `event_detail.competitions[0].competitors` array)
-   **Description:** Extracts details about the home and away teams for each event.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}` (from `$ref`, if needed, but data is usually in parent)
-   **Key Fields (from competitor object):**
    -   `id` (team_id, map to `team_id` in `dlt`)
    -   `uid` (team_uid)
    -   `order` (integer, e.g., 0 or 1)
    -   `homeAway` ("home" or "away")
    -   `winner` (boolean)
    -   `team.$ref` (URL to team details, parse `team_id` again for validation/linking)
    -   `score.$ref` (URL for score sub-resource)
    -   `linescores.$ref` (URL for linescores sub-resource)
    -   `statistics.$ref` (URL for team stats sub-resource)
    -   `leaders.$ref` (URL for leaders sub-resource)
    -   `roster.$ref` (URL for roster sub-resource)
    -   `records.$ref` (URL for pre-game records sub-resource)
    -   `curatedRank.current` (if available)
-   **Primary Key (`dlt`):** `event_id_fk`, `team_id` (composite)
-   **Foreign Keys (`dlt`):**
    -   `event_id_fk` (references `events.id`)
    -   `team_id` (references `teams.id` - master team table)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id_example.json` (detail for one competitor). Data is nested in `events_-event-id_example.json`.
-   **Implementation Notes:**
    -   Iterates through the `competitors` array for each event competition.
    -   Yields one record per team per event, including `homeAway` status and the various `$ref` URLs needed by subsequent transformers.

### 6. Event Scores (Final Score per Team)

-   **`dlt` Resource Name:** `event_scores_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches the final score for each team in an event.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/score` (from `competitor.score.$ref`)
-   **Key Fields:**
    -   `value` (the score)
    -   `displayValue` (string score)
    -   `winner` (boolean, optional)
-   **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_score_example.json`, `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_scores_-score-id_example.json` (Note: `/scores/{score_id}` seems redundant if `/score` gives the final score).
-   **Implementation Notes:**
    -   Fetches score detail using the `$ref` from the competitor object.
    -   Parses `event_id_fk` and `team_id_fk` from the URL.

### 7. Event Linescores (Score per Period)

-   **`dlt` Resource Name:** `event_linescores_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches the score for each team for each period (half) of the game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/linescores` (from `competitor.linescores.$ref`)
-   **Paginated:** Yes (but typically 2 items for basketball halves)
-   **Key Fields (Item):**
    -   `value` (score for the period)
    -   `period` (integer, e.g., 1 or 2)
    -   `displayValue`
-   **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `period` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_linescores_example.json` (list), `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_linescores_-linescore-id-_-1-id_example.json` (detail, likely not needed if list provides all).
-   **Implementation Notes:**
    -   Fetches the list of linescore items using the `$ref` from the competitor object.
    -   Yields one record per team per period.

### 8. Event Team Statistics (Aggregates)

-   **`dlt` Resource Name:** `event_team_statistics_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches aggregated team statistics for the game.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/statistics` (from `competitor.statistics.$ref`)
-   **Key Fields:**
    -   `splits.categories` array (e.g., "defensive", "offensive")
    -   `splits.categories[...].stats` array (individual stats)
        -   `name`, `displayName`, `abbreviation`, `value`, `displayValue`
    -   `splits.athletes[...].statistics.$ref` (URLs for player stats)
-   **Table Structure (`dlt` - Tidy Format Recommended):** `event_team_stats`
    -   Columns: `event_id_fk`, `team_id_fk`, `category_name`, `stat_name`, `stat_value`, `stat_display_value`
    -   Primary Key: `event_id_fk`, `team_id_fk`, `stat_name`
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_statistics_example.json` (detail), `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_statistics_-statistic-id_example.json` (detail for one category, likely not needed directly).
-   **Implementation Notes:**
    -   Fetches the single JSON object containing all team stats categories.
    -   Unnests the `categories` and `stats` arrays into a tidy format.
    -   Crucially, **yields the `$ref` URLs for each player's statistics** found in `splits.athletes` for the `event_player_statistics_transformer` to consume.

### 9. Event Player Statistics

-   **`dlt` Resource Name:** `event_player_statistics_transformer`
-   **Parent Resource:** `event_team_statistics_transformer` (yielding player stat `$ref` URLs)
-   **Description:** Fetches detailed game statistics for each individual player.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster/{athlete_id}/statistics/{split_id}` (from `$ref`)
-   **Key Fields:**
    -   `athlete.$ref` (parse `athlete_id`)
    -   `splits.categories` array (similar to team stats)
    -   `splits.categories[...].stats` array (individual player stats)
        -   `name`, `displayName`, `value`, `displayValue`
-   **Table Structure (`dlt` - Tidy Format Recommended):** `event_player_stats`
    -   Columns: `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `category_name`, `stat_name`, `stat_value`, `stat_display_value`
    -   Primary Key: `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `stat_name`
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_roster_-roster-id-_statistics_-statistic-id_example.json`
-   **Implementation Notes:**
    -   Fetches details concurrently using the `$ref` URLs yielded by the parent.
    -   Parses `event_id`, `team_id`, `athlete_id` from the URL.
    -   Unnests categories and stats into a tidy format.

### 10. Event Team Leaders

-   **`dlt` Resource Name:** `event_leaders_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches the leading players for each team in key statistical categories for the game.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/leaders` (from `competitor.leaders.$ref`)
-   **Key Fields:**
    -   `categories` array (e.g., "points", "assists")
    -   `categories[...].leaders` array
        -   `displayValue`, `value`
        -   `athlete.$ref` (parse `athlete_id`)
-   **Table Structure (`dlt` - Tidy Format Recommended):** `event_leaders`
    -   Columns: `event_id_fk`, `team_id_fk`, `category_name`, `athlete_id_fk`, `stat_value`, `stat_display_value`
    -   Primary Key: `event_id_fk`, `team_id_fk`, `category_name`, `athlete_id_fk`
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_leaders_example.json`
-   **Implementation Notes:**
    -   Fetches the leader data using the `$ref` from the competitor object.
    -   Unnests categories and leaders into a tidy format.

### 11. Event Roster

-   **`dlt` Resource Name:** `event_roster_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches the roster of players who were available/participated for a team in a specific game.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster` (from `competitor.roster.$ref`)
-   **Key Fields:**
    -   `entries` array (one per player)
        -   `playerId` (map to `athlete_id`)
        -   `starter` (boolean)
        -   `didNotPlay` (boolean)
        -   `ejected` (boolean)
        -   `displayName`
        -   `athlete.$ref` (parse `athlete_id`)
        -   `position.$ref` (parse `position_id`)
        -   `statistics.$ref` (URL for player stats - can be used as alternative trigger)
-   **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `position_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_roster_example.json`
-   **Implementation Notes:**
    -   Fetches roster details using the `$ref` from the competitor object.
    -   Yields one record per player on the game roster.

### 12. Event Team Records (Pre-Game)

-   **`dlt` Resource Name:** `event_records_transformer`
-   **Parent Resource:** `event_competitors_transformer`
-   **Description:** Fetches the team's record (overall, home, away, vs conf.) *before* the game started.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/records` (from `competitor.records.$ref`)
-   **Paginated:** Yes (for different record types)
-   **Key Fields (Item):**
    -   `name` (e.g., "overall", "Home", "Road", "vs. Conf.")
    -   `type` (e.g., "total", "home", "road", "vsconf")
    -   `summary` (W-L string)
    -   `stats` array (wins, losses, ties, winPercent, etc.)
-   **Table Structure (`dlt` - Tidy Format Recommended):** `event_pregame_records`
    -   Columns: `event_id_fk`, `team_id_fk`, `record_type`, `stat_name`, `stat_value`, `stat_display_value`, `summary`
    -   Primary Key: `event_id_fk`, `team_id_fk`, `record_type`, `stat_name`
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_records_example.json` (list), `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_records_-record-id_example.json` (detail, likely not needed).
-   **Implementation Notes:**
    -   Fetches the list of pre-game records using the `$ref` from the competitor object.
    -   Unnests the `stats` array into a tidy format, using the record `type` or `name` to distinguish record types.

### 13. Event Status

-   **`dlt` Resource Name:** `event_status_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches the current status (scheduled, live, final) and clock/period information for a game.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/status` (from `event_detail.competitions[0].status.$ref`)
-   **Key Fields:** `clock`, `displayClock`, `period`, `type` (nested object with `id`, `name`, `state`, `completed`, `description`, `detail`).
-   **Primary Key (`dlt`):** `event_id_fk`
-   **Foreign Keys (`dlt`):** `event_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_status_example.json`
-   **Implementation Notes:** Essential for tracking game completion and live updates.

### 14. Event Situation

-   **`dlt` Resource Name:** `event_situation_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches live game situation details like timeouts remaining, fouls, and possession.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/situation` (from `event_detail.competitions[0].situation.$ref`)
-   **Key Fields:** `lastPlay.$ref`, `homeTimeouts`, `awayTimeouts`, `homeFouls`, `awayFouls`, `possession.$ref`.
-   **Primary Key (`dlt`):** `event_id_fk`
-   **Foreign Keys (`dlt`):** `event_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_situation_example.json`
-   **Implementation Notes:** Primarily useful for real-time dashboards or analysis during live games.

### 15. Event Odds

-   **`dlt` Resource Name:** `event_odds_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches betting odds from various providers for a game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/odds` (from `event_detail.competitions[0].odds.$ref`)
-   **Paginated:** Yes (one item per provider)
-   **Key Fields (Item):** `provider.$ref` (parse `provider_id`), `details`, `overUnder`, `spread`, `awayTeamOdds`, `homeTeamOdds` (nested odds details).
-   **Primary Key (`dlt`):** `event_id_fk`, `provider_id_fk` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `provider_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_odds_example.json` (list), `events_-event-id-_competitions_-competition-id-_odds_-odd-id_example.json` (detail).
-   **Implementation Notes:** Unnest complex odds structures (open, current, close, moneyline, spread, etc.) as needed. Requires a `providers` master table.

### 16. Event Broadcasts

-   **`dlt` Resource Name:** `event_broadcasts_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches broadcast information (TV, web) for a game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/broadcasts` (from `event_detail.competitions[0].broadcasts.$ref`)
-   **Paginated:** Yes
-   **Key Fields (Item):** `media.$ref` (parse `media_id`), `type.shortName`, `market.type`, `lang`, `region`.
-   **Primary Key (`dlt`):** `event_id_fk`, `media_id_fk`, `market_type` (composite, adjust if needed for uniqueness).
-   **Foreign Keys (`dlt`):** `event_id_fk`, `media_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_broadcasts_example.json`
-   **Implementation Notes:** Requires a `media` master table.

### 17. Event Plays (Play-by-Play)

-   **`dlt` Resource Name:** `event_plays_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches the detailed play-by-play sequence for a game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/plays` (from `event_detail.competitions[0].plays.$ref`)
-   **Paginated:** Yes (use `limit=API_LIMIT`)
-   **Key Fields (Item):** `id` (play ID), `sequenceNumber`, `type.text`, `text`, `awayScore`, `homeScore`, `period.number`, `clock.displayValue`, `scoringPlay`, `shootingPlay`, `team.$ref`, `participants` array.
-   **Primary Key (`dlt`):** `event_id_fk`, `id` (play's own ID)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk` (if `team.$ref` exists)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_plays_example.json` (list), `events_-event-id-_competitions_-competition-id-_plays_-play-id_example.json` (detail).
-   **Implementation Notes:**
    -   This can be a large resource. Fetch pages sequentially or manage concurrency carefully.
    -   Handle the `participants` array, potentially in a separate `play_participants` table linking `event_id`, `play_id`, `athlete_id`, and `participant_type`.

### 18. Event Predictor

-   **`dlt` Resource Name:** `event_predictor_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches ESPN's win probability and predicted score/margin.
-   **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/predictor` (from `event_detail.competitions[0].predictor.$ref`)
-   **Key Fields:** `homeTeam.statistics` array (find `gameProjection`, `teamChanceLoss`), `awayTeam.statistics` array.
-   **Primary Key (`dlt`):** `event_id_fk`
-   **Foreign Keys (`dlt`):** `event_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `events_-event-id-_competitions_-competition-id-_predictor_example.json`
-   **Implementation Notes:** Extract relevant prediction stats (win probability, predicted margin).

### 19. Event Probabilities

-   **`dlt` Resource Name:** `event_probabilities_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches time-series win probability data, often linked to plays.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/probabilities` (from `event_detail.competitions[0].probabilities.$ref`)
-   **Paginated:** Yes
-   **Key Fields (Item):** `play.$ref` (parse `play_id`), `homeWinPercentage`, `awayWinPercentage`, `tiePercentage`.
-   **Primary Key (`dlt`):** `event_id_fk`, `play_id_fk` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`, `play_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_probabilities_example.json` (list), `events_-event-id-_competitions_-competition-id-_probabilities_-probabilitie-id_example.json` (detail).
-   **Implementation Notes:** Links win probability snapshots to specific plays in the game log.

### 20. Event Power Index

-   **`dlt` Resource Name:** `event_powerindex_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches team power index ratings (BPI/FPI) related to the game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/powerindex` (from `event_detail.competitions[0].powerindex.$ref`)
-   **Paginated:** Yes (usually 2 items, one per team)
-   **Key Fields (Item):** `team.$ref` (parse `team_id`), `stats` array (`name`, `value`, `displayValue` for BPI stats like `matchupquality`, `teampredwinpct`).
-   **Table Structure (`dlt` - Tidy Format Recommended):** `event_powerindex_stats`
    -   Columns: `event_id_fk`, `team_id_fk`, `stat_name`, `stat_value`, `stat_display_value`
    -   Primary Key: `event_id_fk`, `team_id_fk`, `stat_name`
-   **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_powerindex_example.json` (list), `events_-event-id-_competitions_-competition-id-_powerindex_-powerindex-id_example.json` (detail).
-   **Implementation Notes:** Unnest the `stats` array.

### 21. Event Officials

-   **`dlt` Resource Name:** `event_officials_transformer`
-   **Parent Resource:** `events_transformer`
-   **Description:** Fetches the officials assigned to the game.
-   **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/officials` (from `event_detail.competitions[0].officials.$ref`)
-   **Paginated:** Yes
-   **Key Fields (Item):** `id` (official's ID), `displayName`, `position.name`.
-   **Primary Key (`dlt`):** `event_id_fk`, `official_id` (composite)
-   **Foreign Keys (`dlt`):** `event_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `events_-event-id-_competitions_-competition-id-_officials_example.json` (list), `events_-event-id-_competitions_-competition-id-_officials_-official-id_example.json` (detail).
-   **Implementation Notes:** Creates a link between events and officials. A master `officials` table could be created if more official details are desired/available elsewhere.

---

## Master / Dimension Tables

These tables store reference data that changes less frequently.

### 22. Teams (Master)

-   **`dlt` Resource Name:** `teams_resource` (if fetched independently) or `teams_transformer` (if triggered by refs)
-   **Parent Resource:** N/A if independent, or various (e.g., `event_competitors_transformer`)
-   **Description:** Fetches details for all teams, potentially filtered by season.
-   **Endpoint Path (List):** `/seasons/{season_id}/teams` (Provides list of team `$ref`s for a season)
-   **Endpoint Path (Detail):** `/seasons/{season_id}/teams/{team_id}` (from `$ref`)
-   **Key Fields (Detail):** `id`, `uid`, `slug`, `location`, `name`, `nickname`, `displayName`, `abbreviation`, `color`, `logos`, `venue.$ref`, `franchise.$ref`.
-   **Primary Key (`dlt`):** `id`, `season_id_fk` (composite, as team details like roster link are season-specific)
-   **Foreign Keys (`dlt`):** `season_id_fk`, `venue_id_fk`, `franchise_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `seasons_-season-id-_teams_example.json` (list), `seasons_-season-id-_teams_-team-id_example.json` (detail).
-   **Implementation Notes:**
    -   Crucial dimension table.
    -   Needs to be populated for *all* relevant seasons.
    -   The endpoint `/seasons/{season_id}/teams` is the primary way to discover teams for a season. Fetch details from the `$ref`s.

### 23. Athletes (Master)

-   **`dlt` Resource Name:** `athletes_resource` (if fetched independently) or `athletes_transformer`
-   **Parent Resource:** N/A or various (e.g., `event_roster_transformer`, `season_team_athletes_list`)
-   **Description:** Fetches details for athletes.
-   **Endpoint Path (List):** `/seasons/{season_id}/athletes` (List all athletes for a season) OR `/seasons/{season_id}/teams/{team_id}/athletes` (List athletes for a team/season).
-   **Endpoint Path (Detail):** `/seasons/{season_id}/athletes/{athlete_id}` (from `$ref`)
-   **Key Fields (Detail):** `id`, `uid`, `guid`, `displayName`, `shortName`, `firstName`, `lastName`, `displayHeight`, `displayWeight`, `age`, `dateOfBirth`, `birthPlace`, `jersey`, `position.$ref`, `team.$ref`, `headshot`.
-   **Primary Key (`dlt`):** `id` (Assuming athlete ID is globally unique across seasons). If API data *forces* seasonal context, use `id`, `season_id_fk`.
-   **Foreign Keys (`dlt`):** `position_id_fk`, `team_id_fk` (current team for the season).
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `seasons_-season-id-_athletes_example.json` (list), `seasons_-season-id-_athletes_-athlete-id_example.json` (detail), `seasons_-season-id-_teams_-team-id-_athletes_example.json` (list for team).
-   **Implementation Notes:**
    -   Another critical dimension.
    -   Needs robust discovery - iterating through all teams in all seasons via `/seasons/{s}/teams/{t}/athletes` might be necessary.
    -   Handle potential duplicates if an athlete appears in multiple seasons' lists. Merge ensures the latest info is kept based on the PK.

### 24. Venues (Master)

-   **`dlt` Resource Name:** `venues_resource` or `venues_transformer`
-   **Parent Resource:** N/A or various (e.g., `teams_resource`, `events_transformer`)
-   **Description:** Fetches details for venues.
-   **Endpoint Path (Detail):** `/venues/{venue_id}` (from `$ref`)
-   **Key Fields (Detail):** `id`, `fullName`, `address` (nested), `capacity`, `grass`, `indoor`, `images`.
-   **Primary Key (`dlt`):** `id`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `venues_-venue-id_example.json`
-   **Implementation Notes:** Master table for locations. Needs a way to discover all relevant `venue_id`s (e.g., from team or event details).

### 25. Positions (Master)

-   **`dlt` Resource Name:** `positions_resource` or `positions_transformer`
-   **Parent Resource:** N/A or various (e.g., `athletes_resource`)
-   **Description:** Fetches details for player positions.
-   **Endpoint Path (Detail):** `/positions/{position_id}` (from `$ref`)
-   **Key Fields (Detail):** `id`, `name`, `displayName`, `abbreviation`.
-   **Primary Key (`dlt`):** `id`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `positions_-position-id_example.json`
-   **Implementation Notes:** Master table for positions. Need to discover relevant `position_id`s.

### 26. Coaches (Master & Seasonal)

-   **`dlt` Resource Names:** `coaches_master_resource`, `coach_records_transformer`, `team_season_coaches_transformer`
-   **Parent Resource:** N/A or various (e.g., `teams_resource`)
-   **Description:** Fetches coach details, records, and team assignments.
-   **Endpoint Paths:** See detailed breakdown in the previous inventory version (Section 28). Key paths include `/coaches/{id}`, `/seasons/{s}/coaches/{id}`, `/seasons/{s}/teams/{t}/coaches`.
-   **Key Fields:** Coach info (`id`, `firstName`, `lastName`), Record info (`summary`, `wins`, `losses`), Team assignment info (`team.$ref`, `season.$ref`).
-   **Primary Keys (`dlt`):**
    -   `coaches_master`: `id`
    -   `coach_records`: `coach_id_fk`, `season_id_fk`, `type_id_fk`, `record_type_name`
    -   `team_season_coaches`: `season_id_fk`, `team_id_fk`, `coach_id_fk`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `coaches_-coache-id_example.json`, `seasons_-season-id-_coaches_-coache-id_example.json`, `seasons_-season-id-_teams_-team-id-_coaches_example.json`, etc.
-   **Implementation Notes:** Requires careful handling of different endpoints to build a complete picture.

### 27. Awards (Master & Seasonal)

-   **`dlt` Resource Names:** `awards_master_resource`, `season_awards_transformer`
-   **Parent Resource:** N/A or `seasons_resource`
-   **Description:** Fetches award types and recipients by season.
-   **Endpoint Paths:** `/awards`, `/awards/{id}`, `/seasons/{s}/awards`, `/seasons/{s}/awards/{id}`.
-   **Key Fields:** Award info (`id`, `name`, `description`), Recipient info (`athlete.$ref`, `team.$ref`).
-   **Primary Keys (`dlt`):**
    -   `awards_master`: `id`
    -   `season_awards_recipients`: `season_id_fk`, `award_id_fk`, `athlete_id_fk` (or other recipient identifier)
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `awards_example.json`, `awards_-award-id_example.json`, `seasons_-season-id-_awards_example.json`, `seasons_-season-id-_awards_-award-id_example.json`.
-   **Implementation Notes:** Distinguish between award definitions and instances.

### 28. Franchises (Master)

-   **`dlt` Resource Name:** `franchises_resource` or `franchises_transformer`
-   **Parent Resource:** N/A or `teams_resource`
-   **Description:** Fetches franchise information (linking teams over time/name changes).
-   **Endpoint Path (List):** `/franchises`
-   **Endpoint Path (Detail):** `/franchises/{franchise_id}` (from `$ref`)
-   **Key Fields:** `id`, `uid`, `slug`, `location`, `name`, `nickname`, `team.$ref` (current team).
-   **Primary Key (`dlt`):** `id`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample Files:** `franchises_example.json` (list), `franchises_-franchise-id_example.json` (detail).
-   **Implementation Notes:** Useful for historical analysis.

### 29. Providers (Master)

-   **`dlt` Resource Name:** `providers_resource` or `providers_transformer`
-   **Parent Resource:** N/A or `event_odds_transformer`
-   **Description:** Fetches details of odds providers.
-   **Endpoint Path (Detail):** `/providers/{provider_id}` (from `$ref`)
-   **Key Fields:** `id`, `name`, `priority`.
-   **Primary Key (`dlt`):** `id`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `providers_-provider-id_example.json`
-   **Implementation Notes:** Need to discover IDs from odds endpoints.

### 30. Media (Master)

-   **`dlt` Resource Name:** `media_resource` or `media_transformer`
-   **Parent Resource:** N/A or `event_broadcasts_transformer`
-   **Description:** Fetches details of media outlets.
-   **Endpoint Path (Detail):** `/media/{media_id}` (from `$ref`)
-   **Key Fields:** `id`, `name`, `shortName`, `slug`.
-   **Primary Key (`dlt`):** `id`
-   **Write Disposition (`dlt`):** `merge`
-   **Sample File:** `media_-media-id_example.json`
-   **Implementation Notes:** Need to discover IDs from broadcast endpoints.

---

## Other Potential Resources (Less Common/Placeholder Samples)

-   **Transactions:** `/transactions` (Sample `transactions_example.json` is empty)
-   **Notes:** `/notes`, `/teams/{id}/notes`, `/seasons/{s}/athletes/{id}/notes` (Samples empty)
-   **Injuries:** `/teams/{id}/injuries` (Sample empty)
-   **Calendar:** `/calendar/*` (Samples exist, use depends on logic needed)
-   **Futures:** `/seasons/{s}/futures/*` (Samples exist, for betting futures)
-   **Power Index (Season):** `/seasons/{s}/powerindex/*` (Samples exist, season-level BPI)
-   **Rankings (Season/Overall):** `/rankings`, `/seasons/{s}/rankings/*` (Samples exist, overview of poll types)
-   **Groups/Conferences:** `/seasons/{s}/types/{t}/groups/*` (Samples exist, defines conference structure)
-   **Standings:** `/seasons/{s}/types/{t}/groups/{g}/standings/*` (Samples exist, detailed standings data)
-   **Tournaments/Bracketology:** `/tournaments/*` (Samples exist, for tournament-specific views)

---
