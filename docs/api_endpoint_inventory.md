# ESPN API Endpoint Inventory for `dlt` Source (v2) - Revised Concurrency

**Version:** 2.2 **Date:** 2025-05-08 **Purpose:** This document catalogs the ESPN API endpoints for the NCAA Men's
College Basketball league (`mens-college-basketball`) and provides detailed guidance for implementing a `dlt` (Data Load
Tool) source. This version includes revised recommendations for concurrency, favoring `@dlt.defer`, and tracks
implementation progress.

**Base URL:** `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball`

---

## General `dlt` Implementation Guidelines

Based on `espn_source.py` and API structure:

1. **`$ref` Handling:** The API extensively uses `$ref` fields.
   - Parent `dlt` resources/transformers will typically list these references (often under an `"items"` key).
   - For fetching details from these `$ref`s, a two-transformer pattern is often preferred:
     - A "Lister" transformer: Takes a parent ID/object, makes the paginated API call to list child `$ref`s, and yields
       _each individual child `$ref` object_ (augmented with foreign keys).
     - A "Detail Fetcher" transformer: Takes an individual child `$ref` object from the "Lister", is decorated with
       `@dlt.defer`, and fetches the details from the absolute URL in the `$ref`.
2. **Client Configuration:**
   - **List Client:** `RESTClient` with `base_url`, `PageNumberPaginator`, and `data_selector="items"`.
   - **Detail Client:** `RESTClient` with `base_url=None` for absolute `$ref` URLs.
3. **Primary Keys & Foreign Keys:**
   - Composite primary keys are common.
   - Establish clear foreign key relationships (`_fk` suffix).
   - Store all ID fields as **strings**.
4. **Pagination:** List endpoints are paginated. Use `limit=API_LIMIT` (e.g., 1000). The "Lister" transformer (see
   point 1) will handle iterating through pages of `$ref`s.
5. **Data Structure:** Use `max_table_nesting=0` in `@dlt.source`.
6. **Concurrency:**
   - **Prefer `@dlt.defer`:** For any transformer that takes a single input item (like a `$ref` object) and needs to
     make an API call to fetch its details, use the `@dlt.defer` decorator. This allows `dlt` to manage a thread pool
     for these operations.
   - **Manual `ThreadPoolExecutor`:** May still be useful in specific, complex scenarios within a single transformer if
     `@dlt.defer` is not a natural fit, but aim to break down tasks to leverage `@dlt.defer` where possible.
7. **Error Handling:** Robust `try...except` blocks within all API call logic, especially in `@dlt.defer`-decorated
   functions. Log errors and allow the pipeline to continue with other items.
8. **Incremental Loading:** Utilize `dlt` state for incremental loads (e.g., last fetched date, processed event IDs).
9. **API Path Structure Anomaly:** Reminder: `event_id` often serves as `competition_id` in event sub-resource paths.
10. **Resource Naming:** Descriptive names (e.g., `seasons_resource`, `season_types_lister_transformer`,
    `season_type_detail_fetcher_transformer`).

---

## Core Resources (Sequential Discovery Flow)

### 1. Seasons

- **Description:** Fetches season details. If `season_year` is given, fetches that specific season. Otherwise, lists all
  season `$ref`s and then fetches details.
- **Status:** TODO
- **`dlt` Table Name:** `seasons` (also `league_info` for the root)
- **Key Transformer(s):** `league_info_resource`, `season_refs_lister_transformer`, `season_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/seasons`
- **Endpoint Path (Detail):** `/seasons/{season_id}` (from `$ref` or constructed if `season_year` is given)
- **Primary Key (`dlt`):** `id` (string, from API `year`)
- **Implementation Notes:**
  - If listing all seasons:
    - `season_refs_lister_transformer`: Lists `$ref`s.
    - `season_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details for each `$ref`.
  - If `season_year` is provided, `seasons_resource` can directly fetch that season's detail.

### 2. Season Types

- **Parent Resource:** `season_detail_fetcher_transformer`
- **Description:** For each season, lists season type `$ref`s, then fetches details for each.
- **Status:** TODO
- **`dlt` Table Name:** `season_types`
- **Key Transformer(s):** `season_type_refs_lister_transformer`, `season_type_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/types`
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Primary Key (`dlt`):** `id`, `season_id_fk`
- **Implementation Notes:**
  - `season_type_refs_lister_transformer`: Takes `season_detail`, lists type `$ref`s, yields individual type `$ref`
    objects (augmented with `season_id_fk`).
  - `season_type_detail_fetcher_transformer` (using `@dlt.defer`): Takes a type `$ref` object, fetches details.

### 3. Weeks

- **Parent Resource:** `season_type_detail_fetcher_transformer`
- **Description:** For each season type, lists week `$ref`s, then fetches details for each.
- **Status:** TODO
- **`dlt` Table Name:** `weeks`
- **Key Transformer(s):** `week_refs_lister_transformer`, `week_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/types/{type_id}/weeks`
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Primary Key (`dlt`):** `id`, `type_id_fk`, `season_id_fk`
- **Implementation Notes:**
  - `week_refs_lister_transformer`: Takes `season_type_detail`, lists week `$ref`s, yields individual week `$ref`
    objects (augmented with `type_id_fk`, `season_id_fk`).
  - `week_detail_fetcher_transformer` (using `@dlt.defer`): Takes a week `$ref` object, fetches details. The API's
    `number` field maps to `id`.

### 4. Events (Games)

- **Parent Resource:** `week_detail_fetcher_transformer`
- **Description:** For each week, lists event `$ref`s, then fetches details for each.
- **Status:** TODO
- **`dlt` Table Name:** `events`
- **Key Transformer(s):** `event_refs_lister_transformer`, `event_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/events`
- **Endpoint Path (Detail):** `/events/{event_id}` (from `$ref` in list response)
- **Primary Key (`dlt` - in `events` table from detail fetcher):** `id`
- **Foreign Keys (`dlt`):** `week_id_fk`, `type_id_fk`, `season_id_fk`
- **Implementation Notes:**
  - `event_refs_lister_transformer`: Takes `week_detail`, lists event `$ref`s using `list_client.paginate`, yields
    _individual event `$ref` objects_ (augmented with `week_id_fk`, `type_id_fk`, `season_id_fk`).
  - `event_detail_fetcher_transformer` (using `@dlt.defer`): Takes an individual event `$ref` object, uses
    `detail_client` to fetch the full event details. Yields the event detail object which will be processed by
    subsequent event sub-resource transformers.

---

## Event Sub-Resources (Game Details)

These transformers generally process data from the `event_detail` object yielded by `event_detail_fetcher_transformer`
or fetch further details using `$ref`s found within it.

### 5. Event Competitors

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Extracts competitor details _directly_ from the `event_detail.competitions[0].competitors` array. No
  new API call needed by this transformer itself.
- **Status:** TODO
- **`dlt` Table Name:** `event_competitors`
- **Key Transformer(s):** `event_competitors_transformer`
- **Data Validated:** No
- **Primary Key (`dlt`):** `event_id_fk`, `team_id` (Note: `team_id` is called `id` in the transformer's PK definition)
- **Implementation Notes:** Iterates the `competitors` array in the parent event detail. Yields one record per
  competitor, including their various `$ref` URLs for subsequent detail transformers.

### 6. Event Scores (Final Score per Team)

- **Parent Resource:** `event_competitors_transformer` (which yields competitor objects containing `score.$ref`)
- **Description:** Fetches the final score for each team in an event.
- **Status:** TODO
- **`dlt` Table Name:** `event_scores`
- **Key Transformer(s):** `event_scores_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/score` (from
  `competitor.score.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`
- **Implementation Notes:**
  - Takes a `competitor_detail` object (containing `score_ref` and parent FKs).
  - Uses `@dlt.defer` to fetch score details from `score_ref` using `detail_client`.

### 7. Event Linescores (Score per Period)

- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the score for each team for each period (half) of the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_linescores`
- **Key Transformer(s):** `event_linescores_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/linescores` (from
  `competitor.linescores.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `period`
- **Implementation Notes:**
  - `event_linescores_transformer`: Takes `competitor_detail`, fetches the list of linescore items. If items are
    complete, yields them.
  - Yields one record per team per period.

### 8. Event Team Statistics (Aggregates)

- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches aggregated team statistics for the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_team_stats` (also intermediate `event_team_stats_raw_data`)
- **Key Transformer(s):** `event_team_stats_raw_fetcher_transformer`, `event_team_stats_processor_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/statistics` (from
  `competitor.statistics.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_team_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `stat_name`
- **Implementation Notes:**
  - `event_team_stats_raw_fetcher_transformer`: Takes `competitor_detail`, fetches raw JSON containing team and player
    stat refs.
  - `event_team_stats_processor_transformer`: Consumes raw data, unnests team stats into tidy format.
  - **Crucially, yields the `$ref` URLs for each player's statistics** found in `splits.athletes` for the
    `event_player_statistics_refs_lister_transformer` to consume.

### 9. Event Player Statistics

- **Parent Resource:** `event_team_stats_raw_fetcher_transformer` (yielding player stat `$ref` URLs via
  `event_player_stats_refs_lister_transformer`)
- **Description:** Fetches detailed game statistics for each individual player.
- **Status:** TODO
- **`dlt` Table Name:** `event_player_stats`
- **Key Transformer(s):** `event_player_stats_refs_lister_transformer`, `event_player_stats_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):**
  `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster/{athlete_id}/statistics/{split_id}` (from
  `$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_player_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `stat_name`
- **Implementation Notes:**
  - `event_player_stats_refs_lister_transformer`: Takes the list of player stat `$ref`s (and necessary FKs like
    `event_id`, `team_id`) from the parent. Yields individual player stat `$ref` objects (augmented with FKs).
  - `event_player_stats_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details for each player stat `$ref`.
    Parses `athlete_id` from the URL or ref object. Unnests categories and stats.

### 10. Event Team Leaders

- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the leading players for each team in key statistical categories for the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_leaders`
- **Key Transformer(s):** `event_leaders_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/leaders` (from
  `competitor.leaders.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_leaders`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `category_name`, `athlete_id_fk`
- **Implementation Notes:**
  - Takes `competitor_detail` (with `leaders_ref`).
  - Uses `@dlt.defer` to fetch leader data. Unnests categories and leaders.

### 11. Event Roster

- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the roster of players for a team in a specific game.
- **Status:** TODO
- **`dlt` Table Name:** `event_roster`
- **Key Transformer(s):** `event_roster_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster` (from
  `competitor.roster.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`
- **Implementation Notes:**
  - Takes `competitor_detail` (with `roster_ref`).
  - Uses `@dlt.defer` to fetch roster details. Yields one record per player on the game roster.

### 12. Event Team Records (Pre-Game)

- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the team's record (overall, home, away, vs conf.) _before_ the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_pregame_records`
- **Key Transformer(s):** `event_pregame_records_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/records` (from
  `competitor.records.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_pregame_records`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `record_type`, `stat_name`
- **Implementation Notes:**
  - `event_pregame_records_transformer`: Takes `competitor_detail` (with `records_ref`). Fetches the list of pre-game
    record items. Unnests `stats` array.

### 13. Event Status

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches current status for a game.
- **Status:** TODO
- **`dlt` Table Name:** `event_status`
- **Key Transformer(s):** `event_status_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/status` (from
  `event_detail.competitions[0].status.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `status.$ref`).
  - Uses `@dlt.defer` to fetch status details.

### 14. Event Situation

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches live game situation details.
- **Status:** TODO
- **`dlt` Table Name:** `event_situation`
- **Key Transformer(s):** `event_situation_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/situation` (from
  `event_detail.competitions[0].situation.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `situation.$ref`).
  - Uses `@dlt.defer` to fetch situation details.

### 15. Event Odds

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches betting odds for a game.
- **Status:** TODO
- **`dlt` Table Name:** `event_odds`
- **Key Transformer(s):** `event_odds_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/odds` (from
  `event_detail.competitions[0].odds.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `provider_id_fk`
- **Implementation Notes:**
  - `event_odds_transformer`: Takes `event_detail` (with `odds.$ref`). Fetches list of odds items (one per provider).
    Unnest complex odds structures.
  - Requires a `providers` master table (Item 29 - currently TODO for providers master).

### 16. Event Broadcasts

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches broadcast information for a game.
- **Status:** TODO
- **`dlt` Table Name:** `event_broadcasts`
- **Key Transformer(s):** `event_broadcasts_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/broadcasts` (from
  `event_detail.competitions[0].broadcasts.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `media_id_fk`, `type`
- **Implementation Notes:**
  - `event_broadcasts_transformer`: Takes `event_detail` (with `broadcasts.$ref`). Fetches list of broadcast items.
  - Requires a `media` master table (Item 30 - currently TODO for media master).

### 17. Event Plays (Play-by-Play)

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches detailed play-by-play sequence.
- **Status:** TODO
- **`dlt` Table Name:** `event_plays`
- **Key Transformer(s):** `event_plays_lister_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/plays` (from
  `event_detail.competitions[0].plays.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `id` (play's own ID)
- **Implementation Notes:**
  - `event_plays_lister_transformer`: Takes `event_detail` (with `plays.$ref`). Fetches pages of play items. Handle
    `participants` array.
  - This can be a large resource. Current implementation uses `@dlt.defer` on the lister itself and paginates within.

### 18. Event Predictor

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches ESPN's win probability and predicted score.
- **Status:** TODO
- **`dlt` Table Name:** `event_predictor`
- **Key Transformer(s):** `event_predictor_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/predictor` (from
  `event_detail.competitions[0].predictor.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `predictor.$ref`).
  - Uses `@dlt.defer` to fetch predictor details.

### 19. Event Probabilities

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches time-series win probability data.
- **Status:** TODO
- **`dlt` Table Name:** `event_probabilities`
- **Key Transformer(s):** `event_probabilities_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/probabilities` (from
  `event_detail.competitions[0].probabilities.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `play_id`
- **Implementation Notes:**
  - `event_probabilities_transformer`: Takes `event_detail` (with `probabilities.$ref`). Fetches list of probability
    items.

### 20. Event Power Index

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches team power index ratings (BPI/FPI) for the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_powerindex_stats`
- **Key Transformer(s):** `event_powerindex_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/powerindex` (from
  `event_detail.competitions[0].powerindex.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_powerindex_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `stat_name`
- **Implementation Notes:**
  - `event_powerindex_transformer`: Takes `event_detail` (with `powerindex.$ref`). Fetches list of power index items
    (usually 2). Unnest `stats`.

### 21. Event Officials

- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches officials assigned to the game.
- **Status:** TODO
- **`dlt` Table Name:** `event_officials`
- **Key Transformer(s):** `event_officials_transformer`
- **Data Validated:** No
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/officials` (from
  `event_detail.competitions[0].officials.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `official_id`
- **Implementation Notes:**
  - `event_officials_transformer`: Takes `event_detail` (with `officials.$ref`). Fetches list of official items.

---

## Master / Dimension Tables

Fetching details for master data from their respective `$ref` URLs should also use the "Lister" + "Detail Fetcher with
`@dlt.defer`" pattern if the primary way to discover them is via a list of refs, or directly with a "Detail Fetcher with
`@dlt.defer`" if individual refs are obtained from other resources.

### 22. Teams (Master)

- **Parent Resource:** `season_detail_fetcher_transformer`
- **Description:** Fetches details for all teams, typically discovered per season.
- **Status:** TODO
- **`dlt` Table Name:** `teams`
- **Key Transformer(s):** `team_refs_lister_transformer`, `team_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/teams`
- **Endpoint Path (Detail):** `/seasons/{season_id}/teams/{team_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`, `season_id_fk`
- **Implementation Notes:**
  - `team_refs_lister_transformer`: Takes `season_detail` (or just `season_id`), lists team `$ref`s for that season.
    Yields individual team `$ref` objects (augmented with `season_id_fk`).
  - `team_detail_fetcher_transformer` (using `@dlt.defer`): Takes a team `$ref` object, fetches details.

### 23. Athletes (Master)

- **Parent Resource:** `season_detail_fetcher_transformer` or `team_detail_fetcher_transformer`
- **Description:** Fetches details for athletes.
- **Status:** TODO
- **`dlt` Table Name:** `athletes`
- **Key Transformer(s):** `athlete_refs_lister_transformer`, `athlete_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/athletes` OR `/seasons/{season_id}/teams/{team_id}/athletes`
- **Endpoint Path (Detail):** `/seasons/{season_id}/athletes/{athlete_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id` (assuming global uniqueness). If contextually seasonal, use `id`, `season_id_fk`.
- **Implementation Notes:**
  - `athlete_refs_lister_transformer`: Takes `season_detail` lists athlete `$ref`s. Yields individual athlete `$ref`
    objects (augmented with `discovery_season_id_fk`).
  - `athlete_detail_fetcher_transformer` (using `@dlt.defer`): Takes an athlete `$ref` object, fetches details.

### 24. Venues (Master)

- **Parent Resource:** Various (e.g., `team_detail_fetcher_transformer`, `event_detail_fetcher_transformer` that yield
  venue `$ref`s)
- **Description:** Fetches details for venues.
- **Status:** TODO
- **`dlt` Table Name:** `venues`
- **Key Transformer(s):** `team_venue_ref_extractor_transformer`, `event_venue_ref_extractor_transformer`,
  `venue_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/venues/{venue_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - Ref extractors (`team_venue_ref_extractor_transformer`, `event_venue_ref_extractor_transformer`) yield venue
    `$ref`s.
  - `venue_detail_fetcher_transformer` (using `@dlt.defer`) consumes these refs and fetches details. Discovery is
    opportunistic.

### 25. Positions (Master)

- **Parent Resource:** Various (e.g., `athlete_detail_fetcher_transformer` that yields position `$ref`s)
- **Description:** Fetches details for player positions.
- **Status:** TODO
- **`dlt` Table Name:** `positions`
- **Key Transformer(s):** `athlete_position_ref_extractor_transformer`, `position_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/positions/{position_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - `athlete_position_ref_extractor_transformer` yields position `$ref` from athlete data.
  - `position_detail_fetcher_transformer` (using `@dlt.defer`) consumes `$ref`s.

### 26. Coaches (Master & Seasonal)

- **Description:** Fetches coach details and records.
- **Status:** TODO
- **`dlt` Table Name:** `coach_team_assignments`, `coaches`
- **Key Transformer(s):** `coach_team_assignments_resource`, `coach_master_ref_extractor_transformer`,
  `coaches_resource`
- **Data Validated:** No
- **Endpoint Paths:** `/coaches/{id}`, `/seasons/{s}/coaches/{id}`, `/seasons/{s}/teams/{t}/coaches` (list of refs).
- **Implementation Notes:**
  - `coach_team_assignments_resource`: Consumes `team_detail`, lists seasonal coach assignments for a team.
  - `coach_master_ref_extractor_transformer`: Extracts master coach `$ref` from assignments.
  - `coaches_resource` (using `@dlt.defer`): Fetches master coach details.

### 27. Awards (Master & Seasonal)

- **Description:** Fetches award details.
- **Status:** TODO
- **`dlt` Table Name:** `awards_master`, `awards_seasonal`
- **Key Transformer(s):** `award_master_refs_lister_transformer`, `award_master_detail_fetcher_transformer`,
  `season_award_instance_refs_lister_transformer`, `season_award_instance_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Paths:** `/awards` (list master award refs), `/awards/{id}` (detail), `/seasons/{s}/awards` (list season
  award refs), `/seasons/{s}/awards/{id}` (detail).
- **Implementation Notes:**
  - Master awards fetched via `award_master_refs_lister_transformer` and `award_master_detail_fetcher_transformer`.
  - Seasonal awards fetched via `season_award_instance_refs_lister_transformer` (from `season_detail`) and
    `season_award_instance_detail_fetcher_transformer`.

### 28. Franchises (Master)

- **Description:** Fetches franchise details.
- **Status:** TODO
- **`dlt` Table Name:** `franchises`
- **Key Transformer(s):** `franchise_refs_lister_transformer`, `franchises_resource`
- **Data Validated:** No
- **Endpoint Path (List of Refs):** `/franchises` (or constructed relative to `league_base_url`)
- **Endpoint Path (Detail):** `/franchises/{franchise_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - `franchise_refs_lister_transformer`: Lists all franchise `$ref`s from a potentially constructed general endpoint or
    from `league_doc`.
  - `franchises_resource` (using `@dlt.defer`): Fetches details.

### 29. Providers (Master - Odds)

- **Parent Resource:** `event_odds_transformer` (which yields items containing `provider.$ref`)
- **Description:** Fetches details of odds providers.
- **Status:** TODO
- **`dlt` Table Name:** `providers`
- **Key Transformer(s):** `odds_provider_ref_extractor_transformer`, `provider_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/providers/{provider_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - `odds_provider_ref_extractor_transformer`: Consumes from `event_odds_transformer` to get provider `$ref`s.
  - `provider_detail_fetcher_transformer` (using `@dlt.defer`) to fetch provider details.

### 30. Media (Master - Broadcasts)

- **Parent Resource:** `event_broadcasts_transformer` (which yields items containing `media.$ref`)
- **Description:** Fetches details of media outlets.
- **Status:** TODO
- **`dlt` Table Name:** `media`
- **Key Transformer(s):** `broadcast_media_ref_extractor_transformer`, `media_detail_fetcher_transformer`
- **Data Validated:** No
- **Endpoint Path (Detail):** `/media/{media_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - `broadcast_media_ref_extractor_transformer`: Consumes from `event_broadcasts_transformer` to get media `$ref`s.
  - `media_detail_fetcher_transformer` (using `@dlt.defer`) to fetch media details.

---

## Other Potential Resources (Placeholder Samples & Future Consideration)

These endpoints had samples in the discovery but may be lower priority or require further investigation for their
integration strategy. The "Lister" + "Detail Fetcher with `@dlt.defer`" pattern should be considered if they follow
similar `$ref` structures. All are **Status: TODO** and **Data Validated: No**.

- **Transactions:** `/transactions` (Sample `transactions_example.json` is empty)
  - **`dlt` Table Name:** `transactions`
  - **Key Transformer(s):** (To be defined)
- **Notes:** `/notes`, `/teams/{id}/notes`, `/seasons/{s}/athletes/{id}/notes` (Samples empty)
  - **`dlt` Table Name:** `notes`
  - **Key Transformer(s):** (To be defined)
- **Injuries:** `/teams/{id}/injuries` (Sample empty)
  - **`dlt` Table Name:** `injuries`
  - **Key Transformer(s):** (To be defined)
- **Calendar:** `/calendar/*` (Samples exist, use depends on logic needed) E.g., `/calendar/buyseason`,
  `/calendar/ondays`
  - **`dlt` Table Name:** `calendar_entries` (example)
  - **Key Transformer(s):** (To be defined)
- **Futures:** `/seasons/{s}/futures/*` (Samples exist, for betting futures) E.g., `/seasons/{s}/futures`,
  `/seasons/{s}/futures/{id}`
  - **`dlt` Table Name:** `futures_odds` (example)
  - **Key Transformer(s):** (To be defined)
- **Power Index (Season):** `/seasons/{s}/powerindex/*` (Samples exist, season-level BPI) E.g.,
  `/seasons/{s}/powerindex`, `/seasons/{s}/powerindex/{id}`
  - **`dlt` Table Name:** `season_power_index` (example)
  - **Key Transformer(s):** (To be defined)
- **Rankings (Season/Overall):** `/rankings`, `/seasons/{s}/rankings/*` (Samples exist, overview of poll types) E.g.,
  `/rankings`, `/rankings/{id}`, `/seasons/{s}/rankings`, `/seasons/{s}/types/{t}/weeks/{w}/rankings`,
  `/seasons/{s}/types/{t}/weeks/{w}/rankings/{id}`
  - **`dlt` Table Name:** `rankings`, `seasonal_rankings` (example)
  - **Key Transformer(s):** (To be defined)
- **Groups/Conferences:** `/seasons/{s}/types/{t}/groups/*` (Samples exist, defines conference structure) E.g.,
  `/seasons/{s}/types/{t}/groups`, `/seasons/{s}/types/{t}/groups/{id}`
  - **`dlt` Table Name:** `groups_conferences` (example)
  - **Key Transformer(s):** (To be defined)
- **Standings:** `/seasons/{s}/types/{t}/groups/{g}/standings/*` (Samples exist, detailed standings data) E.g.,
  `/seasons/{s}/types/{t}/groups/{g}/standings`, `/seasons/{s}/types/{t}/groups/{g}/standings/{id}` (standings are
  usually by season, not specific ID for the "standings" entity itself).
  - **`dlt` Table Name:** `standings` (example)
  - **Key Transformer(s):** (To be defined)
- **Tournaments/Bracketology:** `/tournaments/*` (Samples exist, for tournament-specific views) E.g., `/tournaments`,
  `/tournaments/{id}`
  - **`dlt` Table Name:** `tournaments`, `tournament_brackets` (example)
  - **Key Transformer(s):** (To be defined)

---
