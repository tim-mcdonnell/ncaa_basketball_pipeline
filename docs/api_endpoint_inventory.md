# ESPN API Endpoint Inventory for `dlt` Source (v2) - Revised Concurrency

**Version:** 2.1 **Date:** 2025-05-08 **Purpose:** This document catalogs the ESPN API endpoints for the NCAA Men's
College Basketball league (`mens-college-basketball`) and provides detailed guidance for implementing a `dlt` (Data Load
Tool) source. This version includes revised recommendations for concurrency, favoring `@dlt.defer` for detail fetching.

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

- **`dlt` Resource Name:** `seasons_resource`
- **Description:** Fetches season details. If `season_year` is given, fetches that specific season. Otherwise, lists all
  season `$ref`s and then fetches details.
- **Endpoint Path (List):** `/seasons`
- **Endpoint Path (Detail):** `/seasons/{season_id}` (from `$ref` or constructed if `season_year` is given)
- **Primary Key (`dlt`):** `id` (string, from API `year`)
- **Implementation Notes:**
  - If listing all seasons:
    - `seasons_resource` (or a `season_refs_lister_transformer`): Lists `$ref`s.
    - `season_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details for each `$ref`.
  - If `season_year` is provided, `seasons_resource` can directly fetch that season's detail.

### 2. Season Types

- **`dlt` Resource Names:** `season_type_refs_lister_transformer`, `season_type_detail_fetcher_transformer`
- **Parent Resource:** `seasons_resource` (or `season_detail_fetcher_transformer` if seasons are fetched one by one)
- **Description:** For each season, lists season type `$ref`s, then fetches details for each.
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/types`
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Primary Key (`dlt`):** `id`, `season_id_fk`
- **Implementation Notes:**
  - `season_type_refs_lister_transformer`: Takes `season_detail`, lists type `$ref`s, yields individual type `$ref`
    objects (augmented with `season_id_fk`).
  - `season_type_detail_fetcher_transformer` (using `@dlt.defer`): Takes a type `$ref` object, fetches details.

### 3. Weeks

- **`dlt` Resource Names:** `week_refs_lister_transformer`, `week_detail_fetcher_transformer`
- **Parent Resource:** `season_type_detail_fetcher_transformer`
- **Description:** For each season type, lists week `$ref`s, then fetches details for each.
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/types/{type_id}/weeks`
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Primary Key (`dlt`):** `id`, `type_id_fk`, `season_id_fk`
- **Implementation Notes:**
  - `week_refs_lister_transformer`: Takes `season_type_detail`, lists week `$ref`s, yields individual week `$ref`
    objects (augmented with `type_id_fk`, `season_id_fk`).
  - `week_detail_fetcher_transformer` (using `@dlt.defer`): Takes a week `$ref` object, fetches details. The API's
    `number` field maps to `id`.

### 4. Events (Games)

- **`dlt` Resource Names:** `event_refs_lister_transformer`, `event_detail_fetcher_transformer`
- **Parent Resource:** `week_detail_fetcher_transformer`
- **Description:** For each week, lists event `$ref`s, then fetches details for each.
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

- **`dlt` Resource Name:** `event_competitors_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Extracts competitor details _directly_ from the `event_detail.competitions[0].competitors` array. No
  new API call needed by this transformer itself.
- **Primary Key (`dlt`):** `event_id_fk`, `team_id`
- **Implementation Notes:** Iterates the `competitors` array in the parent event detail. Yields one record per
  competitor, including their various `$ref` URLs for subsequent detail transformers.

### 6. Event Scores (Final Score per Team)

- **`dlt` Resource Name:** `event_scores_detail_fetcher_transformer`
- **Parent Resource:** `event_competitors_transformer` (which yields competitor objects containing `score.$ref`)
- **Description:** Fetches the final score for each team in an event.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/score` (from
  `competitor.score.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`
- **Implementation Notes:**
  - Takes a `competitor_detail` object (containing `score_ref` and parent FKs).
  - Uses `@dlt.defer` to fetch score details from `score_ref` using `detail_client`.

### 7. Event Linescores (Score per Period)

- **`dlt` Resource Names:** `event_linescores_refs_lister_transformer`, `event_linescore_detail_fetcher_transformer` (if
  detail needed beyond list) OR simply `event_linescores_lister_transformer` if list provides all data.
- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the score for each team for each period (half) of the game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/linescores` (from
  `competitor.linescores.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `period`
- **Implementation Notes:**
  - `event_linescores_lister_transformer`: Takes `competitor_detail`, fetches the list of linescore items. If items are
    complete, yields them.
  - If linescore items are `$ref`s themselves (unlikely based on samples but possible), then:
    - `event_linescores_refs_lister_transformer`: Lists `$ref`s.
    - `event_linescore_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details.
  - Yields one record per team per period.

### 8. Event Team Statistics (Aggregates)

- **`dlt` Resource Name:** `event_team_statistics_detail_fetcher_transformer`
- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches aggregated team statistics for the game.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/statistics` (from
  `competitor.statistics.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_team_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `stat_name`
- **Implementation Notes:**
  - Takes `competitor_detail` (with `statistics_ref`).
  - Uses `@dlt.defer` to fetch the single JSON object containing all team stats categories.
  - Unnests `categories` and `stats` arrays into a tidy format.
  - **Crucially, yields the `$ref` URLs for each player's statistics** found in `splits.athletes` for the
    `event_player_statistics_refs_lister_transformer` to consume.

### 9. Event Player Statistics

- **`dlt` Resource Names:** `event_player_statistics_refs_lister_transformer`,
  `event_player_statistics_detail_fetcher_transformer`
- **Parent Resource:** `event_team_statistics_detail_fetcher_transformer` (yielding player stat `$ref` URLs)
- **Description:** Fetches detailed game statistics for each individual player.
- **Endpoint Path (Detail):**
  `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster/{athlete_id}/statistics/{split_id}` (from
  `$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_player_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `stat_name`
- **Implementation Notes:**
  - `event_player_statistics_refs_lister_transformer`: Takes the list of player stat `$ref`s (and necessary FKs like
    `event_id`, `team_id`) from the parent. Yields individual player stat `$ref` objects (augmented with FKs).
  - `event_player_statistics_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details for each player stat
    `$ref`. Parses `athlete_id` from the URL or ref object. Unnests categories and stats.

### 10. Event Team Leaders

- **`dlt` Resource Name:** `event_leaders_detail_fetcher_transformer`
- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the leading players for each team in key statistical categories for the game.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/leaders` (from
  `competitor.leaders.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_leaders`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `category_name`, `athlete_id_fk`
- **Implementation Notes:**
  - Takes `competitor_detail` (with `leaders_ref`).
  - Uses `@dlt.defer` to fetch leader data. Unnests categories and leaders.

### 11. Event Roster

- **`dlt` Resource Name:** `event_roster_detail_fetcher_transformer`
- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the roster of players for a team in a specific game.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster` (from
  `competitor.roster.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`
- **Implementation Notes:**
  - Takes `competitor_detail` (with `roster_ref`).
  - Uses `@dlt.defer` to fetch roster details. Yields one record per player on the game roster.

### 12. Event Team Records (Pre-Game)

- **`dlt` Resource Names:** `event_records_lister_transformer`
- **Parent Resource:** `event_competitors_transformer`
- **Description:** Fetches the team's record (overall, home, away, vs conf.) _before_ the game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/records` (from
  `competitor.records.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_pregame_records`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `record_type`, `stat_name`
- **Implementation Notes:**
  - `event_records_lister_transformer`: Takes `competitor_detail` (with `records_ref`). Fetches the list of pre-game
    record items. Unnests `stats` array. (This is a list endpoint, so `@dlt.defer` might not be directly on this one,
    but on a subsequent detail fetcher if the list items were refs themselves, which they don't appear to be).

### 13. Event Status

- **`dlt` Resource Name:** `event_status_detail_fetcher_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches current status for a game.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/status` (from
  `event_detail.competitions[0].status.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `status.$ref`).
  - Uses `@dlt.defer` to fetch status details.

### 14. Event Situation

- **`dlt` Resource Name:** `event_situation_detail_fetcher_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches live game situation details.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/situation` (from
  `event_detail.competitions[0].situation.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `situation.$ref`).
  - Uses `@dlt.defer` to fetch situation details.

### 15. Event Odds

- **`dlt` Resource Names:** `event_odds_lister_transformer` (or `event_odds_refs_lister_transformer` +
  `event_odd_detail_fetcher_transformer` if list items are refs)
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches betting odds for a game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/odds` (from
  `event_detail.competitions[0].odds.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `provider_id_fk`
- **Implementation Notes:**
  - `event_odds_lister_transformer`: Takes `event_detail` (with `odds.$ref`). Fetches list of odds items (one per
    provider). Unnest complex odds structures.
  - Requires a `providers` master table.

### 16. Event Broadcasts

- **`dlt` Resource Names:** `event_broadcasts_lister_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches broadcast information for a game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/broadcasts` (from
  `event_detail.competitions[0].broadcasts.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `media_id_fk`, `market_type`
- **Implementation Notes:**
  - `event_broadcasts_lister_transformer`: Takes `event_detail` (with `broadcasts.$ref`). Fetches list of broadcast
    items.
  - Requires a `media` master table.

### 17. Event Plays (Play-by-Play)

- **`dlt` Resource Names:** `event_plays_lister_transformer` (or `event_play_refs_lister_transformer` +
  `event_play_detail_fetcher_transformer` if list items are refs)
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches detailed play-by-play sequence.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/plays` (from
  `event_detail.competitions[0].plays.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `id` (play's own ID)
- **Implementation Notes:**
  - `event_plays_lister_transformer`: Takes `event_detail` (with `plays.$ref`). Fetches pages of play items. Handle
    `participants` array.
  - This can be a large resource.

### 18. Event Predictor

- **`dlt` Resource Name:** `event_predictor_detail_fetcher_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches ESPN's win probability and predicted score.
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/predictor` (from
  `event_detail.competitions[0].predictor.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`
- **Implementation Notes:**
  - Takes `event_detail` (with `predictor.$ref`).
  - Uses `@dlt.defer` to fetch predictor details.

### 19. Event Probabilities

- **`dlt` Resource Names:** `event_probabilities_lister_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches time-series win probability data.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/probabilities` (from
  `event_detail.competitions[0].probabilities.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `play_id_fk`
- **Implementation Notes:**
  - `event_probabilities_lister_transformer`: Takes `event_detail` (with `probabilities.$ref`). Fetches list of
    probability items.

### 20. Event Power Index

- **`dlt` Resource Names:** `event_powerindex_lister_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches team power index ratings (BPI/FPI) for the game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/powerindex` (from
  `event_detail.competitions[0].powerindex.$ref`)
- **Table Structure (`dlt` - Tidy Format Recommended):** `event_powerindex_stats`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `stat_name`
- **Implementation Notes:**
  - `event_powerindex_lister_transformer`: Takes `event_detail` (with `powerindex.$ref`). Fetches list of power index
    items (usually 2). Unnest `stats`.

### 21. Event Officials

- **`dlt` Resource Names:** `event_officials_lister_transformer`
- **Parent Resource:** `event_detail_fetcher_transformer`
- **Description:** Fetches officials assigned to the game.
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/officials` (from
  `event_detail.competitions[0].officials.$ref`)
- **Primary Key (`dlt`):** `event_id_fk`, `official_id`
- **Implementation Notes:**
  - `event_officials_lister_transformer`: Takes `event_detail` (with `officials.$ref`). Fetches list of official items.

---

## Master / Dimension Tables

Fetching details for master data from their respective `$ref` URLs should also use the "Lister" + "Detail Fetcher with
`@dlt.defer`" pattern if the primary way to discover them is via a list of refs, or directly with a "Detail Fetcher with
`@dlt.defer`" if individual refs are obtained from other resources.

### 22. Teams (Master)

- **`dlt` Resource Names:** `team_refs_lister_transformer` (e.g., per season), `team_detail_fetcher_transformer`
- **Parent Resource:** `seasons_resource` (or `season_detail_fetcher_transformer`)
- **Description:** Fetches details for all teams, typically discovered per season.
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/teams`
- **Endpoint Path (Detail):** `/seasons/{season_id}/teams/{team_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`, `season_id_fk`
- **Implementation Notes:**
  - `team_refs_lister_transformer`: Takes `season_detail` (or just `season_id`), lists team `$ref`s for that season.
    Yields individual team `$ref` objects (augmented with `season_id_fk`).
  - `team_detail_fetcher_transformer` (using `@dlt.defer`): Takes a team `$ref` object, fetches details.

### 23. Athletes (Master)

- **`dlt` Resource Names:** `athlete_refs_lister_transformer` (e.g., per season or per team/season),
  `athlete_detail_fetcher_transformer`
- **Parent Resource:** `seasons_resource` or `team_detail_fetcher_transformer`
- **Description:** Fetches details for athletes.
- **Endpoint Path (List of Refs):** `/seasons/{season_id}/athletes` OR `/seasons/{season_id}/teams/{team_id}/athletes`
- **Endpoint Path (Detail):** `/seasons/{season_id}/athletes/{athlete_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id` (assuming global uniqueness). If contextually seasonal, use `id`, `season_id_fk`.
- **Implementation Notes:**
  - `athlete_refs_lister_transformer`: Takes `season_detail` or `team_detail`, lists athlete `$ref`s. Yields individual
    athlete `$ref` objects (augmented with FKs).
  - `athlete_detail_fetcher_transformer` (using `@dlt.defer`): Takes an athlete `$ref` object, fetches details.

### 24. Venues (Master)

- **`dlt` Resource Name:** `venue_detail_fetcher_transformer`
- **Parent Resource:** Various (e.g., `team_detail_fetcher_transformer`, `event_detail_fetcher_transformer` that yield
  venue `$ref`s)
- **Description:** Fetches details for venues.
- **Endpoint Path (Detail):** `/venues/{venue_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - This would typically be a "Detail Fetcher" transformer using `@dlt.defer`.
  - It would consume venue `$ref`s yielded by other resources (like teams or events). A separate "lister" for all venues
    might not exist or be practical. Discovery is opportunistic.

### 25. Positions (Master)

- **`dlt` Resource Name:** `position_detail_fetcher_transformer`
- **Parent Resource:** Various (e.g., `athlete_detail_fetcher_transformer` that yields position `$ref`s)
- **Description:** Fetches details for player positions.
- **Endpoint Path (Detail):** `/positions/{position_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - Similar to Venues, a "Detail Fetcher" using `@dlt.defer` consuming `$ref`s from athlete data.

### 26. Coaches (Master & Seasonal)

- **`dlt` Resource Names:** `coach_refs_lister_transformer` (e.g., per team/season), `coach_detail_fetcher_transformer`,
  `coach_record_detail_fetcher_transformer`
- **Endpoint Paths:** `/coaches/{id}`, `/seasons/{s}/coaches/{id}`, `/seasons/{s}/teams/{t}/coaches` (list of refs).
- **Implementation Notes:**
  - Use "Lister" + "Detail Fetcher with `@dlt.defer`" for `/seasons/{s}/teams/{t}/coaches`.
  - Individual coach details/records from specific IDs/refs would use a "Detail Fetcher with `@dlt.defer`".

### 27. Awards (Master & Seasonal)

- **`dlt` Resource Names:** `awards_master_lister_transformer`, `award_master_detail_fetcher_transformer`,
  `season_award_refs_lister_transformer`, `season_award_detail_fetcher_transformer`
- **Endpoint Paths:** `/awards` (list master award refs), `/awards/{id}` (detail), `/seasons/{s}/awards` (list season
  award refs), `/seasons/{s}/awards/{id}` (detail).
- **Implementation Notes:** Apply "Lister" + "Detail Fetcher with `@dlt.defer`" pattern.

### 28. Franchises (Master)

- **`dlt` Resource Names:** `franchise_refs_lister_transformer`, `franchise_detail_fetcher_transformer`
- **Endpoint Path (List of Refs):** `/franchises`
- **Endpoint Path (Detail):** `/franchises/{franchise_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - `franchise_refs_lister_transformer`: Lists all franchise `$ref`s.
  - `franchise_detail_fetcher_transformer` (using `@dlt.defer`): Fetches details.

### 29. Providers (Master - Odds)

- **`dlt` Resource Name:** `provider_detail_fetcher_transformer`
- **Parent Resource:** `event_odds_lister_transformer` (which yields items containing `provider.$ref`)
- **Description:** Fetches details of odds providers.
- **Endpoint Path (Detail):** `/providers/{provider_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - Takes provider `$ref` from odds data.
  - Uses `@dlt.defer` to fetch details.

### 30. Media (Master - Broadcasts)

- **`dlt` Resource Name:** `media_detail_fetcher_transformer`
- **Parent Resource:** `event_broadcasts_lister_transformer` (which yields items containing `media.$ref`)
- **Description:** Fetches details of media outlets.
- **Endpoint Path (Detail):** `/media/{media_id}` (from `$ref`)
- **Primary Key (`dlt`):** `id`
- **Implementation Notes:**
  - Takes media `$ref` from broadcast data.
  - Uses `@dlt.defer` to fetch details.

---

## Other Potential Resources (Placeholder Samples & Future Consideration)

These endpoints had samples in the discovery but may be lower priority or require further investigation for their
integration strategy. The "Lister" + "Detail Fetcher with `@dlt.defer`" pattern should be considered if they follow
similar `$ref` structures.

- **Transactions:** `/transactions` (Sample `transactions_example.json` is empty)
- **Notes:** `/notes`, `/teams/{id}/notes`, `/seasons/{s}/athletes/{id}/notes` (Samples empty)
- **Injuries:** `/teams/{id}/injuries` (Sample empty)
- **Calendar:** `/calendar/*` (Samples exist, use depends on logic needed)
  - E.g., `/calendar/buyseason`, `/calendar/ondays`
- **Futures:** `/seasons/{s}/futures/*` (Samples exist, for betting futures)
  - E.g., `/seasons/{s}/futures`, `/seasons/{s}/futures/{id}`
- **Power Index (Season):** `/seasons/{s}/powerindex/*` (Samples exist, season-level BPI)
  - E.g., `/seasons/{s}/powerindex`, `/seasons/{s}/powerindex/{id}`
- **Rankings (Season/Overall):** `/rankings`, `/seasons/{s}/rankings/*` (Samples exist, overview of poll types)
  - E.g., `/rankings`, `/rankings/{id}`, `/seasons/{s}/rankings`, `/seasons/{s}/types/{t}/weeks/{w}/rankings`,
    `/seasons/{s}/types/{t}/weeks/{w}/rankings/{id}`
- **Groups/Conferences:** `/seasons/{s}/types/{t}/groups/*` (Samples exist, defines conference structure)
  - E.g., `/seasons/{s}/types/{t}/groups`, `/seasons/{s}/types/{t}/groups/{id}`
- **Standings:** `/seasons/{s}/types/{t}/groups/{g}/standings/*` (Samples exist, detailed standings data)
  - E.g., `/seasons/{s}/types/{t}/groups/{g}/standings`, `/seasons/{s}/types/{t}/groups/{g}/standings/{id}` (standings
    are usually by season, not specific ID for the "standings" entity itself).
- **Tournaments/Bracketology:** `/tournaments/*` (Samples exist, for tournament-specific views)
  - E.g., `/tournaments`, `/tournaments/{id}`

---
