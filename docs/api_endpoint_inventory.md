# ESPN API Endpoint Inventory for `dlt` Source

This document catalogs the ESPN API endpoints for the NCAA Men's College Basketball league and provides guidance for implementing a `dlt` (Data Load Tool) source to ingest this data.

**Base URL:** `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball`

---

## General `dlt` Implementation Guidelines

- **`$ref` Handling:** The API extensively uses `$ref` fields with full URLs. Parent `dlt` resources will typically list these references, and child transformer resources will fetch the details from these URLs.
- **Client Configuration:**
    - Use a `RESTClient` with `PageNumberPaginator` (page_param="page", total_path="pageCount", base_page=1) for list endpoints that return `$ref` items. Data selector will typically be `"items"`.
    - Use a `RESTClient` with `base_url=None` for fetching details from the absolute `$ref` URLs.
- **Primary Keys & Foreign Keys:**
    - Many entities will use composite primary keys derived from path parameters.
    - Foreign keys (`_fk`) should be added to child resources to link back to their parents.
    - All IDs should be consistently stored as strings.
- **Pagination:** List endpoints are paginated. Use `limit=1000` (or a configurable `API_LIMIT`) in API requests.
- **Error Handling:** Implement robust error handling, logging, and retries for API requests.
- **Incremental Loading:** For events, the "Status" endpoint can indicate if an event is "final," allowing for incremental loading strategies by tracking processed and final `event_id`s.
- **API Path Structure for Event Sub-Resources:** For many event-specific sub-resources (e.g., scores, statistics), the `event_id` is used in the path where a unique `competition_id` might conventionally appear (e.g., `/events/{event_id}/competitions/{event_id}/...`). This `event_id` effectively serves as the `competition_id` in these contexts.

---

## Core Resources (Sequential Discovery)

### 1. Seasons

- **`dlt` Resource Name:** `seasons_resource`
- **Endpoint Path (List):** `/seasons`
- **Endpoint Path (Detail):** `/seasons/{season_id}` (from `$ref`)
- **Request Parameters (List):** `limit` (query)
- **Key Fields (Detail):**
    - `year` (map to `id` in `dlt`, primary key)
    - `startDate`, `endDate`
    - `displayName`
    - `types.$ref` (link to list Season Types)
- **Primary Key (`dlt`):** `id` (string, derived from API `year`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_example.json` (list), `seasons_-season-id_example.json` (detail)
- **Implementation Notes:**
    - The resource first lists season references.
    - Then, it fetches details for each season from its `$ref` URL.
    - The API's `year` field (e.g., 2024) is used as the `id` for the `seasons` table.
    - Supports fetching a single `season_year` if provided to the source.

---

### 2. Season Types

- **`dlt` Resource Name:** `season_types_transformer`
- **Parent Resource:** `seasons_resource`
- **Endpoint Path (List):** `/seasons/{season_id}/types` (from `season_detail.types.$ref` or constructed)
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Request Parameters (List):** `limit` (query)
- **Key Fields (Detail):**
    - `id` (map to `id` in `dlt`)
    - `name`, `abbreviation`
    - `year`, `startDate`, `endDate`
    - `hasStandings`, `hasGroups` (booleans)
    - `weeks.$ref` (link to list Weeks for this season type)
- **Primary Key (`dlt`):** `id`, `season_id_fk` (composite)
- **Foreign Keys (`dlt`):** `season_id_fk` (references `seasons.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_example.json` (list), `seasons_-season-id-_types_-type-id_example.json` (detail)
- **Implementation Notes:**
    - Transforms data from `seasons_resource`.
    - For each season, fetches the list of its season type references.
    - Fetches details for each season type.
    - `season_id_fk` is the `id` from the parent season.
    - The API's `id` for season type (e.g., "1", "2") becomes the `id` for this table.

---

### 3. Weeks

- **`dlt` Resource Name:** `weeks_transformer`
- **Parent Resource:** `season_types_transformer`
- **Endpoint Path (List):** `/seasons/{season_id}/types/{type_id}/weeks` (from `season_type_detail.weeks.$ref` or constructed)
- **Endpoint Path (Detail):** (from `$ref` in the list response)
- **Request Parameters (List):** `limit` (query)
- **Key Fields (Detail):**
    - `number` (map to `id` in `dlt`)
    - `startDate`, `endDate`, `text`
    - `events.$ref` (link to list Events for this week)
- **Primary Key (`dlt`):** `id`, `type_id_fk`, `season_id_fk` (composite)
- **Foreign Keys (`dlt`):**
    - `type_id_fk` (references `season_types.id`)
    - `season_id_fk` (references `seasons.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_weeks_example.json` (list), `seasons_-season-id-_types_-type-id-_weeks_-week-id_example.json` (detail)
- **Implementation Notes:**
    - Transforms data from `season_types_transformer`.
    - For each season type, fetches its list of week references.
    - Fetches details for each week.
    - The API's `number` field for a week (e.g., 1, 2) is mapped to `id`.
    - `type_id_fk` is the `id` from the parent season type.
    - `season_id_fk` is the `season_id_fk` from the parent season type.

---

### 4. Events (Games)

- **`dlt` Resource Name:** `events_transformer`
- **Parent Resource:** `weeks_transformer`
- **Endpoint Path (List):** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/events` (from `week_detail.events.$ref` or constructed)
- **Endpoint Path (Detail):** `/events/{event_id}` (from `$ref` in the list response)
- **Request Parameters (List):** `limit` (query)
- **Key Fields (Detail):**
    - `id` (primary key for this table)
    - `date`, `name`, `shortName`
    - `season.year`, `season.type`, `week.number` (contextual)
    - `competitions[0].id` (Note: API uses event_id here as well)
    - `competitions[0].competitors` (array, contains home and away team info)
        - `id` (team_id), `uid`, `type` ("home"/"away"), `winner` (boolean)
        - `team.$ref` (link to team details, e.g., `/seasons/{year}/teams/{team_id}`)
        - `score.$ref` (link to team score details)
        - `linescores.$ref` (link to team linescores)
        - `statistics.$ref` (link to team game statistics)
        - `leaders.$ref` (link to team game leaders)
        - `roster.$ref` (link to team game roster)
        - `records.$ref` (link to team records pre-game)
    - `competitions[0].venue.$ref` (link to venue details)
    - `competitions[0].status.$ref` (link to game status details)
    - `competitions[0].situation.$ref` (link to game situation details)
    - `competitions[0].odds.$ref` (link to game odds details)
    - `competitions[0].broadcasts.$ref` (link to game broadcast details)
    - `competitions[0].plays.$ref` (link to game play-by-play list)
    - `competitions[0].predictor.$ref` (link to game predictor details)
    - `competitions[0].powerindex.$ref` (link to game power index details)
    - `competitions[0].probabilities.$ref` (link to game probabilities details)
    - `competitions[0].officials.$ref` (link to game officials list)
- **Primary Key (`dlt`):** `id` (string, from API `id`)
- **Foreign Keys (`dlt`):**
    - `week_id_fk` (references `weeks.id`)
    - `type_id_fk` (references `season_types.id`)
    - `season_id_fk` (references `seasons.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_example.json` (list), `events_-event-id_example.json` (detail, core event)
- **Implementation Notes:**
    - Transforms data from `weeks_transformer`.
    - For each week, fetches its list of event references.
    - Fetches details for each event from `/events/{event_id}`.
    - Store the main event ID as `id`. Add foreign keys `week_id_fk`, `type_id_fk`, `season_id_fk`.
    - The `competitions` array usually has one item. Its `id` is the same as the parent `event_id`.
    - Competitor data within `competitions[0].competitors` should be extracted/flattened. Consider a separate `event_competitors` table or denormalizing into `events`. For `dlt` with `max_table_nesting=0`, flattening is preferred if structure is consistent.
    - Many fields are `$ref` links to game-specific sub-resources. These will be handled by separate transformer resources.
    - Concurrent fetching of event details is recommended (see `ThreadPoolExecutor` in `espn_source.py`).

---

## Event Sub-Resources (Game Details)

These resources are typically children of an `event_detail` object. The parent `event_id` and relevant `team_id` (competitor ID) will be crucial foreign keys. The `competition_id` in API paths is often the same as `event_id`.

### 5. Event Competitions (Detail for each team in a game)
- **`dlt` Resource Name:** `event_competitors_transformer` (Example name)
- **Parent Resource:** `events_transformer` (Iterates over `event_detail.competitions[0].competitors`)
- **Endpoint Path (Detail):** N/A (Data is within `events_transformer` parent, or use `/events/{event_id}/competitions/{event_id}/competitors/{team_id}` if a direct ref exists, see `event_competitors_-competitor-id_example.json`)
- **Key Fields:**
    - `id` (team_id from competitor entry, map to `team_id` in `dlt`)
    - `uid` (team_uid)
    - `type` ("home" or "away")
    - `order` (1 for home, 2 for away, or similar)
    - `winner` (boolean)
    - `displayName`, `abbreviation`, `logo`, `color`, `alternateColor`
    - `team.$ref` (link to specific team details for the season, e.g. `/seasons/{year}/teams/{team_id}`)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id` (composite)
- **Foreign Keys (`dlt`):** `event_id_fk` (references `events.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id_example.json` (shows structure for a single competitor). Data is also nested in `events_-event-id_example.json`.
- **Implementation Notes:**
    - This resource processes each competitor (team) within an event.
    - The `id` from the competitor entry in the `event_detail` is the `team_id`.
    - `event_id_fk` is the `id` from the parent event.
    - This table links an event to the participating teams and their roles (home/away).

---

### 6. Team Details (Season Specific)

- **`dlt` Resource Name:** `teams_transformer` (or similar, may need careful handling if called from multiple places)
- **Parent Resource:** Could be `event_competitors_transformer` (from `competitor.team.$ref`) or another process that lists all teams.
- **Endpoint Path (Detail):** `/seasons/{season_id}/teams/{team_id}` (from `$ref`)
- **Key Fields:**
    - `id` (team_id, map to `id` in `dlt`)
    - `uid`, `slug`, `location`, `name`, `nickname`, `displayName`, `shortDisplayName`, `abbreviation`
    - `color`, `alternateColor`, `logo`
    - `groups.$ref`
    - `venue.$ref`
    - `parent.$ref` (if it's a sub-team/program)
    - `links` (array of various links, e.g., to roster, schedule)
    - `franchise.$ref`
    - `nextEvent[0].$ref` (link to next event if available)
- **Primary Key (`dlt`):** `id`, `season_id_fk` (composite, assuming `season_id` is part of the context or path)
- **Foreign Keys (`dlt`):** `season_id_fk`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_teams_-team-id_example.json`
- **Implementation Notes:**
    - Fetches detailed information about a specific team for a given season.
    - The `$ref` often looks like `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/2024/teams/2509`. The `season_id` (2024) and `team_id` (2509) are key.
    - Ensure `season_id_fk` is captured.

---

### 7. Score (Final Score per Team per Game)

- **`dlt` Resource Name:** `event_scores_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.score.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/score`
- **Key Fields:**
    - `value` (the actual score)
    - `winner` (boolean, sometimes present)
    - `displayValue` (string representation of score)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`, or use the competitor's `team_id` directly)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_score_example.json`
- **Implementation Notes:**
    - Fetches the final score for a specific team in a specific game.
    - `event_id_fk` and `team_id_fk` are parsed from the `$ref` URL.

---

### 8. Linescores (Score per Period per Team per Game)

- **`dlt` Resource Name:** `event_linescores_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.linescores.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/linescores`
- **Paginated:** True (but typically few items, e.g., 2 for halves)
- **Key Fields (Item):**
    - `value` (score for the period)
    - `period` (integer, e.g., 1 for 1st half, 2 for 2nd half)
    - `displayValue`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `period` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_linescores_example.json`
- **Implementation Notes:**
    - Fetches period scores for a team in a game. Each item in the `items` array is a period.
    - `event_id_fk`, `team_id_fk` from URL. `period` from item.

---

### 9. Team Game Statistics (Aggregates)

- **`dlt` Resource Name:** `event_team_statistics_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.statistics.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/statistics`
- **Key Fields:**
    - `splits.categories` (array, e.g., "defensive", "general", "offensive")
        - `name` (category name)
        - `stats` (array of stat objects)
            - `name` (stat short name, e.g., "BLK")
            - `displayName` (e.g., "Blocks")
            - `value`
            - `displayValue`
    - `splits.athletes[0].statistics.$ref` (link to player-specific game stats) - **IMPORTANT for Player Game Stats**
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `category_name`, `stat_name` (composite, very granular) or consider structuring differently. A flatter structure might be one row per stat: `event_id_fk, team_id_fk, stat_name, stat_value, category_name`.
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_statistics_example.json`
- **Implementation Notes:**
    - This endpoint provides aggregated team statistics for the game, broken down by categories.
    - It also contains `$ref` links within `splits.athletes` to individual player statistics for the game. These are crucial for the `Player Game Statistics` resource.
    - Decide on table structure: one table with many columns (stat names as columns) or a long/tidy format (one row per stat). Tidy format is generally more flexible for `dlt`.
    - If using tidy format, PK would be `event_id_fk, team_id_fk, stat_name`.

---

### 10. Player Game Statistics

- **`dlt` Resource Name:** `event_player_statistics_transformer`
- **Parent Resource:** `event_team_statistics_transformer` (from `splits.athletes[...].statistics.$ref`) OR `event_roster_transformer` (from `roster_entry.statistics.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster/{athlete_id}/statistics/{split_id}` (typically `split_id` is '0')
- **Key Fields:**
    - `athlete.$ref` (link to athlete detail) -> athlete_id can be parsed from this or the request URL.
    - `splits.categories` (array, similar to team stats)
        - `name` (category name)
        - `stats` (array of stat objects)
            - `name`, `displayName`, `value`, `displayValue`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk`, `stat_name` (composite, assuming tidy format)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
    - `athlete_id_fk` (references `athletes.id` - an Athlete master table would be needed)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_roster_-roster-id-_statistics_-statistic-id_example.json`
- **Implementation Notes:**
    - Provides individual player statistics for a specific game.
    - `event_id`, `team_id`, `athlete_id` are parsed from the input `$ref` URL. `split_id` is usually '0'.
    - An `athletes` master table (see Athletes section) would be beneficial. `athlete_id_fk` links to it.
    - Similar to team stats, decide on wide vs. tidy format. Tidy recommended.

---

### 11. Team Game Leaders

- **`dlt` Resource Name:** `event_leaders_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.leaders.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/leaders`
- **Key Fields:**
    - `categories` (array, e.g., "points", "assists", "rebounds")
        - `name` (category name)
        - `leaders` (array of leader objects)
            - `value` (stat value)
            - `athlete.$ref` (link to athlete, parse `athlete_id`)
            - `statistics.$ref` (link to full player game stats, redundant if already fetching)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `category_name`, `athlete_id_fk` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
    - `athlete_id_fk` (references `athletes.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_leaders_example.json`
- **Implementation Notes:**
    - Lists top players for a team in various statistical categories for the game.
    - Parse `athlete_id` from `athlete.$ref`.

---

### 12. Game Roster

- **`dlt` Resource Name:** `event_roster_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.roster.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster`
- **Key Fields:**
    - `entries` (array of roster entries/players)
        - `playerId` (map to `athlete_id` or `player_id`)
        - `jersey`, `starter` (boolean), `didNotPlay` (boolean), `active` (boolean)
        - `displayName`
        - `athlete.$ref` (link to athlete detail, parse `athlete_id`)
        - `position.$ref` (link to position detail, parse `position_id`)
        - `statistics.$ref` (link to player game stats - can be used to trigger player stat fetching)
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `athlete_id_fk` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
    - `athlete_id_fk` (references `athletes.id`)
    - `position_id_fk` (references `positions.id` - a Positions master table would be needed)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_roster_example.json`
- **Implementation Notes:**
    - Provides the game-specific roster for a team.
    - Parse `athlete_id` from `athlete.$ref` or use `playerId`.
    - Parse `position_id` from `position.$ref`.
    - This can be another entry point to fetch individual player game statistics via `statistics.$ref`.

---

### 13. Team Records (Up to Game Time)

- **`dlt` Resource Name:** `event_records_transformer`
- **Parent Resource:** `event_competitors_transformer` (from `competitor.records.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/records`
- **Paginated:** True (but typically a small, fixed set of record types)
- **Key Fields (Item):**
    - `id` (record item ID, seems internal)
    - `name` (e.g., "overall", "home", "road", "vsconf")
    - `abbreviation`
    - `type` (e.g., "total", "home", "road", "vsconf")
    - `summary` (W-L string, e.g., "10-5")
    - `stats` (array of stat objects for the record type)
        - `name` (e.g., "wins", "losses", "ties", "winPercent", "pointsFor", "pointsAgainst", "streak")
        - `value`
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `record_type` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_competitors_-competitor-id-_records_example.json`
- **Implementation Notes:**
    - Provides various W-L records for a team leading up to the game.
    - Each item in `items` is a different record type. Use `type` or `name` field from item as part of the PK.
    - Stats need to be unnested or flattened. A tidy format for stats (one row per record_type per stat_name) could be: `event_id_fk, team_id_fk, record_type, stat_name, stat_value`.

---

### 14. Game Status

- **`dlt` Resource Name:** `event_status_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].status.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/status`
- **Key Fields:**
    - `clock` (decimal, seconds remaining in period)
    - `displayClock` (string, e.g., "20:00")
    - `period` (integer)
    - `type.id`
    - `type.name` (e.g., "STATUS_SCHEDULED", "STATUS_FINAL", "STATUS_IN_PROGRESS")
    - `type.state` ("pre", "in", "post")
    - `type.completed` (boolean)
    - `type.description`
    - `type.detail`
    - `type.shortDetail`
- **Primary Key (`dlt`):** `event_id_fk`
- **Foreign Keys (`dlt`):** `event_id_fk` (references `events.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_status_example.json`
- **Implementation Notes:**
    - Provides detailed status of the game (scheduled, live, final).
    - Crucial for incremental loading strategies (checking `type.completed` or `type.state == "post"`).
    - `event_id_fk` is parsed from the `$ref` URL.

---

### 15. Game Situation (Live Game Data)

- **`dlt` Resource Name:** `event_situation_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].situation.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/situation`
- **Key Fields:**
    - `lastPlay.$ref` (link to the last play detail)
    - `down` (football specific, likely null for basketball)
    - `yardLine` (football specific)
    - `distance` (football specific)
    - `downDistanceText` (football specific)
    - `shortDownDistanceText` (football specific)
    - `possession.$ref` (link to team in possession)
    - `possessionText`
    - `homeTimeouts`, `awayTimeouts` (remaining timeouts)
    - `homeFouls`, `awayFouls` (team fouls if tracked this way, or bonus status)
- **Primary Key (`dlt`):** `event_id_fk`
- **Foreign Keys (`dlt`):** `event_id_fk` (references `events.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_situation_example.json`
- **Implementation Notes:**
    - Describes current game state, most relevant for live games (timeouts, fouls, possession).
    - `event_id_fk` from URL.

---

### 16. Game Odds

- **`dlt` Resource Name:** `event_odds_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].odds.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/odds`
- **Paginated:** True (one item per odds provider)
- **Key Fields (Item):**
    - `provider.$ref` (link to provider detail, parse `provider_id`)
    - `provider.name`, `provider.priority`
    - `details` (e.g., "Line: DUKE -7.5")
    - `overUnder`
    - `spread`
    - `awayTeamOdds.moneyLine`, `awayTeamOdds.spreadOdds`, `awayTeamOdds.team.$ref`
    - `homeTeamOdds.moneyLine`, `homeTeamOdds.spreadOdds`, `homeTeamOdds.team.$ref`
    - Opening, current, live odds structures within `awayTeamOdds` / `homeTeamOdds`.
- **Primary Key (`dlt`):** `event_id_fk`, `provider_id_fk` (composite)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `provider_id_fk` (references `providers.id` - a Providers master table needed)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_odds_example.json` (list), `events_-event-id-_competitions_-competition-id-_odds_-odd-id_example.json` (detail for one provider)
- **Implementation Notes:**
    - Provides betting odds from multiple providers. Each item in `items` is for one provider.
    - Parse `provider_id` from `provider.$ref`.
    - A `providers` master table would be useful.
    - Odds data is complex and nested (current, open, close). Flatten as needed.

---

### 17. Game Broadcasts

- **`dlt` Resource Name:** `event_broadcasts_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].broadcasts.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/broadcasts`
- **Paginated:** True (but often just one or a few items)
- **Key Fields (Item):**
    - `market` (e.g., "national", "home", "away")
    - `media.$ref` (link to media/network detail, parse `media_id`)
    - `media.shortName`
    - `type.id`, `type.shortName` (e.g., "TV", "Web")
    - `lang`, `region`
- **Primary Key (`dlt`):** `event_id_fk`, `media_id_fk`, `market`, `type_shortName` (composite, needs refinement based on uniqueness)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `media_id_fk` (references `media.id` - a Media master table needed)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_broadcasts_example.json`
- **Implementation Notes:**
    - Lists broadcast information for the game. Each item in `items` is a broadcast.
    - Parse `media_id` from `media.$ref`. A `media` master table would be useful.

---

### 18. Game Play-by-Play

- **`dlt` Resource Name:** `event_plays_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].plays.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/plays`
- **Paginated:** True (potentially many plays per game)
- **Key Fields (Item):**
    - `id` (play ID, unique within game)
    - `sequenceNumber`
    - `type.id`, `type.text` (e.g., "Jump Shot", "Foul", "Timeout")
    - `text` (description of the play)
    - `awayScore`, `homeScore` (score after this play)
    - `period.number`
    - `clock.displayValue`
    - `scoringPlay` (boolean)
    - `shootingPlay` (boolean)
    - `team.$ref` (link to team associated with play, parse `team_id`)
    - `participants` (array of players involved)
        - `athlete.$ref` (parse `athlete_id`)
        - `type` (role in play, e.g., "shooter", "rebounder", "fouler")
- **Primary Key (`dlt`):** `event_id_fk`, `id` (composite - `id` is play's own ID)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `team_id_fk` (references `teams.id`, for the team associated with the play)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_plays_example.json` (list), `events_-event-id-_competitions_-competition-id-_plays_-play-id_example.json` (detail of one play)
- **Implementation Notes:**
    - Fetches the detailed play-by-play log.
    - `id` is the play's own unique identifier.
    - `participants` array needs to be handled. Consider a separate `play_participants` table: `event_id_fk, play_id_fk, athlete_id_fk, participant_type`. PK: `event_id_fk, play_id_fk, athlete_id_fk`.
    - Parse `team_id` from `team.$ref` if present.

---

### 19. Game Predictor / Win Probability (Often tied together)

- **`dlt` Resource Name:** `event_predictor_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].predictor.$ref`)
- **Endpoint Path (Detail):** `/events/{event_id}/competitions/{event_id}/predictor`
- **Key Fields:**
    - `header` (e.g., "ESPN Win Probability")
    - `homeTeam.id`, `homeTeam.abbreviation`, `homeTeam.logo`
    - `homeTeam.gameProjection` (projected score)
    - `homeTeam.teamChanceLoss`, `homeTeam.teamChanceTie` (usually 0 for bball)
    - `awayTeam` (similar fields)
    - `lastPlay.$ref`
    - `seconds`, `overUnder`, `spread`
- **Primary Key (`dlt`):** `event_id_fk`
- **Foreign Keys (`dlt`):** `event_id_fk` (references `events.id`)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_predictor_example.json`
- **Implementation Notes:**
    - Provides pre-game or live game predictions/probabilities.

---

### 20. Game Probabilities (More detailed than predictor, e.g. time-series)

- **`dlt` Resource Name:** `event_probabilities_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].probabilities.$ref`)
- **Endpoint Path (List or Detail):** `/events/{event_id}/competitions/{event_id}/probabilities`
    - The sample `events_-event-id-_competitions_-competition-id-_probabilities_example.json` (list) is large and paginated.
    - `events_-event-id-_competitions_-competition-id-_probabilities_-probabilitie-id_example.json` (detail) suggests items might have own IDs.
- **Paginated:** True (if it's a list)
- **Key Fields (Item if list, or top-level if detail):**
    - `gameId`
    - `homeWinPercentage`, `awayWinPercentage`, `tiePercentage`
    - `playId` (links probability to a specific play)
    - `secondsRemaining`
- **Primary Key (`dlt`):** `event_id_fk`, `play_id_fk` (if tied to plays and this is a list of play-specific probabilities) or `event_id_fk`, `probability_id` (if items have own IDs).
- **Foreign Keys (`dlt`):** `event_id_fk`, potentially `play_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** `events_-event-id-_competitions_-competition-id-_probabilities_example.json`, `events_-event-id-_competitions_-competition-id-_probabilities_-probabilitie-id_example.json`
- **Implementation Notes:**
    - Provides win probabilities, often as a time series across the game.
    - If it's a list of probabilities tied to `playId`, this would be a child of `event_plays_transformer` or fetched in parallel and linked.

---

### 21. Game Power Index

- **`dlt` Resource Name:** `event_powerindex_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].powerindex.$ref`)
- **Endpoint Path (List or Detail):** `/events/{event_id}/competitions/{event_id}/powerindex`
    - `events_-event-id-_competitions_-competition-id-_powerindex_example.json` (list, paginated)
    - `events_-event-id-_competitions_-competition-id-_powerindex_-powerindex-id_example.json` (detail)
- **Key Fields (Item if list, or top-level if detail):**
    - `team.$ref` (parse `team_id`)
    - `powerIndex`, `rank`, `trend`
    - `previous.powerIndex`, `previous.rank`
    - `high.powerIndex`, `low.powerIndex`
    - `description`
    - `type` (e.g., "pregame", "live", "postgame")
- **Primary Key (`dlt`):** `event_id_fk`, `team_id_fk`, `type` (composite, assuming type makes it unique per team per event)
- **Foreign Keys (`dlt`):** `event_id_fk`, `team_id_fk`
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** `events_-event-id-_competitions_-competition-id-_powerindex_example.json`, `events_-event-id-_competitions_-competition-id-_powerindex_-powerindex-id_example.json`
- **Implementation Notes:**
    - Provides ESPN's FPI/BPI ratings related to the game for each team.

---

### 22. Game Officials

- **`dlt` Resource Name:** `event_officials_transformer`
- **Parent Resource:** `events_transformer` (from `event_detail.competitions[0].officials.$ref`)
- **Endpoint Path (List):** `/events/{event_id}/competitions/{event_id}/officials`
- **Paginated:** True (but usually few officials)
- **Key Fields (Item):**
    - `id` (official's unique ID)
    - `displayName`
    - `position.$ref` (link to official's position/role, parse `official_position_id`)
    - `type` (e.g., "Referee", "Umpire") - this might be from `position.name`
    - `jersey` (if applicable)
    - `athlete.$ref` (sometimes officials are linked to athlete profiles)
- **Primary Key (`dlt`):** `event_id_fk`, `official_id` (composite, where `official_id` is `item.id`)
- **Foreign Keys (`dlt`):**
    - `event_id_fk` (references `events.id`)
    - `official_position_id_fk` (references a hypothetical `official_positions` table)
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `events_-event-id-_competitions_-competition-id-_officials_example.json` (list), `events_-event-id-_competitions_-competition-id-_officials_-official-id_example.json` (detail of one official)
- **Implementation Notes:**
    - Lists officials for the game. Each item in `items` is an official.
    - `item.id` is the official's own identifier.

---

## Master / Dimension Tables (Less frequently changing)

These resources represent entities that are often referenced by transactional data (like games) but don't change as frequently. They should be loaded and merged.

### 23. Athletes (Master List)

- **`dlt` Resource Name:** `athletes_resource` or `athletes_transformer`
- **Parent Resource:** Can be called from `event_roster_transformer` (from `athlete.$ref`), `event_player_statistics_transformer`, etc. or have its own discovery mechanism if one exists (e.g., `/athletes` if there's a global list). Sample `seasons_-season-id-_teams_-team-id-_athletes_example.json` lists athletes for a team-season.
- **Endpoint Path (Detail):** Example: `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/2024/athletes/{athlete_id}` (from `$ref`) OR potentially just `/athletes/{athlete_id}` if globally unique and such an endpoint exists.
    - Based on `seasons_-season-id-_athletes_-athlete-id_example.json`, the path is `/seasons/{season_id}/athletes/{athlete_id}`.
- **Key Fields (Detail):**
    - `id` (athlete's unique ID)
    - `uid`, `guid`
    - `displayName`, `shortName`, `fullName`
    - `birthPlace.city`, `birthPlace.state`, `birthPlace.country`
    - `displayDOB`
    - `age`
    - `height`, `weight`, `displayHeight`, `displayWeight`
    - `jersey`
    - `position.$ref` (link to position, parse `position_id`)
    - `team.$ref` (link to current team, parse `team_id` - may be season specific)
    - `headshot.$ref`, `headshot.href`
    - `injuries` (array, if any)
    - `experience.years`
    - `active` (boolean)
- **Primary Key (`dlt`):** `id` (athlete's API ID). If season-specific athlete records: `id`, `season_id_fk`.
- **Foreign Keys (`dlt`):** `position_id_fk`, potentially `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:**
    - `seasons_-season-id-_teams_-team-id-_athletes_example.json` (list for a team-season)
    - `seasons_-season-id-_athletes_-athlete-id_example.json` (detail for a season-athlete context)
- **Implementation Notes:**
    - Represents individual athletes. This table is crucial for linking player stats, roster info, etc.
    - The `/seasons/{season_id}/athletes/{athlete_id}` path suggests that athlete records might be contextualized by season in some views. A global `/athletes/{athlete_id}` might also exist or be preferred if athlete IDs are globally unique and that endpoint provides comprehensive data. For now, assume `id` is globally unique for the athlete.
    - If athlete data is indeed season-specific from the API, then `(id, season_id_fk)` would be the PK.

---

### 24. Positions (Master List)

- **`dlt` Resource Name:** `positions_resource` or `positions_transformer`
- **Parent Resource:** Called from `event_roster_transformer` (from `position.$ref`), `athletes_resource`, etc.
- **Endpoint Path (Detail):** `/positions/{position_id}` (from `$ref`)
- **Key Fields (Detail):**
    - `id`
    - `name` (e.g., "Guard", "Forward", "Center")
    - `displayName`, `abbreviation`
    - `leaf` (boolean)
    - `parent.$ref` (for hierarchical positions)
- **Primary Key (`dlt`):** `id`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `positions_-position-id_example.json`
- **Implementation Notes:**
    - Master list of player positions.

---

### 25. Venues (Master List)

- **`dlt` Resource Name:** `venues_resource` or `venues_transformer`
- **Parent Resource:** Called from `events_transformer` (from `venue.$ref`), `teams_transformer`, etc.
- **Endpoint Path (Detail):** `/venues/{venue_id}` (from `$ref`)
- **Key Fields (Detail):**
    - `id` (venue's unique ID)
    - `fullName`, `shortName`
    - `address.city`, `address.state`, `address.zipCode`
    - `capacity`, `grass` (boolean), `indoor` (boolean)
    - `images` (array of image links)
- **Primary Key (`dlt`):** `id`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `venues_-venue-id_example.json`
- **Implementation Notes:**
    - Master list of venues where games are played.

---

### 26. Providers (Betting Odds Providers)

- **`dlt` Resource Name:** `providers_resource` or `providers_transformer`
- **Parent Resource:** Called from `event_odds_transformer` (from `provider.$ref`).
- **Endpoint Path (Detail):** `/providers/{provider_id}` (from `$ref`)
- **Key Fields (Detail):**
    - `id`
    - `name`
    - `priority`
    - `regions` (array of strings)
- **Primary Key (`dlt`):** `id`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `providers_-provider-id_example.json`
- **Implementation Notes:**
    - Master list of betting odds providers.

---

### 27. Media (Broadcast Networks/Channels)

- **`dlt` Resource Name:** `media_resource` or `media_transformer`
- **Parent Resource:** Called from `event_broadcasts_transformer` (from `media.$ref`).
- **Endpoint Path (Detail):** `/media/{media_id}` (from `$ref`)
- **Key Fields (Detail):**
    - `id`
    - `sourceId` (internal ID)
    - `name`, `shortName`
    - `type`
    - `outlets` (array, if applicable)
- **Primary Key (`dlt`):** `id`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `media_-media-id_example.json`
- **Implementation Notes:**
    - Master list of media outlets (TV channels, web streams).

---

### 28. Coaches (Master List & Seasonal Roles)

- **`dlt` Resource Names:** `coaches_master_resource`, `season_coaches_resource`
- **Parent Resource:** Various, e.g., from team details or specific coach endpoints.
- **Endpoint Paths:**
    - General Coach Info: `/coaches/{coach_id}` (Sample: `coaches_-coache-id_example.json`)
    - Coach Record (overall/by season): `/coaches/{coach_id}/record` (Sample: `coaches_-coache-id-_record_-record-id_example.json` seems to be specific record, maybe a list endpoint `/coaches/{coach_id}/records` exists?)
    - Coach for a specific season: `/seasons/{season_id}/coaches/{coach_id}` (Sample: `seasons_-season-id-_coaches_-coache-id_example.json`)
    - Coach record for a specific season/type: `/seasons/{season_id}/types/{type_id}/coaches/{coach_id}/record` (Sample: `seasons_-season-id-_types_-type-id-_coaches_-coache-id-_record_example.json`)
    - List coaches for a team in a season: `/seasons/{season_id}/teams/{team_id}/coaches` (Sample: `seasons_-season-id-_teams_-team-id-_coaches_example.json`)
- **Key Fields (General Coach Info - `coaches_-coache-id_example.json`):**
    - `id` (coach's unique ID)
    - `uid`, `guid`
    - `firstName`, `lastName`, `displayName`
    - `experience`, `dateOfBirth`
    - `team.$ref` (current team)
    - `headshot.$ref`
- **Key Fields (Coach Season Record - `seasons_-season-id-_types_-type-id-_coaches_-coache-id-_record_example.json`):**
    - `team.$ref`
    - `type` (e.g., "Overall", "Conference")
    - `summary` (W-L)
    - `wins`, `losses`, `ties`, `gamesPlayed`, `winPercent`
- **Primary Keys (`dlt`):**
    - `coaches_master`: `id`
    - `coach_records`: `coach_id_fk`, `season_id_fk`, `type_id_fk`, `record_type_name` (composite)
    - `team_season_coaches`: `season_id_fk`, `team_id_fk`, `coach_id_fk` (role within team)
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** Many coach-related samples provided.
- **Implementation Notes:**
    - Coach data is spread across several endpoints.
    - A `coaches_master` table for relatively static coach info.
    - A `coach_records` table for W-L records, potentially by season and record type.
    - A `team_season_coaches` table to link coaches to teams for specific seasons and roles.

---

## Seasonal Aggregates & Standings-like Data

These endpoints typically provide aggregated statistics or leaderboards at a seasonal or season-type level, rather than game-specific.

### 29. Season Team Statistics

- **`dlt` Resource Name:** `season_team_statistics_transformer`
- **Parent Resource:** `teams_transformer` (if iterating through teams of a season) or `season_types_transformer` (if data is by season type for teams).
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/teams/{team_id}/statistics`
- **Key Fields:** Similar to `event_team_statistics`: `splits.categories` with `name` and `stats` array.
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`, `stat_name` (assuming tidy format).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_teams_-team-id-_statistics_example.json` (also `seasons_-season-id-_types_-type-id-_teams_-team-id-_statistics_-statistic-id_example.json` which might be a detail for a specific stat category from the main list response).
- **Implementation Notes:** Aggregated team statistics over a season or season type. Structure like game stats (tidy format recommended).

---

### 30. Season Athlete Statistics

- **`dlt` Resource Name:** `season_athlete_statistics_transformer`
- **Parent Resource:** `athletes_resource` (if iterating athletes in a season) or `season_team_statistics_transformer` (if links to player season stats are found there).
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/teams/{team_id}/athletes/{athlete_id}/statistics` (or potentially `/seasons/{season_id}/types/{type_id}/athletes/{athlete_id}/statistics` if not team-specific context in URL for this view)
    - Sample `seasons_-season-id-_types_-type-id-_teams_-team-id-_athletes_-athlete-id-_statistics_-statistic-id_example.json` implies path like first option.
    - Sample `seasons_-season-id-_types_-type-id-_athletes_-athlete-id-_statistics_example.json` implies path like second option (more general for athlete in season type).
    - Sample `seasons_-season-id-_types_-type-id-_athletes_-athlete-id-_statistics_-statistic-id_example.json` if `statistic-id` is a specific category.
- **Key Fields:** Similar to `event_player_statistics`: `splits.categories` with `name` and `stats` array.
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `athlete_id_fk`, `stat_name` (assuming tidy format, and team context is implicit or handled via FK).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `athlete_id_fk`, (potentially `team_id_fk`).
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** Multiple provided, indicating different ways to access season-level player stats.
- **Implementation Notes:** Aggregated player statistics over a season or season type.

---

### 31. Season Leaders (Statistical)

- **`dlt` Resource Name:** `season_leaders_transformer`
- **Parent Resource:** `season_types_transformer`.
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/leaders`
- **Key Fields:** Similar to `event_leaders`: `categories` array with `name` and `leaders` array (`value`, `athlete.$ref`).
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `category_name`, `athlete_id_fk` (composite).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `athlete_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_leaders_example.json`.
- **Implementation Notes:** League leaders for various stats in a given season type.

---

### 32. Team Season Records

- **`dlt` Resource Name:** `season_team_records_transformer`
- **Parent Resource:** `teams_transformer` (for a specific team in a season) or `season_types_transformer`.
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/teams/{team_id}/records`
- **Paginated:** True (for different record types).
- **Key Fields (Item):** Similar to `event_records`: `name`, `type`, `summary`, `stats` array.
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`, `record_type` (composite).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_teams_-team-id-_record_example.json` (seems like one type), `seasons_-season-id-_types_-type-id-_teams_-team-id-_records_-record-id_example.json` (detail for one record item). The list endpoint is more likely the target.
- **Implementation Notes:** Overall/conference etc. records for a team for a season type.

---

### 33. Team Season ATS (Against The Spread) Records

- **`dlt` Resource Name:** `season_team_ats_transformer`
- **Parent Resource:** `teams_transformer` or `season_types_transformer`.
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/teams/{team_id}/ats` (if this provides a list)
- **Key Fields:** (Infer from `seasons_-season-id-_types_-type-id-_teams_-team-id-_ats_example.json`) - seems like a list of ATS records.
    - `summary` (e.g., "10-5-1 ATS")
    - `stats` (array: "pushes", "wins", "losses", "covers", "coverPercent", etc.)
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`, `ats_record_type_identifier` (if multiple types exist).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_teams_-team-id-_ats_example.json`.
- **Implementation Notes:** Team's ATS performance for a season type.

---

### 34. Team Season Odds Records

- **`dlt` Resource Name:** `season_team_odds_records_transformer`
- **Parent Resource:** `teams_transformer` or `season_types_transformer`.
- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/teams/{team_id}/odds-records`
- **Key Fields:** (Infer from `seasons_-season-id-_types_-type-id-_teams_-team-id-_odds-records_example.json`) - looks like a list of records by odds type (favorite, underdog, over, under).
    - `type` (e.g., "favoriteRecord", "underdogRecord", "overRecord", "underRecord")
    - `summary` (W-L)
    - `stats` (array like other records: wins, losses, pushes, etc.)
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`, `odds_record_type` (composite).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `seasons_-season-id-_types_-type-id-_teams_-team-id-_odds-records_example.json`.
- **Implementation Notes:** How a team performed based on betting odds categories.

---

### 35. Rankings (Polls)

- **`dlt` Resource Name:** `rankings_transformer`
- **Parent Resource:** `weeks_transformer` (if weekly rankings) or `season_types_transformer` (if broader).
- **Endpoint Path (List of Ranking Types for a week):** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/rankings` (Sample: `seasons_-season-id-_types_-type-id-_weeks_-week-id-_rankings_example.json` which is a list of $ref to specific polls like AP Top 25)
- **Endpoint Path (Detail of a Specific Ranking/Poll for a week):** (from `$ref`, e.g., `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/rankings/{ranking_id}`) (Sample: `seasons_-season-id-_types_-type-id-_weeks_-week-id-_rankings_-ranking-id_example.json`)
- **Key Fields (Poll Detail):**
    - `name` (e.g., "AP Top 25")
    - `lastUpdated`
    - `ranks` (array of ranked teams)
        - `team.$ref` (parse `team_id`)
        - `current` (rank number)
        - `previous` (previous rank)
        - `points`, `firstPlaceVotes`
        - `trend`
- **Primary Key (`dlt`):** `season_id_fk`, `type_id_fk`, `week_id_fk`, `ranking_name`, `team_id_fk` (composite, for each team's rank in a poll).
- **Foreign Keys (`dlt`):** `season_id_fk`, `type_id_fk`, `week_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** `rankings_example.json` (top-level list of ranking types), `seasons_-season-id-_types_-type-id-_weeks_-week-id-_rankings_example.json`, `seasons_-season-id-_types_-type-id-_weeks_-week-id-_rankings_-ranking-id_example.json`.
- **Implementation Notes:** Handles weekly polls like AP Top 25. `ranking_id` in path is often the poll name like "APTOP25".

---

### 36. Groups / Conferences

- **`dlt` Resource Name:** `groups_transformer`, `group_teams_transformer`
- **Parent Resource:** `season_types_transformer`.
- **Endpoint Path (List Groups):** `/seasons/{season_id}/types/{type_id}/groups` (Sample: `seasons_-season-id-_types_-type-id-_groups_example.json`)
- **Endpoint Path (Group Detail):** `/seasons/{season_id}/types/{type_id}/groups/{group_id}` (from `$ref`) (Sample: `seasons_-season-id-_types_-type-id-_groups_-group-id_example.json`)
- **Endpoint Path (Teams in Group):** `/seasons/{season_id}/types/{type_id}/groups/{group_id}/teams` (from `group_detail.teams.$ref`) (Sample: `seasons_-season-id-_types_-type-id-_groups_-group-id-_teams_example.json`)
- **Key Fields (Group Detail):** `id`, `name`, `abbreviation`, `shortName`.
- **Key Fields (Teams in Group Item):** `team.$ref` (parse `team_id`).
- **Primary Keys (`dlt`):**
    - `groups`: `season_id_fk`, `type_id_fk`, `id` (group's API ID).
    - `group_teams`: `season_id_fk`, `type_id_fk`, `group_id_fk`, `team_id_fk`.
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** As listed above.
- **Implementation Notes:** Handles conference/group structures and team memberships.

---

## Miscellaneous Endpoints

### 37. Franchises (Historical Team Lineage)

- **`dlt` Resource Name:** `franchises_resource`
- **Parent Resource:** Could be linked from `teams_transformer` if `franchise.$ref` is consistently available.
- **Endpoint Path (Detail):** `/franchises/{franchise_id}` (from `$ref`)
- **Key Fields:** `id`, `uid`, `slug`, `location`, `name`, `nickname`, `displayName`, `shortDisplayName`, `abbreviation`, `color`, `alternateColor`, `logo`. Also `team.$ref` might point to the *current* team representation.
- **Primary Key (`dlt`):** `id`
- **Write Disposition (`dlt`):** `merge`
- **Sample File:** `franchises_-franchise-id_example.json`
- **Implementation Notes:** Provides information about a team's franchise identity, potentially spanning different names/locations over time.

---

### 38. Awards

- **`dlt` Resource Names:** `awards_master_resource`, `season_awards_resource`
- **Parent Resource:** Various.
- **Endpoint Paths:**
    - List of Award Types: `/awards` (Sample: `awards_example.json`)
    - Award Detail (generic): `/awards/{award_id}` (Sample: `awards_-award-id_example.json`)
    - List Awards for a Season: `/seasons/{season_id}/awards` (Sample: `seasons_-season-id-_awards_example.json`)
    - Detail of a Specific Award in a Season (e.g., Player of the Year for 2024): `/seasons/{season_id}/awards/{award_id_instance}` (Sample: `seasons_-season-id-_awards_-award-id_example.json` - note award_id here might be an instance or specific type for the season).
- **Key Fields (Award Detail - generic):** `id`, `name`, `type` (e.g., "Player", "Coach"), `conference`, `athleteType`.
- **Key Fields (Season Award Detail - `seasons_-season-id-_awards_-award-id_example.json`):**
    - `name` (e.g., "John R. Wooden Award")
    - `type` (e.g., "Player")
    - `recipientType`
    - `athlete.$ref` (recipient athlete, parse `athlete_id`)
    - `team.$ref` (recipient team, parse `team_id`)
    - `completed` (boolean)
- **Primary Keys (`dlt`):**
    - `awards_master`: `id` (from `/awards/{award_id}`)
    - `season_awards_recipients`: `season_id_fk`, `award_id_fk` (references `awards_master.id`), `athlete_id_fk` (composite, if one award can have multiple recipients or to make unique per recipient). Or use an instance ID from API if available.
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** As listed.
- **Implementation Notes:** Distinguish between award definitions and specific instances of awards given out.

---

### 39. Notes (Team/Athlete)

- **`dlt` Resource Name:** `notes_transformer`
- **Parent Resource:** `teams_transformer` or `athletes_resource` (if notes are linked from them).
- **Endpoint Paths:**
    - Generic Notes list: `/notes` (Sample: `notes_example.json` - but seems empty/placeholder)
    - Team Notes: `/teams/{team_id}/notes` (Sample: `teams_-team-id-_notes_example.json` - also placeholder)
    - Athlete Notes for Season: `/seasons/{season_id}/athletes/{athlete_id}/notes` (Sample: `seasons_-season-id-_athletes_-athlete-id-_notes_example.json` - placeholder)
- **Key Fields:** (If data were present) `id`, `type`, `headline`, `text`, `dataSourceIdentifier`, `athlete.$ref`, `team.$ref`.
- **Implementation Notes:** Sample files are placeholders. If populated, would store textual notes/news about entities.

---

### 40. Injuries

- **`dlt` Resource Name:** `injuries_transformer`
- **Parent Resource:** `teams_transformer` or `athletes_resource`.
- **Endpoint Path:** `/teams/{team_id}/injuries` (Sample: `teams_-team-id-_injuries_example.json` - placeholder)
- **Key Fields:** (If data were present) `id`, `athlete.$ref`, `team.$ref`, `date`, `description`, `status`, `detail.type`, `detail.location`, `detail.side`.
- **Implementation Notes:** Sample is a placeholder. Would store injury information.

---

### 41. Calendar Endpoints (If Used)

- **Endpoints:** `/calendar`, `/calendar/blacklist`, `/calendar/whitelist`, `/calendar/offdays`, `/calendar/ondays`
- **Sample Files:** `calendar_example.json`, `calendar_blacklist_example.json`, etc.
- **Implementation Notes:** These seem to define valid/invalid dates for events or data. Usefulness depends on pipeline requirements. Might be used to guide event discovery.

---

### 42. Futures (Betting)

- **`dlt` Resource Name:** `futures_transformer`
- **Parent Resource:** `seasons_resource`.
- **Endpoint Path (List):** `/seasons/{season_id}/futures` (Sample: `seasons_-season-id-_futures_example.json`)
- **Endpoint Path (Detail):** `/seasons/{season_id}/futures/{future_id}` (from `$ref`) (Sample: `seasons_-season-id-_futures_-future-id_example.json`)
- **Key Fields (Detail):**
    - `id`, `name`, `shortName`, `type`
    - `marketSuspended` (boolean)
    - `outcomes` (array of betting outcomes)
        - `id` (outcome ID)
        - `description`
        - `odds`, `payout`
        - `team.$ref` (parse `team_id`)
        - `athlete.$ref` (parse `athlete_id`)
- **Primary Keys (`dlt`):**
    - `futures_markets`: `season_id_fk`, `id` (future market ID).
    - `futures_outcomes`: `season_id_fk`, `future_market_id_fk`, `id` (outcome ID).
- **Write Disposition (`dlt`):** `merge`
- **Sample Files:** As listed.
- **Implementation Notes:** For futures betting markets. Each future market has multiple outcomes.

---

### 43. Season Power Index & Leaders

- **Endpoint Path (Season Power Index List):** `/seasons/{season_id}/powerindex` (Sample: `seasons_-season-id-_powerindex_example.json`) - This is a list of team power indexes for the season.
- **Endpoint Path (Season Power Index Leaders):** `/seasons/{season_id}/powerindex/leaders` (Sample: `seasons_-season-id-_powerindex_leaders_example.json`) - This seems to be leaders based on power index changes or values.
- **Key Fields (Season Power Index Item):** `team.$ref`, `powerIndex`, `rank`, `trend`, etc. (similar to game power index but for season).
- **Key Fields (Power Index Leaders):** `description`, `athlete.$ref`, `value`.
- **Implementation Notes:** Provides season-long power index ratings for teams and potentially leaderboards derived from these.

---
This inventory aims to be comprehensive. Endpoints with placeholder sample data (like Notes, Injuries) are included for completeness but may yield no data.
The structure of `$ref` and the actual data within each endpoint will guide the final implementation of `dlt` resources and transformers.
Always parse IDs from relevant `$ref` URLs or from the object's own ID fields to establish foreign key relationships and primary keys.

</rewritten_file> 