# ESPN API Endpoint Inventory

This document catalogs the ESPN API endpoints used in this project, their request parameters, response structure, key fields, and other relevant details.

**Base URL:** `http://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball`

---

## 1. List Seasons

- **Endpoint Path:** `/seasons`
- **Request Parameters:** `limit` (query, optional), `page` (query, optional)
- **Paginated:** True
- **Response Structure:**
  - Top-level object with pagination fields (`count`, `pageIndex`, `pageSize`, `pageCount`).
  - `items`: An array of objects, each containing a single key `$ref`.
- **Notes:**

  - Provides a list of references (`$ref`) to season detail endpoints.

---

## 2. Season Details

- **Endpoint Path:** `/seasons/{season_id}`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
- **Paginated:** False
- **Response Structure:**
  - Top-level object with season details (`season_id`, `startDate`, `endDate`, `displayName`).
- **Notes:**

  - Provides basic season details.

---

## 3. List Season Types

- **Endpoint Path:** `/seasons/{season_id}/types`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
  - `limit` (query, optional), `page` (query, optional)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageIndex`, etc.)
  - `items`: Array of objects, each with `$ref` link to a specific season type detail endpoint.
- **Notes:**

  - Provides list of references (`$ref`) to season type detail endpoints.

---

## 4. Season Type Details

- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
  - `type_id` (path parameter, e.g., 2 for Regular Season)
- **Paginated:** False
- **Response Structure:**
  - Top-level object with details for the specific season type.
  - Includes fields like `id` (renamed `type_id`), `name`, `abbreviation`, `year`, `startDate`, `endDate`.
  - Boolean flags: `hasGroups`, `hasStandings`, `hasLegs`.
- **Notes:**

  - Provides details for a specific season type.

---

## 5. List Weeks

- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/weeks`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
  - `type_id` (path parameter, e.g., 2)
  - `limit` (query, optional), `page` (query, optional)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageIndex`, etc.)
  - `items`: Array of objects, each with `$ref` link to a specific week detail endpoint.
- **Notes:**

  - Provides list of references (`$ref`) to week detail endpoints.

---

## 6. Week Details

- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
  - `type_id` (path parameter, e.g., 2)
  - `week_id` (path parameter, e.g., 1)
- **Paginated:** False
- **Response Structure:**
  - Top-level object with details for the specific week.
  - Includes `week_id` (renamed `number`), `startDate`, `endDate`, `text`.
- **Notes:**

  - Provides details for a specific week.

---

## 7. List Week Events

- **Endpoint Path:** `/seasons/{season_id}/types/{type_id}/weeks/{week_id}/events`
- **Request Parameters:**
  - `season_id` (path parameter, e.g., 2024)
  - `type_id` (path parameter, e.g., 2)
  - `week_id` (path parameter, e.g., 1)
  - `limit` (query, optional), `page` (query, optional)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageSize`, `pageCount`).
  - `items`: Array of objects, each with a `$ref` link to an event detail endpoint.
- **Notes:**

  - Provides list of references (`$ref`) to event detail endpoints for a specific week.

---

## 8. Event Details

- **Endpoint Path:** `/events/{event_id}`
- **Request Parameters:**
  - `event_id` (path parameter)
- **Paginated:** False
- **Response Structure:**
  - Top-level object containing core event metadata and competitor details.
  - Provides context IDs, names, date, attendance, venue ID, and game flags.
  - Competitor information is flattened into home/away specific fields.
- **Notes:**

  - This is the primary hub for game-level information.

---

## 9. Season Specific Team Details

- **Endpoint Path:** `/seasons/{season_id}/teams/{team_id}` (Implicitly linked from `/events/{event_id}`)
- **Request Parameters:** `season_id`, `team_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Top-level object with detailed team information for a specific season.
  - Includes identifiers (`id`, `uid`), names (`location`, `name`, `displayName`), `abbreviation`, `color`.

---

## 10. Score Details

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/score` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Simple top-level object with score details for one competitor.
- **Notes:**

  - Provides the final score for one team in the game.

---

## 11. Linescores

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/linescores` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** True
- **Response Structure:**
  - Paginated list structure (`count`, `pageIndex`, etc.).
  - `items`: Array of objects, each representing score for a period (half).
- **Notes:**

  - Provides score breakdown by period (half) for one team.

---

## 12. Statistics

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/statistics` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Contains statistics split by categories (defensive, general, offensive) within a `splits` object.
  - Each category has a `stats` array containing **team-level aggregate statistics** for the game (e.g., total blocks, total rebounds).
  - Each category also has an `athletes` array containing `$ref` links to athlete details and **player-specific statistics endpoints** (`.../roster/{athlete_id}/statistics/0`).
- **Notes:**

  - Provides team aggregate stats for the game.
  - Links out via `$ref` to fetch individual player stats (identifies new detail endpoints).

---

## 12.1 Player Statistics (per Game)

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster/{athlete_id}/statistics/{split_id}` (Implicitly linked from competitor statistics)
- **Request Parameters:** `event_id`, `event_id`, `team_id`, `athlete_id`, `split_id` (path, split_id likely '0')
- **Paginated:** False
- **Response Structure:**
  - Similar structure to team statistics (`/competitors/{id}/statistics`).
  - Contains `splits` object (usually "Season" split for the game).
  - `splits.categories`: Array of categories (defensive, general, offensive).
  - Each category contains a `stats` array with **individual player statistics** for the game.
- **Notes:**

  - Provides detailed individual player stats for a specific game.

---

## 13. Leaders

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/leaders` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Contains `categories` array (points, assists, rebounds, steals, blocks).
  - Each category has a `leaders` array listing players ranked in that stat for the game.
  - Each leader entry includes the stat `value` and `$ref` links to `athlete` and full `statistics`.
- **Notes:**

  - Provides ranked player lists for key stats for one team in the game.
  - Relies on `$ref` links for player/stat details.

---

## 14. Roster

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/roster` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Contains an `entries` array listing players on the roster for the game.
  - Each entry includes `playerId`, `jersey`, `starter` flag, `didNotPlay` flag, `displayName`.
  - Includes `$ref` links to `athlete` details, `position` details, and player game `statistics`.
- **Notes:**

  - Provides the game-specific roster for one team.
  - Uses `$ref` for athlete/position details and game stats.

---

## 15. Record

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/competitors/{team_id}/records` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id`, `team_id` (path)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageIndex`, etc.).
  - `items`: Array of record objects (e.g., "overall", "Home", "Road", "vs. AP Top 25").
  - Each record item has a `summary` (W-L string) and a `stats` array.
  - `stats` array contains details like wins, losses, ties, winPercent, pointsFor/Against, streak.
- **Notes:**

  - Provides team W-L records (overall, home, away, etc.) up to this specific game.
  - Includes related stats like win %, PPG, streak for each record type.

---

## 16. Venue Details

- **Endpoint Path:** `/venues/{venue_id}` (Implicitly linked)
- **Request Parameters:** `venue_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Top-level object with venue details.
- **Notes:**

  - Provides static venue details.

---

## 17. Situation

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/situation` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Top-level object describing game state.
  - `lastPlay.$ref`: Link to the last play.
  - `homeTimeouts`, `awayTimeouts`: Timeout details.
  - `homeFouls`, `awayFouls`: Team foul details and bonus state.
- **Notes:**

  - Provides dynamic game state (timeouts, fouls). Likely most useful for live games.

---

## 18. Status

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/status` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id` (path)
- **Paginated:** False
- **Response Structure:**
  - Top-level object describing game status.
- **Notes:**

  - Provides detailed game status (scheduled, final, etc.), clock, and period.

---

## 19. Odds

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/odds` (Implicitly linked)
- **Request Parameters:** `event_id`, `event_id` (path)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageIndex`, etc.).
  - `items`: Array of odds objects, one per betting `provider`.
  - Each item contains `details` (text summary), `overUnder`, `spread`.
  - Nested `awayTeamOdds` and `homeTeamOdds` objects contain `moneyLine`, `spreadOdds`, and open/close/current odds breakdown.
- **Notes:**

  - Provides betting odds from multiple sources. Likely low priority.

---

## 20. Play-by-Play

- **Endpoint Path:** `/events/{event_id}/competitions/{event_id}/plays` (Implicitly linked via `details.$ref`)
- **Request Parameters:** `event_id`, `event_id` (path)
- **Paginated:** True
- **Response Structure:**
  - Paginated list (`count`, `pageIndex`, etc.) of play events.
  - `items`: Array of play objects.
  - Each play has `id`, `sequenceNumber`, `type` (e.g., "JumpShot"), `text` description, `awayScore`, `homeScore`, `period`, `clock`.
  - Includes `scoringPlay` flag, `team.$ref` link, and `participants` array.
  - `participants` array lists players involved with `$ref` links to `athlete` and `statistics`, and their role (`type`).
- **Notes:**

  - Provides detailed sequential log of game events.
  - Includes score/clock state at the time of each play.
  - Links players involved via `$ref`.

---

## Common Patterns & Notes

- Extensive use of `$ref` for linking between resources (list endpoints link to detail endpoints).
- Pagination seems standard (`count`, `pageIndex`, `pageSize`, `pageCount` on list endpoints).
- Paginated list endpoints support a maximum of 1000 records per request via the `limit` parameter.
- IDs (`season`, `type`, `week`, `event`, `team`, `venue`, etc.) are used in path parameters.
- Datetime format used throughout this api conforms to ISO 8601 formatted date and time, e.g. `2024-03-19T07:00Z`

---

## Pipeline Implementation Notes (`dlt`)

This section outlines key considerations and strategies for implementing a data pipeline using `dlt` to ingest data from this API.

### 1. Authentication
- **No authentication is required** to access these API endpoints. This simplifies request configurations in `dlt`.

### 2. Rate Limiting
- **Rate limits are currently unknown.**
- **Strategy:** `dlt` includes default retry mechanisms with exponential backoff for common HTTP errors (e.g., 429 Too Many Requests, 5xx server errors). This should provide initial resilience. If specific rate limiting behaviors are observed (e.g., specific error codes, `Retry-After` headers), `dlt` source configurations can be further customized.

### 3. Error Handling
- **Specific API error response structures are currently unknown.**
- **Strategy:** `dlt` will handle generic network or HTTP errors. If the API returns structured JSON error payloads for different error types, understanding these would allow for more granular error logging or conditional logic within the `dlt` Python source code. For now, relying on `dlt`'s default error handling is the starting point.

### 4. `$ref` Link Handling
- The API extensively uses `$ref` fields, where the value is a **full and complete URL** to the linked resource.
- **Strategy:**
    - `dlt` can handle these full URLs. A parent `dlt` resource will fetch an initial object or list and then `yield` these full URLs.
    - Child `dlt` resources will be defined to accept these URLs as parameters and make subsequent HTTP requests to fetch the linked data, creating a dependency chain.
    - **Optimization:** In some cases (e.g., `seasons_2025.json` example for season details), linked resources might have some data embedded alongside the `$ref` link (e.g., `types.items` array). The `dlt` source can first check if this embedded information is sufficient before deciding to follow the `$ref` URL, potentially saving API calls.
    - Most `$ref` target structures are predefined in this inventory, allowing for specific `dlt` resources for each.

### 5. Primary Key Strategy
- **Primary keys for many entities will be composite keys derived from identifiers in the API request URL path.**
- **Example:** For a week record from an endpoint like `/seasons/{season_id}/types/{type_id}/weeks/{week_id}`, the composite primary key in the destination table would be (`season_id`, `type_id`, `week_id`).
- **Strategy:**
    - When defining `dlt` resources, specify the `primary_key` argument (e.g., `primary_key=["season_id", "type_id", "week_id"]`) and `write_disposition="merge"` (or "replace").
    - The Python code within the `dlt` resource function must ensure that the data items being `yield`ed contain these primary key fields. If the API response body doesn't include these IDs directly, they must be parsed from the input URL (used to fetch the data) and added as new fields to the JSON data before `yield`ing.

### 6. Incremental Loading Strategy
- **No explicit `lastModified` timestamps are consistently available across all relevant resources for simple incremental loading.**
- **Strategy:**
    - The "Status" endpoint (Inventory #18), which provides game status (e.g., scheduled, final), can be used to infer if an event's data is complete.
    - `dlt`'s state-keeping capabilities can be used to track `event_id`s that have been processed and are in a "final" state.
    - In subsequent pipeline runs, the source can query the Status endpoint for new `event_id`s or for previously seen `event_id`s that have transitioned to a "final" state, and only process (or re-process) those events and their related sub-resources (scores, stats, etc.).

### 7. Estimated Data Volume
- **Seasons:** Approximately 22
- **Season Types per Season:** Approximately 4 (Preseason, Regular Season, Postseason, Off Season)
- **Weeks (Regular Season):** Approximately 20
- **Weeks (Postseason):** Approximately 4
- **Games per Week (Regular Season):** Approximately 300-350
- **Total Games per Season:** Approximately 6,500 - 7,000
- These volumes are manageable for `dlt` but reinforce the need for efficient pagination and potentially incremental loading strategies.

### 8. API Path Structure for Event Sub-Resources
- For endpoints related to specific competitions within an event (typically endpoints 10 through 20 in this inventory), the API uses the `event_id` in the path segment where a unique `competition_id` might conventionally appear.
- **Example Path Structure:** `/events/{event_id}/competitions/{event_id}/...`
- This means that the `event_id` is used to identify both the overall event and the specific competition context within that event for these sub-resources. This is reflected in the documented paths and request parameters. 