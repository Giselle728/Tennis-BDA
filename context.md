# Tennis-BDA — Internal Technical Reference

**Audience:** project teammates. **Purpose:** explain what the code actually does today, where each
feature lives, and where to edit when something changes.

**Ground rules for this document**

* Everything here was read from the code in this repository (and from the running system). If a file
  is described here, it exists; if a feature is described as visible, it is rendered by
  `frontend/index.html`.
* Code that exists but is **not** wired to the website is labelled explicitly as *implemented but
  not connected to the frontend* (see [Implementation Status](#implementation-status)).
* Empty placeholder files are listed as empty. They are not described as features.
* This is a working reference, not marketing copy and not a theory document.

---

# Project

## What it is

A tennis analytics web application over six seasons of ATP men's match results, sitting on top of a
Hadoop / MongoDB / stream-processing analytics pipeline.

## What it analyses

* **Matches** – 15,980 ATP matches (cleaned dataset), with tournament, surface, round, score,
  duration and serve statistics.
* **Players** – 842 distinct players, their wins/losses, win rate, serve and break-point averages,
  season-by-season trajectory and surface-by-surface record.
* **Head-to-head** – the real recorded meetings between any two players, with a per-surface
  breakdown.
* **Surfaces** – hard / clay / grass comparison (match volume, aces, double faults, first serve,
  break points, duration).
* **Tournaments** – tournament-level match counts and average match duration.
* **Live matches** – whether ATP matches are currently in progress (external live feed).

## Dataset

| File | Contents | Size |
|------|----------|------|
| `data/raw/atp_matches_2019.csv` … `atp_matches_2024.csv` | Raw per-season ATP results | 300 KB – 635 KB each |
| `data/raw/atp_players.csv` | Player master list | 2.4 MB |
| `data/processed/tennis_matches_clean.csv` | Cleaned merged dataset, 15,980 rows × 57 columns | 5.6 MB |
| `data/processed/real_atp_stream_results.json` | Generated stream output (31,960 events), git-ignored | 17 MB |

Seasons are **2019–2024**. The dataset's own `year` column is derived from the tournament start
date, so the 85 matches played on `2018-12-31` (Brisbane, Doha, Pune) are labelled `2018`. Those
rows belong to the 2019 season; `season_years()` in `src/analytics/player_analysis.py` re-derives
the season from `tourney_id` / `match_id` so nothing is counted as a 2018 season.

## Main user-facing functionality (what is actually visible)

Six navigation entries in `frontend/index.html`:

| Nav item | Tab | Data source |
|---|---|---|
| Home | `tab-landing` | `/api/dashboard/overview` |
| Dashboard | `tab-dashboard` | `/api/dashboard/overview`, `/surfaces`, `/top-players`, `/recent-events`, `/live` |
| Players | `tab-player` | `/api/players`, `/api/players/{name}` |
| Matches | `tab-match` | `/api/matches`, `/api/matches/{match_id}` |
| Head-to-Head | `tab-h2h` | `/api/head-to-head` |
| Live Matches | *(no tab)* | `openLiveMatches()` scrolls to the Dashboard live card (`/api/dashboard/live`) |

## Big Data / analytics concepts demonstrated in the implementation

* Batch ETL and cleaning with Pandas (`src/preprocessing/`).
* Distributed storage on HDFS and distributed aggregation with Hadoop MapReduce
  (`src/mapreduce/`, `src/load_*.py`).
* NoSQL storage and querying in MongoDB (`src/mongodb/tennis_queries.py`, `src/stream_mining/stream_mongodb.py`).
* Stream processing with duplicate filtering (Bloom Filter), windowing (sliding window) and
  approximate distinct counting (Flajolet–Martin) plus exact counting for comparison
  (`src/stream_mining/`).
* A read-only analytics REST API and a single-page visualisation frontend (`src/api/`,
  `frontend/`).
---

# Overall Project Flow

## Diagram (matches the actual code)

```
                    ┌─────────────────────────────────────────────┐
                    │ data/raw/atp_matches_2019..2024.csv        │
                    │ data/raw/atp_players.csv                   │
                    └────────────────────┬────────────────────────┘
                                         │  glob + concat
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │ src/preprocessing/clean_data.py            │
                    │  de-duplicate · parse dates · type coercion │
                    │  match_id · derived percentages · validate  │
                    └────────────────────┬────────────────────────┘
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │ data/processed/tennis_matches_clean.csv    │
                    │ (15,980 rows × 57 cols — the analysis fact)│
                    └───────┬─────────────────────────┬───────────┘
                            │                         │
        BATCH PATH          │                         │  STREAM PATH
                            ▼                         ▼
   hdfs dfs -put       src/mapreduce/*_mapper.py   src/stream_mining/
   /user/giselled-     + *_reducer.py (3 jobs)     tennis_match_stream.py
   mello/tennis/data   (python, via hadoop jar     → 2 events per match
                            │  streaming)           (MATCH_WIN/MATCH_LOSS)
                            ▼                               │
   /user/giselled-    src/load_player_wins.py               │
   mello/tennis/      src/load_surface_analysis.py          │
   output/<job>/      src/load_tournament_analysis.py       │
   part-00000         (hdfs dfs -cat → bulk upsert)         │
                            │                               │
                            └───────────────┬───────────────┘
                                            ▼
                    ┌─────────────────────────────────────────────┐
                    │ MongoDB  tennis_analytics                   │
                    │  player_wins · surface_analysis ·           │
                    │  tournament_analysis · stream_events ·      │
                    │  stream_metrics                             │
                    └────────────────────┬────────────────────────┘
                                         │  read-only queries
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │ src/api/tennis_dashboard_api.py             │
                    │ (Starlette + Uvicorn; also serves           │
                    │  frontend/index.html on "/")                │
                    └────────────────────┬────────────────────────┘
                                         │  JSON over HTTP (same origin)
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │ frontend/index.html                         │
                    │  Home · Dashboard · Players · Matches ·     │
                    │  Head-to-Head (+ live card on Dashboard)    │
                    └─────────────────────────────────────────────┘

   Separate live stream (feeds the live card):
   https://api.livetennisapi.com → src/stream_mining/live_tennis_stream.py (15 s poll)
        → live_event_adapter.create_live_events() → same StreamProcessor
        → MongoDB (source="live", metrics upserted as live_stream_current)
        → read by /api/dashboard/live
```

## Stage-by-stage

**1. Raw data** – six per-season CSV files plus a player list, committed under `data/raw/`.

**2. Preprocessing** – `src/preprocessing/clean_data.py` merges them and produces the single cleaned
file. See [Data Preprocessing](#data-preprocessing).

**3. Cleaned dataset** – `data/processed/tennis_matches_clean.csv` is the *fact table*. The Players,
Matches and Head-to-Head pages read it directly with Pandas at request time.

**4a. Batch path (MapReduce)** – the cleaned CSV is uploaded to HDFS, three Hadoop-streaming jobs
read it from HDFS and write aggregated results back to HDFS, and three loader scripts copy those
results into MongoDB. See [MapReduce](#mapreduce).

**4b. Stream path** – `src/stream_mining/tennis_match_stream.py` reads the same cleaned CSV in date
order and turns every match into two events, pushing each event through `StreamProcessor`
(Bloom Filter → exact counters → Flajolet–Martin → sliding window) and writing events + a metrics
snapshot to MongoDB. See [Streaming / Real-Time Processing](#streaming--real-time-processing).

**5. MongoDB** – holds the pre-aggregated analytics (`player_wins`, `surface_analysis`,
`tournament_analysis`) and the stream output (`stream_events`, `stream_metrics`). Only loaders and
stream pipelines write; the API is read-only. See [MongoDB](#mongodb).

**6. API** – `src/api/tennis_dashboard_api.py` exposes the data as JSON. Players / Matches /
Head-to-Head responses are computed per request from the CSV; Dashboard responses come from
MongoDB.

**7. Frontend** – one self-contained page (`frontend/index.html`, HTML + CSS + vanilla JS +
Chart.js + Font Awesome). It fetches JSON and renders tables and charts; it performs no analytics
of its own.

**Which page reads which source**

| Page / tab | Source | Endpoint(s) |
|---|---|---|
| Home | MongoDB | `/api/dashboard/overview` |
| Dashboard KPIs | MongoDB | `/api/dashboard/overview` |
| Dashboard surface chart | MongoDB `surface_analysis` | `/api/dashboard/surfaces` |
| Dashboard top players | MongoDB `player_wins` | `/api/dashboard/top-players` |
| Dashboard recent matches | MongoDB `stream_events` | `/api/dashboard/recent-events` |
| Dashboard live card | MongoDB `stream_metrics` (live key) + live events | `/api/dashboard/live` |
| Players | Cleaned CSV | `/api/players`, `/api/players/{name}` |
| Matches + match modal | Cleaned CSV | `/api/matches`, `/api/matches/{id}` |
| Head-to-Head | Cleaned CSV | `/api/head-to-head` |
---

# Feature-wise Flow

How one user action travels through the system, with real filenames.

### Home — data coverage strip
```
User opens Home tab
  → frontend/index.html  switchTab('landing') → renderLandingCoverage()
  → GET /api/dashboard/overview
  → src/api/tennis_dashboard_api.py  build_overview() → TennisQueries
  → src/mongodb/tennis_queries.py  get_collection_counts(), count_distinct_matches(),
                                  count_distinct_players()
  → MongoDB tennis_analytics
  → JSON { kpis, collection_counts, dataset }
  → #landing-coverage is filled with matches / players / tournaments / surfaces / date range
```

### Dashboard — KPI cards
```
User opens Dashboard
  → switchTab('dashboard') → renderDashboard()
  → GET /api/dashboard/overview   (in Promise.all with 4 more calls)
  → build_overview() → TennisQueries.get_collection_counts(),
                       count_distinct_matches(), count_distinct_players(),
                       get_event_time_range(), get_latest_stream_metrics('historical')
  → MongoDB
  → renderDashKpis(overview) writes #kpi-total-matches, #kpi-unique-players,
                       #kpi-tournaments (from collection_counts.tournament_analysis),
                       #kpi-surfaces, #dash-dataset-meta, #dash-status-text
```

### Dashboard — Matches by Surface chart
```
renderDashboard() → GET /api/dashboard/surfaces
  → build_surfaces() → TennisQueries.get_matches_by_surface()
  → MongoDB surface_analysis (written by the MapReduce surface job)
  → renderDashSurfaces(): Chart.js doughnut (#chart-dashboard-surface)
    + legend rows with counts and percentages
```

### Dashboard — Top Players by Wins
```
renderDashboard() → GET /api/dashboard/top-players?limit=10
  → build_top_players() → TennisQueries.get_top_players_by_wins(limit)
  → MongoDB player_wins (written by the MapReduce player-wins job)
  → renderDashTopPlayers() → #top-players-list ranked rows with win counts
```

### Dashboard — Recent Matches
```
renderDashboard() → GET /api/dashboard/recent-events?limit=15
  → build_recent_events() → TennisQueries.get_recent_stream_events(limit)
  → MongoDB stream_events (historical MATCH_WIN/MATCH_LOSS pairs, sorted by timestamp desc)
  → renderDashRecentEvents() keeps only event_type == 'MATCH_WIN'
    (one row per real match) → #recent-events-body
```

### Dashboard — Live ATP Matches card
```
switchTab('dashboard') also renders / renderDashboard() → GET /api/dashboard/live
Sidebar "Live Matches" → openLiveMatches() → switchTab('dashboard') + scroll to
                                                     #dashboard-live-card
  → build_live() → TennisQueries.get_current_live_metrics() (metric_key='live_stream_current')
                    TennisQueries.get_current_live_events()
  → MongoDB stream_metrics (source='live') + stream_events
  → renderDashLiveStatus() → #live-message, #live-badge-text, #live-event-count
If no ATP match is live the card shows "No live ATP matches currently available".
```

### Players — list and selection
```
switchTab('player') → renderPlayerAnalysis() → loadPlayersList()
  → GET /api/players
  → players_page() → player_list_rows(): value_counts on winner_name and loser_name
  → Pandas over data/processed/tennis_matches_clean.csv
  → players[] { player_name, matches, wins, losses, win_rate }, sorted by wins desc
  → populates #player-selector (842 options); retryPlayersList() on failure
Selecting a player → onPlayerSelected(name) → GET /api/players/{name}
  → player_detail_page() → get_player_analysis(name)
  → renderPlayerProfile() / renderPlayerHistoryChart() / renderPlayerMetricRadar()
```

### Players — profile, trajectory, surfaces
```
GET /api/players/{name} → player_detail_page() → get_player_analysis(name)
  → src/analytics/player_analysis.py: filter rows where winner_name==name OR loser_name==name
  → career totals, averages, season_years() → yearly_performance[],
    groupby(surface) → surface_performance[]
  → renderPlayerProfile() (#player-kpi-grid)
    renderPlayerHistoryChart() (#chart-player-history)
    renderPlayerMetricRadar() (#chart-player-radar)
    surface rows (#player-surface-table-body)
```

### Matches — filters, search, pagination
```
switchTab('match') → renderMatchTable()
  → GET /api/matches?year&surface&tournament&player&search&limit&offset
  → matches_page() → build_match_list()
  → get_match_analysis(...) filters exactly (season via season_years())
  → tournament names normalised (tournament_names / normalise_name_of_series)
  → optional substring search on winner/loser/tournament
  → sort year+match_id desc → slice [offset : offset+limit]
  → populateMatchFilterOptions() (#match-filter-year/surface/tournament, #match-count-badge)
  → renderMatchTable() (#match-table-body), renderMatchPager() (#match-page-info, prev/next)
```

### Matches — match details modal
```
User clicks a row → openMatchModal(matchId)
  → GET /api/matches/{match_id}
  → match_detail_page() → get_match_details(match_id)  (season via season_years())
  → #modal-tournament-badge, #modal-match-title, #modal-match-score, #modal-stats-container
  closeMatchModal() closes #match-modal
```

### Head-to-Head
```
switchTab('h2h') or a dropdown change → renderH2H()
  → needs the player list first: loadPlayersList() → populateH2HPlayerOptions()
  → guards: empty selection / identical players → message, no request
  → GET /api/head-to-head?player1=…&player2=…
  → head_to_head_page() → build_head_to_head() → get_head_to_head(a, b)
  → src/analytics/head_to_head.py: rows where both players appear (either orientation),
    wins per player, newest-first matches, groupby('surface')
  → renderH2HResult(): #h2h-hero-card, #h2h-matches-body, #h2h-surface-table-body
No meetings → 200 with total_matches 0 → empty-state message, no invented numbers
```
---

# Visible Features

Only features a user can actually reach in `frontend/index.html` are documented here. Each entry
follows the same shape: what is seen → input → processing → algorithm → output → BDA concept → code.

## Feature: Home / data coverage

**What the user sees.** Product hero ("Tennis Match Analytics & Performance Analysis System"), six
feature cards, and a "Data Coverage" strip with the real dataset numbers.

**Input.** Opening the Home tab (no user selection).

**Processing.** `switchTab('landing')` calls `renderLandingCoverage()`, which fetches
`/api/dashboard/overview` and writes matches, players, tournaments, surfaces and the date range into
`#landing-coverage`. The same fetch fills the header match count (`#header-match-count`).

**Algorithm.** None — aggregation counts already produced by MongoDB queries
(`collection_counts`, `count_distinct_matches`, `count_distinct_players`, `get_event_time_range`).

**Output.** `15,980 matches · 842 players · 477 tournaments · 3 surfaces · 2018-12-31 → 2024-12-18`.

**BDA concept.** Serving pre-computed aggregates (no recomputation in the browser).

**Code.** `frontend/index.html` → `renderLandingCoverage()`, `renderDashKpis()`;
`src/api/tennis_dashboard_api.py` → `build_overview()`; `src/mongodb/tennis_queries.py`.

---

## Feature: Dashboard summary cards

**What the user sees.** Four cards: Total Matches, Unique Players, Tournaments, Surfaces, each with
a one-line explanation, plus a status chip ("Dataset ready · N matches"), a "last refreshed"
timestamp and a Refresh button (`renderDashboard()` is re-callable from the button).

**Input.** Opening the Dashboard tab or clicking Refresh.

**Processing.** One `GET /api/dashboard/overview` request.

**Algorithm.** `count_distinct_matches()` and `count_distinct_players()` aggregate over the
`stream_events` collection (`$group` + `$addToSet`); `surfaces` is the length of
`get_matches_by_surface()`; `collection_counts` is a `count_documents` per collection, which is
where the Tournaments number comes from.

**Output.** 15,980 / 842 / 477 / 3 and a dataset line "Matches · Players · Date range · 2019-2024
seasons".

**BDA concept.** Aggregation and pre-computed summary tables served over an API.

**Code.** `frontend/index.html` (`#kpi-total-matches`, `#kpi-unique-players`, `#kpi-tournaments`,
`#kpi-surfaces`, `renderDashKpis`, `setDashStatus`); `build_overview()`; `tennis_queries.py`.

---

## Feature: Matches by Surface

**What the user sees.** A Chart.js doughnut chart of Hard / Clay / Grass with a legend showing each
surface's match count and percentage share.

**Input.** Opening the Dashboard tab.

**Processing.** `GET /api/dashboard/surfaces` → `build_surfaces()` →
`TennisQueries.get_matches_by_surface()` reads the `surface_analysis` collection that the
MapReduce surface job produced (plus, for other consumers, `get_surface_performance()` and
`get_stream_events_by_surface()`).

**Algorithm.** The numbers are *not* computed in the frontend. The chart only sums the returned
`match_count` values to build percentages.

**Output.** Hard 9,537 / Clay 4,772 / Grass 1,618 (matches with a missing surface are excluded
upstream).

**BDA concept.** MapReduce aggregation rendered as a visualisation.

**Code.** `frontend/index.html` → `renderDashSurfaces()`, `#chart-dashboard-surface`, `#surface-legend`;
`build_surfaces()`; `get_matches_by_surface()`; produced by
`src/mapreduce/surface_analysis_mapper.py` + `surface_analysis_reducer.py`.
---

## Feature: Top Players by Wins

**What the user sees.** A ranked list (top 10) of players with their number of wins and a proportional
bar, with a note that this is participation volume, not a quality ranking.

**Input.** Opening the Dashboard tab.

**Processing.** `GET /api/dashboard/top-players?limit=10` → `build_top_players()` →
`TennisQueries.get_top_players_by_wins(limit)`, which sorts the `player_wins` collection by
`win_count` descending (indexed).

**Algorithm.** The count itself is the MapReduce player-wins job (see
[MapReduce](#mapreduce)); the frontend only formats numbers and bar widths.

**Output.** e.g. `1 Daniil Medvedev 166 wins`, `2 Stefanos Tsitsipas 158`, `3 Novak Djokovic 157`.

**BDA concept.** MapReduce word-count style aggregation ("emit key 1, sum by key") exposed to the UI.

**Code.** `frontend/index.html` → `renderDashTopPlayers()`, `#top-players-list`;
`build_top_players()`; `get_top_players_by_wins()`; produced by
`src/mapreduce/player_wins_mapper.py` + `player_wins_reducer.py`.

---

## Feature: Recent Matches

**What the user sees.** A table of the 15 newest matches: winner, opponent, tournament, surface,
score, date.

**Input.** Opening the Dashboard tab.

**Processing.** `GET /api/dashboard/recent-events?limit=15` → `build_recent_events()` →
`TennisQueries.get_recent_stream_events(limit)`, a `find()` on `stream_events` sorted by
`timestamp` descending with a projection of player / opponent / tournament / surface / score /
timestamp / event_type.

**Algorithm.** Backend returns **two events per match** (`MATCH_WIN` and `MATCH_LOSS`). The renderer
filters `event_type === 'MATCH_WIN'` so the table shows one row per real match instead of two.

**Output.** e.g. `Joao Fonseca / Learner Tien / Next Gen Finals / Hard / 2-4 4-3(8) 4-0 4-2 /
2024-12-18`.

**BDA concept.** Querying an event store (stream output) as a user-facing list.

**Code.** `frontend/index.html` → `renderDashRecentEvents()`, `#recent-events-body`;
`build_recent_events()`; `get_recent_stream_events()`; events written by
`src/stream_mining/tennis_match_stream.py` + `stream_mongodb.py`.

---

## Feature: Live ATP Matches

**What the user sees.** A card titled "Live ATP Matches" with a status chip, a message, the number of
live matches and the last check time. The sidebar "Live Matches" entry scrolls straight to this card.

**Input.** Opening the Dashboard, or clicking "Live Matches" in the sidebar
(`openLiveMatches()` → `switchTab('dashboard')` + `scrollIntoView`).

**Processing.** `GET /api/dashboard/live` → `build_live()` →
`TennisQueries.get_current_live_metrics()` (the `stream_metrics` document with
`metric_key='live_stream_current'`, i.e. `source='live'`) and `get_current_live_events()`.

**Algorithm.** No client-side logic beyond formatting: `has_live_matches` and `message` come from
the backend.

**Output.** When nothing is live (the usual case): "No live ATP matches currently available" with
`0` live matches. When matches are live: their count plus `event_count` and the last-check time.

**BDA concept.** Consuming a live event pipeline's current state (near-real-time view), without
exposing the implementation to the user.

**Code.** `frontend/index.html` → `renderDashLiveStatus()`, `#dashboard-live-card`, `#live-message`,
`#live-badge-text`, `#live-event-count`, `openLiveMatches()`; `build_live()`;
`get_current_live_metrics()`, `get_current_live_events()`; produced by
`src/stream_mining/live_tennis_stream.py`.

---

## Feature: Players — list and selection

**What the user sees.** A searchable player selector pre-filled with every player in the dataset
(842), a meta line about the loaded list, and a retry button if the list cannot be loaded.

**Input.** Opening the Players tab; choosing a player.

**Processing.** `GET /api/players` → `players_page()` → `player_list_rows()`, which value-counts
`winner_name` and `loser_name` over the cleaned CSV.

**Algorithm.** `matches = wins + losses`, `win_rate = wins / matches * 100`, computed per player in
Pandas; rows sorted by wins desc, matches desc, name asc.

**Output.** `{ players: [{player_name, matches, wins, losses, win_rate}], count }`.

**BDA concept.** Filtering + aggregation over the cleaned dataset (no database round-trip needed for
this list).

**Code.** `frontend/index.html` → `loadPlayersList()`, `retryPlayersList()`, `renderPlayerAnalysis()`,
`onPlayerSelected()`, `#player-selector`; `players_page()`, `player_list_rows()`;
`src/analytics/player_analysis.py` for the shared definitions.
---

## Feature: Player Profile and Performance Trajectory

**What the user sees.** A header line for the player, a KPI grid (matches, wins, losses, win rate,
aces, double faults, first serve %, first-serve points won %, break-point conversion, average
match duration, latest rank), a "Performance Trajectory" chart and an "Attribute Vector" radar
chart.

**Input.** The player chosen in `#player-selector`.

**Processing.** `GET /api/players/{player_name}` → `player_detail_page()` →
`get_player_analysis(player_name)`.

**Algorithm** (all inside `get_player_analysis`, Pandas over the cleaned CSV):

1. `player_matches` = rows where `winner_name == player` OR `loser_name == player`.
2. `wins` = rows where the player is the winner; `losses` = rows where the player is the loser;
   `win_rate = wins / total * 100`.
3. Aces / double faults = `sum(w_ace)` over the player's wins + `sum(l_ace)` over the losses
   (same for `w_df` + `l_df`).
4. Serve and break-point averages = mean of the player's `w_*` values concatenated with their
   `l_*` values (so the player is compared as winner and as loser on the same scale).
5. `avg_match_duration` = mean of `minutes`.
6. `latest_rank` = the last non-null ranking value collected from `winner_rank` / `loser_rank`
   (i.e. the rank in the player's most recent recorded match — not a live ranking).
7. Trajectory: `groupby(season_years(...))` → `matches`, `wins`; then `losses = matches - wins` and
   `win_rate`. Seasons are sorted ascending and cast to int, so the chart is always
   2019 → 2024 for the current dataset.

**Output.** JSON consumed by `renderPlayerProfile()`, `renderPlayerHistoryChart()` and
`renderPlayerMetricRadar()`; surfaces are rendered by `renderPlayerProfile()` into
`#player-surface-table-body`.

**BDA concept.** Per-request aggregation and windowed (per-season) analysis over a cleaned dataset.

**Code.** `frontend/index.html` → `renderPlayerProfile()`, `renderPlayerHistoryChart()`,
`renderPlayerMetricRadar()`, `clearPlayerAnalysis()`, `#player-kpi-grid`, `#chart-player-history`,
`#chart-player-radar`, `#player-surface-table-body`;
`src/api/tennis_dashboard_api.py` → `player_detail_page()`;
`src/analytics/player_analysis.py` → `get_player_analysis()`, `season_years()`.

---

## Feature: Matches — filters, search and pagination

**What the user sees.** Four controls (season, surface, tournament dropdowns + a free-text search
box), a "Showing X of Y matches" badge, a match table and prev/next page buttons with a page label.

**Input.** Any filter change (`onMatchFilterChange()`), the search box, or the pagination buttons
(`gotoMatchPage(step)`).

**Processing.** `GET /api/matches?...` → `matches_page()` → `build_match_list()`.

**Algorithm.**

* Filtering delegates to `get_match_analysis(year, tournament, surface, player)`; the season column
  is replaced by `season_years()` *before* filtering, so the 2018-12-31 rows belong to 2019.
* Tournament names are normalised (`tournament_names()`, `normalise_name_of_series()`,
  `normalise_name()`) because the same event is spelled "US Open" in 2019 and "Us Open" from 2020;
  rows are relabelled with the filter spelling before being counted.
* `search` is a case-insensitive substring match on winner, loser or tournament (applied in the API
  because `get_match_analysis()` only supports exact player names).
* Results are sorted by `year`, `match_id` descending (newest first), then sliced
  `[offset : offset + limit]`. Defaults: `limit=100` (`MATCH_LIST_LIMIT`), max 500
  (`MATCH_LIST_MAX_LIMIT`); an out-of-range offset is clamped to the last page.
* `filter_options` (years, surfaces, tournaments, players) are built by
  `build_match_filter_options()` so the dropdowns can never drift from the data.

**Output.** `{ matches, count, total_count, limit, offset, page, total_pages,
has_previous_page, has_next_page, filters, filter_options }`.

**BDA concept.** Filtering, grouping and pagination over a large table (only one page is ever
returned to the browser).

**Code.** `frontend/index.html` → `renderMatchTable()`, `matchFilterParams()`,
`onMatchFilterChange()`, `populateMatchFilterOptions()`, `renderMatchPager()`, `gotoMatchPage()`,
`matchTableMessage()`, `surfaceBadgeClass()`, `#match-filter-*`, `#match-search-input`,
`#match-count-badge`, `#match-table-body`, `#match-page-info`;
`src/api/tennis_dashboard_api.py` → `matches_page()`, `build_match_list()`,
`build_match_filter_options()`; `src/analytics/match_analysis.py` → `get_match_analysis()`.
---

## Feature: Match Details

**What the user sees.** A modal with the tournament badge, title, score and a stats block for both
players (aces, double faults, first serve %, first-serve points won %, break-point conversion) plus
surface, round and duration.

**Input.** Clicking a row in the Matches table.

**Processing.** `GET /api/matches/{match_id}` → `match_detail_page()` →
`get_match_details(match_id)`.

**Algorithm.** Exact row lookup on `match_id`, then a single-row projection. The season shown is
`season_years(match).iloc[0]`, i.e. the same rule as the list, so the detail never disagrees with
the table. Unknown ids return 404; `/api/matches/` (no id) returns a helpful JSON error.

**Output.** `{ match_id, year, tournament, surface, round, winner, loser, score, duration,
winner_aces, loser_aces, winner_double_faults, loser_double_faults, winner/loser_first_serve_pct,
winner/loser_first_serve_won_pct, winner/loser_bp_conversion }`.

**BDA concept.** Record-level lookup in the cleaned dataset (analytics API detail view).

**Code.** `frontend/index.html` → `openMatchModal()`, `closeMatchModal()`, `#match-modal`,
`#modal-*`; `src/api/tennis_dashboard_api.py` → `match_detail_page()`, `matches_missing_id_page()`;
`src/analytics/match_analysis.py` → `get_match_details()`.

---

## Feature: Head-to-Head

**What the user sees.** Two player dropdowns (same list as the Players page), a VS header, a hero
card with each player's wins and win rate and the total meetings, a win-share bar, a "Recent
Meetings" table (tournament, year, surface, round, winner, score — newest first) and a per-surface
table.

**Input.** The two selections. If they are identical, the page shows a message and **does not call
the API**.

**Processing.** `GET /api/head-to-head?player1=…&player2=…` → `head_to_head_page()` →
`build_head_to_head()` → `get_head_to_head(player1, player2)`.

**Algorithm** (`src/analytics/head_to_head.py`):

1. Load the cleaned CSV; re-assign the season with the shared `season_years()` (2019–2024).
2. Select rows where **both** players appear, in either orientation:
   `(winner == p1 AND loser == p2) OR (winner == p2 AND loser == p1)`.
   Reversing the names therefore finds the same meetings and mirrors the win counts.
3. `player_a_won = winner_name == player1`; `total = len(h2h)`; `player1_wins = sum(player_a_won)`;
   `player2_wins = total - player1_wins`; win rates from those counts.
4. Sort matches newest first for the list.
5. `groupby('surface')` → matches and player-1 wins per surface (rows with a missing surface are
   dropped).
6. If nothing is found, return `None`; the API converts that into a 200 response with
   `EMPTY_HEAD_TO_HEAD` (`total_matches`, `player1_wins`, `player2_wins` all 0), empty lists and a
   note. Nothing is estimated.

**Output.** Totals, win record, newest-first `matches[]`, `surface_breakdown[]`, dataset metadata,
generation timestamp.

**BDA concept.** Backend-side filtering + aggregation over the cleaned dataset. The frontend renders
the response verbatim and calculates no wins itself.

**Code.** `frontend/index.html` → `renderH2H()`, `renderH2HResult()`, `populateH2HPlayerOptions()`,
`h2hMessageCard()`, `h2hClearTables()`, `HEAD_TO_HEAD_API`, `#h2h-player1`, `#h2h-player2`,
`#h2h-hero-card`, `#h2h-matches-body`, `#h2h-surface-table-body`;
`src/api/tennis_dashboard_api.py` → `head_to_head_page()`, `build_head_to_head()`,
`read_player_param()`, `normalise_name()`;
`src/analytics/head_to_head.py` → `get_head_to_head()`.

**Error contract** – missing/blank name → 400; identical players (case-insensitive) → 400; unknown
player → 404 with a hint to `/api/players`; dataset file missing → 503; any other backend failure →
500 with `error_type`.
---

# BDA Concepts Used

Only concepts that are actually implemented in this repository are listed.

| Concept | Where Implemented | What It Does | Why It Is Used |
| ------- | ----------------- | ------------ | -------------- |
| Data profiling / exploration | `src/preprocessing/data_profile.py` | Prints the cleaned dataset's size, columns, matches-by-year, surface distribution and tournament levels | Sanity check before the data is used downstream |
| Data cleaning / ETL | `src/preprocessing/clean_data.py` | Merges 6 raw CSVs, removes duplicates, parses dates, coerces types, standardises text and surfaces, creates `match_id` and derived percentages, validates ranges | One consistent fact table for every later stage |
| Derived feature engineering | `clean_data.py` (percentages), `match_id`, `year` | Adds `w_1st_serve_pct`, `l_1st_serve_pct`, `w_1st_serve_won_pct`, `l_1st_serve_won_pct`, `w_bp_conversion_pct`, `l_bp_conversion_pct` | Lets mappers and analytics work with rates instead of raw counters |
| Distributed storage (HDFS) | `src/load_*.py` (`hdfs dfs -cat`), documented upload path `/user/giselledmello/tennis/` | Stores the cleaned CSV and all job outputs where every node can read them | Required for distributed batch processing |
| MapReduce (mapper / shuffle / reducer) | `src/mapreduce/*_mapper.py`, `*_reducer.py` (3 jobs) | Aggregates wins per player, statistics per surface, counts and duration per tournament | Parallel aggregation of 15,980 rows |
| Combiner-style pre-aggregation | Mapper emits a value per row (`winner_name\t1`, per-match averages) | Keeps the map output small and moves work to the shuffle/reduce phase | Standard MapReduce optimisation |
| NoSQL document storage | `src/mongodb/tennis_queries.py`, `src/stream_mining/stream_mongodb.py` | Stores aggregated results and stream events as documents | Results are returned by the API as-is; no joins, no schema migration |
| NoSQL querying / indexing | `TennisQueries` (`$group`, `$addToSet`, sorted `find`, compound indexes) | Distinct counts, top-N by index, recent events by `source + timestamp` desc | Fast dashboard reads over 31,960 events |
| Aggregation | `TennisQueries` + Pandas `groupby` in `src/analytics/` | Win counts, per-surface and per-tournament statistics, per-season trajectories | Core of every visible feature |
| Filtering | `get_match_analysis()` (year/surface/tournament/player), API substring `search`, H2H both-players filter | Narrows 15,980 rows to what the user asked for | Basis of the Matches and Head-to-Head pages |
| Stream processing (sequential event processing) | `src/stream_mining/stream_processor.py`, `tennis_match_stream.py`, `live_tennis_stream.py` | Turns matches into `MATCH_WIN`/`MATCH_LOSS` events and processes them one at a time | Demonstrates stream-style processing over match data |
| Bloom Filter (stream filtering) | `src/stream_mining/bloom_filter.py`, used by `StreamProcessor.process_event()` | Constant-memory "have I seen this `event_id`?" test | Duplicate detection without storing every id |
| Sliding window | `src/stream_mining/sliding_window.py` (`deque(maxlen=100)`) | Keeps only the newest 100 events; `active_window_size`, `recent_events` | Bounded memory for recent-activity answers |
| Flajolet–Martin (approximate distinct counting) | `src/stream_mining/flajolet_martin.py` | Estimates the number of distinct players from hash trailing zeros | Constant-memory cardinality next to the exact count |
| Exact distinct counting | `src/stream_mining/distinct_count.py` | Set-based exact counts of distinct players and matches | Ground truth to compare against the estimate |
| Frequency counting | `collections.Counter` in `StreamProcessor` | `event_type_counts`, `top_players_by_events`, `top_matches_by_events` | "Most active" analytics on the stream |
| Live event processing | `src/stream_mining/live_tennis_stream.py` + `live_event_adapter.py` | Polls the live ATP feed every 15 s, converts rows into `LIVE_MATCH_UPDATE` events | Backs the "Live ATP Matches" card |
| Event store upsert | `MongoStreamStore.save_metrics(metric_key='live_stream_current')` | One live metrics document updated in place instead of appending | Keeps the live state queryable in O(1) |
| API-based data processing | `src/api/tennis_dashboard_api.py` | Read-only JSON layer; per-request analytics for players/matches/H2H | Single contract between data and UI; CORS-free because it also serves the page |
| Visualization | `frontend/index.html` (Chart.js doughnut, trajectory chart, radar chart, H2H share bar) | Renders aggregated results as charts | Makes the analytics inspectable by non-technical users |

**Supporting technologies (not BDA concepts, but load-bearing)**: Python 3.10, Pandas/NumPy
(ETL and per-request analytics), Starlette + Uvicorn (ASGI API and static file serving), PyMongo +
certifi + python-dotenv (MongoDB access and configuration), requests (live feed), Streamlit
(optional second dashboard).

**Frontend technologies**: plain HTML/CSS/vanilla JavaScript, Tailwind-style utility classes,
Chart.js, Font Awesome. The frontend contains **no** analytics code — every number is rendered from
an API response.

**Not implemented anywhere in this repository** (do not claim these): content-based/cosine
recommendation or player-similarity scoring (`src/recommendation/player_similarity.py` is a 0-byte
file), any R statistical analysis (`r_analysis/*.R` are 0-byte files), matrix–vector MapReduce,
relational-algebra MapReduce, DGIM / Datar–Gionis–Indyk–Motwani decaying windows, clustering or
social-graph analysis.
---

# Algorithms

Every significant algorithm actually implemented, with the logic as the code performs it.

### Season normalization — `season_years()`

**Purpose.** Decide which *season* a match belongs to, because the CSV's `year` column follows the
tournament start date and would label the 2019-opening matches (played 2018-12-31) as 2018.

**Input.** Any DataFrame of match rows (the full dataset, a filtered slice, or a single row).

**Process.**
1. Extract a leading 4-digit year from `tourney_id` (`2019-M020` → `2019`).
2. Where that is missing, extract it from `match_id` (`2019-M020_271` → `2019`).
3. Where both are missing, fall back to the dataset's own `year` column.
4. Cast to nullable `Int64`, so an unusable row stays missing (and is skipped by aggregations)
   instead of silently becoming year 0.

**Output.** A nullable integer Series aligned with the input frame.

**Implementation.** `src/analytics/player_analysis.py` → `season_years()`. Imported and reused by
`src/analytics/match_analysis.py` and `src/analytics/head_to_head.py`, so Players, Matches and
Head-to-Head can never disagree.

---

### Player win rate and career aggregates

**Purpose.** Answer "how did this player do?" from raw match rows.

**Input.** One player name; the cleaned CSV.

**Process.** Filter rows where the player is winner or loser; count wins and losses; sum aces and
double faults from the matching winner/loser columns; average the player's `w_*` and `l_*` percentage
columns together; average `minutes`; take the last non-null ranking.

**Output.** `{matches, wins, losses, win_rate, total_aces, total_double_faults,
avg_first_serve_pct, avg_first_serve_won_pct, avg_break_point_conversion, avg_match_duration,
latest_rank, yearly_performance[], surface_performance[]}`.

**Formulas used.**

```
matches   = wins + losses
win_rate  = wins / matches * 100
losses(season)  = matches(season) - wins(season)
win_rate(season)= wins(season) / matches(season) * 100
```

**Implementation.** `src/analytics/player_analysis.py` → `get_player_analysis()`.
`/api/players` recomputes the same definitions in `player_list_rows()` with `value_counts()` so the
list and the profile agree.

---

### Player wins by MapReduce (word-count pattern)

**Purpose.** Count how many matches each player won.

**Input.** One CSV line per match, read from HDFS.

**Process.**
*Mapper* (`player_wins_mapper.py`): `csv.DictReader`, take `winner_name`, skip the row if empty, and
emit `winner_name \t 1`. No counting happens here.
*Shuffle/sort*: Hadoop groups and sorts by key so all lines for one player are consecutive.
*Reducer* (`player_wins_reducer.py`): keep `current_player` / `current_wins`; add each line's value;
when the key changes, print the accumulated total and start a new accumulator; flush the last player
after the loop.

**Output.** `player_name \t win_count`, e.g. `Medvedev, Daniil\t166` → MongoDB `player_wins`
(391 documents).

**Implementation.** `src/mapreduce/player_wins_mapper.py`, `src/mapreduce/player_wins_reducer.py`,
loaded by `src/load_player_wins.py`.

---

### Surface statistics by MapReduce (average of per-match averages)

**Purpose.** Compare hard / clay / grass on volume, aces, double faults, first serve, break points
and duration.

**Input.** The cleaned CSV on HDFS.

**Process.**
*Mapper* (`surface_analysis_mapper.py`): skip the header (`row[0] == "tourney_id"`) and any row
whose column count does not match the 57-column schema; read `surface` and skip rows without one;
for every match compute the **average of the winner's and the loser's value** for aces, double
faults, first-serve %, break-point conversion %, plus `minutes`; emit
`surface \t 1 \t avg_aces \t avg_df \t avg_first_serve \t avg_bp \t minutes`, using `-1` as a
"missing value" sentinel.
*Reducer* (`surface_analysis_reducer.py`): accumulate `match_count` and, per statistic, a running
sum **and** a count of valid samples, skipping `-1`; emit
`surface \t match_count \t avg_aces \t avg_df \t avg_first_serve \t avg_bp \t avg_duration`
(2 decimals).

**Output.** One line per surface → MongoDB `surface_analysis` (Clay 4,772 / avg_aces 4.08 /
avg_first_serve_pct 62.8 / avg_break_point_conversion_pct 43.15 / avg_duration_minutes 115.38, etc.).

**Implementation.** `src/mapreduce/surface_analysis_mapper.py`,
`src/mapreduce/surface_analysis_reducer.py`, `src/load_surface_analysis.py`.

---

### Tournament statistics by MapReduce (fixed column parsing)

**Purpose.** Match count and average duration per tournament.

**Input.** The cleaned CSV on HDFS.

**Process.**
*Mapper* (`tournament_mapper.py`): `csv.reader` with **fixed column positions** (column 1 =
`tourney_name`, column 26 = `minutes`), guarded by `if len(row) <= 26: continue` and a header check;
unparsable durations become `-1`; emit `tournament \t 1 \t duration`.
*Reducer* (`tournament_reducer.py`): count matches, average only durations that are not `-1`, emit
`tournament \t match_count \t avg_duration`.

**Output.** `ATP Rio de Janeiro\t31\t135.06` → MongoDB `tournament_analysis` (477 documents).

**Implementation.** `src/mapreduce/tournament_mapper.py`, `src/mapreduce/tournament_reducer.py`,
`src/load_tournament_analysis.py`.
---

### Head-to-head selection and counting

**Purpose.** Compare two players using only their actual recorded meetings.

**Input.** `player1`, `player2`.

**Process.**
1. Load the cleaned CSV; `year = season_years(df)`.
2. Keep rows containing both players in either orientation:
   `(winner_name == player1 & loser_name == player2) | (winner_name == player2 & loser_name == player1)`.
   Each kept row is one meeting → the result is order-independent.
3. `player_a_won = winner_name == player1`; `total_matches = len(rows)`;
   `player1_wins = sum(player_a_won)`; `player2_wins = total - player1_wins`;
   `win_rate = wins / total * 100` for both.
4. Sort the meetings newest first for display.
5. `groupby('surface')` → `matches` and `player_a_wins` per surface (missing surfaces dropped).

**Output.** Totals, per-player wins and win rates, ordered match history, surface breakdown; or
`None` when the players never met.

**Implementation.** `src/analytics/head_to_head.py` → `get_head_to_head()`; shaped into JSON by
`src/api/tennis_dashboard_api.py` → `build_head_to_head()`.

---

### Bloom Filter (stream duplicate detection)

**Purpose.** Decide whether a stream event has already been seen, using fixed memory.

**Input.** An `event_id` string (`<match_id>_winner` / `<match_id>_loser` / `live_<id>_<seq>_p1|p2`).

**Process.**
1. `size = max(1, int(-capacity * ln(error_rate) / ln(2)²))` — with the defaults
   (`capacity=100_000`, `error_rate=0.01`) that is ~958 k bits (~117 KB).
2. `num_hashes = max(1, int((size / capacity) * ln 2))` (~7).
3. **Double hashing:** two SHA-256 digests (`b"hash_1:"+data`, `b"hash_2:"+data`) produce `hash_1`
   and `hash_2`; position *i* is `(hash_1 + i * hash_2) % size`. SHA-256 is used because Python's
   built-in `hash()` is randomised per process and would break reproducibility.
4. `add(item)` sets those bits in a `bytearray`; `might_contain(item)` returns `True` only if **all**
   bits are set.

**Output.** `False` = definitely not seen. `True` = *probably* seen. The filter never stores items,
only bits, and can produce false positives but not false negatives.

**How the project uses it.** `StreamProcessor.process_event()` calls `might_contain(event_id)`
*before* counting: a hit increments `duplicate_events` (the event is still stored and counted in the
frequency counters, only flagged), then the id is added. Current metrics: 3 duplicates out of
31,960 events.

**Implementation.** `src/stream_mining/bloom_filter.py`; used by `src/stream_mining/stream_processor.py`.

---

### Sliding window

**Purpose.** Keep the newest N events with bounded memory.

**Input.** Configured `max_size` (100 in both pipelines).

**Process.** A `collections.deque(maxlen=100)`. `add()` appends; when full, Python automatically
discards the oldest element. `get_events()` returns oldest → newest; `latest()` / `oldest()` give the
edges; `size()` / `is_full()` report occupancy.

**Output.** `active_window_size` (occupancy) and `recent_events` (the retained events) in the
metrics document; `get_recent_events()` exposes the same list to callers.

**Implementation.** `src/stream_mining/sliding_window.py`; used by `StreamProcessor`, surfaced by
`TennisQueries.get_recent_stream_events()` for the Dashboard "Recent Matches" table.

---

### Flajolet–Martin approximate distinct counting

**Purpose.** Estimate how many distinct players appear in the stream, using constant memory.

**Input.** Player names, one per event.

**Process.**
1. `num_hashes` (default 32) independent hashes per value:
   `int.from_bytes(sha256(f"{seed}:{item}").digest()[:8], "big")` — a 64-bit hash.
2. Count the **trailing zero bits** of each hash (`(value & -value).bit_length() - 1`; a zero hash
   counts as 64) and keep the running maximum per seed: `max_trailing_zeros[seed]`.
3. Group the hashes (`group_size = max(1, num_hashes // 4)` = 8), average the trailing-zero counts
   inside each group and convert:
   `estimate = (2 ** average_zeros) / 0.77351` (0.77351 is the Flajolet–Martin constant).
4. Return the **median** of the group estimates, so a single unlucky hash cannot dominate.

**Output.** An integer estimate. On the current dataset it reports ≈1,900 distinct players versus
the exact 842 — the classic failure mode of approximate counting on a small-cardinality stream, and
the reason the exact counter is kept next to it.

**Implementation.** `src/stream_mining/flajolet_martin.py`; used by `StreamProcessor` as
`estimated_distinct_players` (stored alongside `exact_distinct_players`).
---

### Exact distinct counting

**Purpose.** Ground truth for cardinality, stored next to the estimate.

**Process.** `DistinctCounter` wraps a Python `set`; `add(item)` returns `True` if the item was new.

**Output.** `exact_distinct_players` (842) and `exact_distinct_matches` (15,980).

**Implementation.** `src/stream_mining/distinct_count.py`; instantiated twice in `StreamProcessor`
(`distinct_players`, `distinct_matches`).

---

### Stream frequency counting and event identity

**Purpose.** Produce "top players / top matches / event-type distribution", and make events
reproducible so duplicates are detectable.

**Process.**
* Event id: `_create_event_id()` uses the explicit `event_id` when present, otherwise a SHA-256 of
  `timestamp|match_id|player|event_type|point|score` — deterministic across runs.
* Field extraction is tolerant: `_get_player` accepts `player` / `player_name` / `participant`,
  `_get_match_id` accepts `match_id` / `match` / `matchId`, `_get_event_type` accepts
  `event_type` / `event` / `type` (upper-cased), `_get_timestamp` accepts `timestamp` / `time` /
  `datetime` and falls back to `now()`.
* Invalid events (not a dict, or empty) increment `invalid_events` and are skipped.
* `collections.Counter` objects tally `event_type_counts`, `player_event_counts` and
  `match_event_counts`; `most_common(10)` produces the top-player and top-match lists.

**Output.** The `stream_metrics` fields (see [Streaming](#streaming--real-time-processing)).

**Implementation.** `src/stream_mining/stream_processor.py`.

---

### Tournament name normalisation and substring search

**Purpose.** Make the tournament filter work despite inconsistent spelling, and support free-text
search.

**Process.** `tournament_names()` picks the dataset's own label; `normalise_name()` lowercases and
collapses spaces; `normalise_name_of_series()` applies that to a column, after which rows are
relabelled with the filter spelling and compared with `==`. Search lowercases the needle and uses
`str.contains(needle, regex=False)` over winner, loser and tournament.

**Output.** Stable filter results and a substring search that the analytics layer itself does not
support.

**Implementation.** `src/api/tennis_dashboard_api.py` → `normalise_name()`,
`tournament_filter_value()`, `tournament_names()`, `normalise_name_of_series()`, `build_match_list()`.

---

# MapReduce

The three jobs are Hadoop-streaming jobs: Python scripts that read stdin and write stdout, so Hadoop
provides the parallelism, the shuffle and the grouping. Typical run (paths must match the loaders):

```bash
hdfs dfs -put -f data/processed/tennis_matches_clean.csv /user/giselledmello/tennis/data/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input  /user/giselledmello/tennis/data/tennis_matches_clean.csv \
  -mapper "python3 src/mapreduce/player_wins_mapper.py" \
  -reducer "python3 src/mapreduce/player_wins_reducer.py" \
  -output /user/giselledmello/tennis/output/player_wins
```

### Job A — Player Wins

| | |
|---|---|
| Input | Cleaned CSV rows on HDFS |
| Mapper | `src/mapreduce/player_wins_mapper.py` — emits `winner_name \t 1`, skips rows without a winner |
| Key | `winner_name` |
| Value | `1` per match |
| Shuffle/sort | Hadoop groups and sorts by key so each player's lines are consecutive |
| Reducer | `src/mapreduce/player_wins_reducer.py` — sums values per player, emits on key change |
| Output | `player_name \t win_count` in `/user/giselledmello/tennis/output/player_wins/part-00000` |
| Purpose | "Top Players by Wins" on the Dashboard |
| Loaded by | `src/load_player_wins.py` → MongoDB `player_wins` (391 docs) |

### Job B — Surface Analysis

| | |
|---|---|
| Input | Cleaned CSV rows on HDFS |
| Mapper | `src/mapreduce/surface_analysis_mapper.py` — validates the 57-column schema, skips rows without a surface, averages winner/loser aces, double faults, first serve %, break-point %, plus minutes; `-1` marks a missing statistic |
| Key | `surface` (Hard / Clay / Grass) |
| Value | `1, avg_aces, avg_df, avg_first_serve, avg_bp, minutes` |
| Shuffle/sort | Grouped and sorted by surface |
| Reducer | `src/mapreduce/surface_analysis_reducer.py` — per-surface match count and mean of each valid statistic |
| Output | `surface \t match_count \t avg_*` (2 decimals) in `output/surface_analysis/part-00000` |
| Purpose | "Matches by Surface" chart and surface statistics |
| Loaded by | `src/load_surface_analysis.py` → MongoDB `surface_analysis` (3 docs) |

### Job C — Tournament Analysis

| | |
|---|---|
| Input | Cleaned CSV rows on HDFS |
| Mapper | `src/mapreduce/tournament_mapper.py` — `csv.reader` with fixed positions (col 1 = tournament, col 26 = minutes), guarded by a length check and a header check |
| Key | `tourney_name` |
| Value | `1, duration` (`-1` if unparsable) |
| Shuffle/sort | Grouped and sorted by tournament |
| Reducer | `src/mapreduce/tournament_reducer.py` — match count + mean of valid durations |
| Output | `tournament \t match_count \t avg_duration` in `output/tournament_analysis/part-00000` |
| Purpose | Tournament activity and duration statistics |
| Loaded by | `src/load_tournament_analysis.py` → MongoDB `tournament_analysis` (477 docs) |

### Loaders (HDFS → MongoDB)

All three follow the same pattern: run `hdfs dfs -cat <path>` via `subprocess`, split each line on
`\t` (tab only, so spaces in player names survive), build documents, then
`collection.bulk_write([UpdateOne({...}, {"$set": {...}}, upsert=True), ...])`. Keys: `player_name`,
`surface`, `tournament` — so re-running a loader updates instead of duplicating.

> Note: the loaders read only `part-00000`. With more data and multiple reducers, they would need to
> read every part file.

---
# Streaming / Real-Time Processing

Everything in this section lives in `src/stream_mining/`.

## Components and how they connect

```
historical: data/processed/tennis_matches_clean.csv
        │  pandas read + chronological sort (match_date → tourney_date → date)
        ▼
tennis_match_stream.create_match_events(row)   → 2 events per match
        ▼
StreamProcessor.process_event(event)            → Bloom Filter, exact counters,
                                                   Flajolet–Martin, sliding window
        ▼
MongoStreamStore.save_events() / save_metrics() → MongoDB (source="historical")
        ▼
data/processed/real_atp_stream_results.json     (metrics + all events)

live: https://api.livetennisapi.com/api/public/v1/matches
        │  requests.get(), filtered to tour == "atp"
        ▼
live_event_adapter.create_live_events(match)    → up to 2 LIVE_MATCH_UPDATE events
        ▼
same StreamProcessor + MongoStreamStore         → MongoDB (source="live",
                                                     metrics upserted as live_stream_current)
```

## Event model

A stream event is **one player's result in one match**, not the match itself.

| Event type | Produced by | Fields |
|---|---|---|
| `MATCH_WIN` | `tennis_match_stream.create_match_events()` | `event_id` = `<match_id>_winner`, `player` (winner), `opponent` (loser), `role`, `match_id`, `tournament`, `surface`, `round`, `score`, `minutes`, `year`, `ranking`, `ranking_points`, `aces`, `double_faults`, `first_serve_pct`, `timestamp` |
| `MATCH_LOSS` | same function | `event_id` = `<match_id>_loser`, player/opponent swapped, `role="loser"` |
| `LIVE_MATCH_UPDATE` | `live_event_adapter.create_live_events()` | `event_id` = `live_<match_id>_<sequence>_p1\|_p2`, `player`, `player_id`, `opponent`, `status`, `tour`, `tournament`, `surface`, `round`, `format`, score fields |

15,980 matches × 2 = **31,960 historical events**. NaN values are converted to `None` by
`safe_value()` so the JSON and the MongoDB documents stay clean.

## Live adapter and poller

`live_event_adapter.create_live_events(match)` flattens the feed's nested `players.p1` / `players.p2`
structure into one event per named player, sharing the match information. The event id includes the
score `sequence`, so a new score state is a new event while a repeated poll of the same state
produces the same id — which is what the Bloom Filter detects.

`live_tennis_stream.py` polls every `POLL_INTERVAL = 15` seconds, filters ATP matches locally (the
feed returns several tours), processes the events, writes them with `source="live"` and **upserts**
the metrics document under `metric_key="live_stream_current"` instead of appending a snapshot every
cycle. `while True:` + `time.sleep(POLL_INTERVAL)` — it runs until interrupted.

## Metrics produced by StreamProcessor

`get_metrics()` returns: `total_events`, `duplicate_events`, `duplicate_rate`, `invalid_events`,
`exact_distinct_players`, `estimated_distinct_players`, `exact_distinct_matches`,
`active_window_size`, `configured_window_size`, `event_type_counts`, `top_players_by_events`,
`top_matches_by_events`, `first_timestamp`, `last_timestamp`, `recent_events`.
`process_csv()` additionally records `csv_file` and `csv_rows_read`.

Current values: 31,960 events, 3 duplicates (rate ≈ 9.4e-5), 0 invalid, 842 exact players,
≈1,900 estimated players, 15,980 exact matches, window 100/100, `{MATCH_WIN: 15980, MATCH_LOSS: 15980}`.

## MongoDB store

`stream_mongodb.MongoStreamStore` connects with `MONGODB_URI` from `.env` (TLS via `certifi`),
creates indexes, and writes:
* `stream_events` — one document per event, plus `source`, `sequence`, `stale`, `stored_at` and the
  original `event_data` payload.
* `stream_metrics` — the metrics snapshot (`source`, `timestamp`, `metric_key`).

Methods: `build_event_document()`, `save_event()`, `save_events()`, `save_metrics()`,
`get_event_count()`, `get_metrics_count()`, `close()`.

## What is connected to the frontend

| Stream component | Connected to the UI? | Where it shows up |
|---|---|---|
| Historical events | **Yes** | "Recent Matches" table (`/api/dashboard/recent-events`) |
| Live metrics + live events | **Yes** | "Live ATP Matches" card (`/api/dashboard/live`) |
| Stream metrics snapshot | API only | `/api/dashboard/stream` and `/api/dashboard/pipeline` exist but the frontend does not call them |
| Event-type distribution | API only | `/api/dashboard/event-types` (no UI) |
| `real_atp_stream_results.json` | Second dashboard only | `dashboard/stream_analytics.py` reads this file (needs the stream pipeline to have run) |
| Sliding-window contents | Indirect | The window is what `recent_events` reflects inside the metrics document |
| Bloom Filter / Flajolet–Martin | **Not connected to the frontend** | They only affect the metrics document; no visible widget reads `estimated_distinct_players` or `duplicate_events` |

> Implemented in code but not currently connected to the main frontend: the Bloom Filter duplicate
> counter, the Flajolet–Martin estimate, the sliding-window size and the event-type distribution.
> They are produced by the pipeline and readable through the API, but the Dashboard does not
> display them.
---

# Analytics Modules

`src/analytics/` — Pandas analytics over `data/processed/tennis_matches_clean.csv`. Every module
loads the CSV through its own `load_data()` (relative path, so **the API must be started from the
project root**), and the match/head-to-head modules import `season_years()` from `player_analysis` so
the season rule is defined once.

| Module | Purpose | Input | Key calculations | Output | Used by |
|---|---|---|---|---|---|
| `player_analysis.py` | Player list definitions, profile statistics, season helper | Player name (or a DataFrame for `season_years()`) | wins/losses, win rate, ace and double-fault sums, serve/bp averages, `latest_rank`, `groupby(season)`, `groupby(surface)` | Career summary + `yearly_performance[]` + `surface_performance[]` | `players_page()`, `player_detail_page()`; `season_years()` is imported by `match_analysis` and `head_to_head` |
| `match_analysis.py` | Filterable match list and single-match detail | `match_id`, `year`, `tournament`, `surface`, `player` | boolean filters, column projection, sort by `year, match_id` | DataFrame of matches / one match dict | `matches_page()`, `match_detail_page()` |
| `head_to_head.py` | Two-player comparison | `player_a`, `player_b` | both-players filter (either orientation), win counting, win rates, newest-first sort, `groupby('surface')` | Totals, per-surface breakdown, ordered match history, or `None` | `head_to_head_page()` |
| `surface_analysis.py` | Standalone surface helper | – | reads the cleaned CSV and groups by surface | Per-surface aggregates | **Not imported anywhere** — implemented but not connected to the frontend |

Each module also has an `if __name__ == "__main__":` demo block that prints sample output, so they can
be run directly for a quick check:

```bash
.venv/bin/python src/analytics/match_analysis.py
.venv/bin/python src/analytics/player_analysis.py
```

---

# Data Preprocessing

## Raw datasets

| File | Rows | Notes |
|---|---|---|
| `data/raw/atp_matches_2019.csv` | 2,806 | includes the 85 matches dated 2018-12-31 |
| `data/raw/atp_matches_2020.csv` | 1,462 | COVID-shortened season |
| `data/raw/atp_matches_2021.csv` | 2,733 | |
| `data/raw/atp_matches_2022.csv` | 2,917 | |
| `data/raw/atp_matches_2023.csv` | 2,986 | |
| `data/raw/atp_matches_2024.csv` | 3,076 | |
| `data/raw/atp_players.csv` | 65,989 | player master list; **not merged into the match dataset** |

## `src/preprocessing/clean_data.py` — the pipeline

Run from the project root: `.venv/bin/python src/preprocessing/clean_data.py`

| Step | Code | Effect |
|---|---|---|
| 1. Load | `sorted(RAW_DIR.glob("atp_matches_*.csv"))` → `pd.concat` | one DataFrame of 15,980 rows |
| 2. Duplicates | `df.drop_duplicates()` | exact duplicate rows removed |
| 3. Dates | `pd.to_datetime(tourney_date, format="%Y%m%d", errors="coerce")` | invalid dates → missing (not fatal) |
| 4. Year | `df["year"] = df["tourney_date"].dt.year` | start-date year (see season normalization) |
| 5. Text | `.astype("string").str.strip()` on 12 columns | whitespace normalised |
| 6. Surfaces | `.str.title()`, anything not Hard/Clay/Grass → `pd.NA` | 53 rows keep a missing surface and are excluded from surface analytics |
| 7. Numeric | `pd.to_numeric(..., errors="coerce")` on 30+ columns | numeric typing without crashing on bad cells |
| 8. `match_id` | `tourney_id + "_" + match_num` | unique per match (15,980 unique values) |
| 9. Derived percentages | formulas below | six new columns |
| 10. Validation | values `< 0` or `> 100` → `pd.NA` | impossible percentages removed, rows kept |
| 11. Sort | `tourney_date, tourney_name, match_num` | chronological output |
| 12. Save | `data/processed/tennis_matches_clean.csv` | 15,980 × 57 |

```
w_1st_serve_pct      = w_1stIn / w_svpt * 100
w_1st_serve_won_pct  = w_1stWon / w_1stIn * 100
w_bp_conversion_pct = (w_bpFaced - w_bpSaved) / w_bpFaced * 100
```

**Missing-value strategy.** Nothing is deleted for being incomplete: invalid values become missing
(`coerce` / `pd.NA`) and every consumer either averages only valid values (`mean()` skips `pd.NA`) or
filters rows explicitly (`dropna(subset=["surface"])`).

## `src/preprocessing/data_profile.py`

Reads the **cleaned** CSV (it prints "Run clean_data.py first" if it is missing) and prints: dataset
size, the full column list, matches by year, surface distribution and tournament levels. Useful after
a cleaning change.
---

# MongoDB

**Database:** `tennis_analytics` (`DATABASE_NAME` in `src/mongodb/tennis_queries.py`; the
`MongoStreamStore` uses the same literal). Connection string comes from `MONGODB_URI` in `.env`
(`python-dotenv`), TLS CA bundle from `certifi`.

## Collections

| Collection | Docs | Written by | Read by | Used by the UI? |
|---|---|---|---|---|
| `player_wins` | 391 | `src/load_player_wins.py` (MapReduce job A) | `get_top_players_by_wins()` | **Yes** – Top Players by Wins |
| `surface_analysis` | 3 | `src/load_surface_analysis.py` (MapReduce job B) | `get_matches_by_surface()`, `get_surface_performance()` | **Yes** – Matches by Surface |
| `tournament_analysis` | 477 | `src/load_tournament_analysis.py` (MapReduce job C) | `get_tournament_analysis()` | Not directly; the Matches filter list is built from the CSV |
| `stream_events` | 31,960 | `stream_mongodb.MongoStreamStore.save_events()` (historical + live) | `get_recent_stream_events()`, `get_event_type_distribution()`, `get_current_live_events()`, `get_top_players_by_stream_events()`, `get_stream_events_by_surface()` | **Yes** – Recent Matches |
| `stream_metrics` | 3 | `stream_mongodb.save_metrics()` (historical snapshot + `live_stream_current`) | `get_latest_stream_metrics()`, `get_current_live_metrics()` | **Yes** – Live card |

## Document shapes

```jsonc
// player_wins
{ "player_name": "Daniil Medvedev", "win_count": 166 }

// surface_analysis
{ "surface": "Clay", "match_count": 4772, "avg_aces": 4.08,
  "avg_double_faults": 2.71, "avg_first_serve_pct": 62.8,
  "avg_break_point_conversion_pct": 43.15, "avg_duration_minutes": 115.38 }

// tournament_analysis
{ "tournament": "ATP Rio de Janeiro", "match_count": 31, "avg_duration_minutes": 135.06 }

// stream_events (abridged)
{ "event_id": "2019-M020_271_winner", "event_type": "MATCH_WIN", "source": "historical",
  "match_id": "2019-M020_271", "player": "Denis Kudla", "opponent": "Taylor Fritz",
  "role": "winner", "tournament": "Brisbane", "surface": "Hard", "round": "R32",
  "score": "7-6(5) 6-7(2) 6-4", "minutes": 144.0, "ranking": 63.0, "aces": 8.0,
  "double_faults": 4.0, "first_serve_pct": 117.0, "year": 2018,
  "timestamp": "2018-12-31 00:00:00", "sequence": ..., "stale": ...,
  "stored_at": ..., "event_data": { /* original source row */ } }

// stream_metrics
{ "source": "historical", "timestamp": ..., "metric_key": "live_stream_current",
  "total_events": 31960, "duplicate_events": 3, "duplicate_rate": 9.3867e-05,
  "invalid_events": 0, "exact_distinct_players": 842, "estimated_distinct_players": 1900,
  "exact_distinct_matches": 15980, "active_window_size": 100, "configured_window_size": 100,
  "event_type_counts": {"MATCH_WIN": 15980, "MATCH_LOSS": 15980},
  "top_players_by_events": [...], "top_matches_by_events": [...],
  "first_timestamp": "2018-12-31 00:00:00", "last_timestamp": "2024-12-18 00:00:00" }
```

Note: `stream_events.year` and `stream_metrics.first_timestamp` use the **dataset's start-date year**,
so season-opening matches appear as `2018`. Season-aware pages use `season_years()` on the CSV, not
these fields.

## Query layer (`src/mongodb/tennis_queries.py`)

`TennisQueries` is **read-only**. It holds one `MongoClient` (`get_queries()` in the API reuses a
singleton) and exposes ~20 methods: collection counts, distinct counts (`$group` + `$addToSet`), top-N
queries, sorted `find()`s with projections, and live-metric lookups by `metric_key`.

Indexes (`INDEX_PLAN`, created by `ensure_analysis_indexes()`):

* `stream_events`: `source`, `surface`, `tournament`, and compound `source + timestamp` **descending**
  (the recent-events query).
* `stream_metrics`: `source`, `metric_key`.
* `player_wins`: `win_count` descending.
* `tournament_analysis`: `match_count` descending.

Helpers: `jsonable()` / `dataframe_records()` in the API convert Pandas values (NaN, numpy types)
into JSON-safe values, so missing data is returned as `null` rather than breaking the response.

## Who writes and who reads

* **Writers:** `src/load_*.py` (MapReduce results) and `src/stream_mining/stream_mongodb.py`
  (stream output). Nothing else.
* **Readers:** `src/api/tennis_dashboard_api.py` only, through `TennisQueries`.
* **Verification helpers:** `src/verify_mongodb.py` prints collections, document counts, sample
  documents and required-field checks; `src/database_test.py` is a minimal connectivity/ping script.
  Both are manual tools, not part of the app.

**Is MongoDB connected to the visible frontend?** Yes — every Dashboard card reads from it
(`/api/dashboard/*`). The Players, Matches and Head-to-Head pages deliberately read the cleaned CSV
instead, because they need per-request, season-normalised filtering.

---

# R Analysis

`r_analysis/` contains exactly two files:

| File | Size | Status |
|---|---|---|
| `r_analysis/statistical_analysis.R` | 0 bytes | Empty placeholder |
| `r_analysis/visualizations.R` | 0 bytes | Empty placeholder |

**There is no R analysis implemented in this repository.** No R script, no statistical test, no R
visualisation, no R output file, and nothing imports or executes them. Do not describe R analysis as
a project feature; if it is needed for a submission, it has to be written first.

---
---

# Frontend / Dashboard

There are **two** UIs in this repository. The website is `frontend/index.html`; the Streamlit page is
a separate, optional analytics view.

## A. Website — `frontend/index.html`

One self-contained file: HTML structure, CSS (Tailwind-style utility classes), vanilla JavaScript,
Chart.js and Font Awesome from a CDN. There is no build step — `frontend/package-lock.json` is an
empty stub and no npm package is required. The backend serves the file at `/`.

Navigation is a `switchTab(tabId)` function that shows one `<section id="tab-*">`, updates the nav
highlight and the header title, then triggers that tab's render function:

| Nav item | Tab id | Render trigger | Header title |
|---|---|---|---|
| Home | `tab-landing` | `renderLandingCoverage()` | Home |
| Dashboard | `tab-dashboard` | `renderDashboard()` | Main Analytics Dashboard |
| Players | `tab-player` | `renderPlayerAnalysis()` | Player Deep Performance Analysis |
| Matches | `tab-match` | `renderMatchTable()` | Historical Match Records & Search |
| Head-to-Head | `tab-h2h` | `renderH2H()` | Head-to-Head Rivalry Analysis |
| Live Matches | *(none)* | `openLiveMatches()` → dashboard + scroll to `#dashboard-live-card` | — |

### Data layer in the frontend

| Constant / helper | Value | Used by |
|---|---|---|
| `DASH_API` | `/api/dashboard` | `fetchDash()` → overview, surfaces, top-players, recent-events, live |
| `PLAYERS_API` | `/api/players` | `loadPlayersList()`, `onPlayerSelected()` |
| `MATCHES_API` | `/api/matches` | `renderMatchTable()`, `openMatchModal()` |
| `HEAD_TO_HEAD_API` | `/api/head-to-head` | `renderH2H()` |
| `playersList`, `playersLoaded` | cached player list | shared by the Players page and both H2H dropdowns |
| `h2hRequestId` | request counter | ignores out-of-order H2H responses |
| `activeCharts`, `destroyChart()` | Chart.js instances | doughnut, trajectory, radar |
### Section-by-section

| UI block | Key element ids | Data | Renderer |
|---|---|---|---|
| Sidebar brand / footer | – | static | – |
| Top bar (title, match count, "Tennis Analytics") | `#page-title-header`, `#header-match-count` | match count from `/api/dashboard/overview` | `renderDashKpis()`, `renderLandingCoverage()` |
| Landing hero + 6 feature cards | – | static text | – |
| Landing data coverage | `#landing-coverage` | `/api/dashboard/overview` | `renderLandingCoverage()` |
| Dashboard header | `#dash-status-badge`, `#dash-status-text`, `#dash-dataset-meta`, `#dash-last-refresh` | `/api/dashboard/overview` | `setDashStatus()`, `renderDashKpis()` |
| Dashboard KPI cards | `#kpi-total-matches`, `#kpi-unique-players`, `#kpi-tournaments`, `#kpi-surfaces` (+ notes) | `/api/dashboard/overview` | `renderDashKpis()` |
| Matches by Surface | `#chart-dashboard-surface`, `#surface-legend` | `/api/dashboard/surfaces` | `renderDashSurfaces()` |
| Top Players by Wins | `#top-players-list` | `/api/dashboard/top-players?limit=10` | `renderDashTopPlayers()` |
| Recent Matches | `#recent-events-body` | `/api/dashboard/recent-events?limit=15` | `renderDashRecentEvents()` |
| Live ATP Matches | `#dashboard-live-card`, `#live-message`, `#live-badge`, `#live-badge-text`, `#live-event-count`, `#live-interval` | `/api/dashboard/live` | `renderDashLiveStatus()` |
| Player selector | `#player-selector`, `#player-header-meta` | `/api/players` | `loadPlayersList()`, `retryPlayersList()` |
| Player KPI grid | `#player-kpi-grid` | `/api/players/{name}` | `renderPlayerProfile()` |
| Performance Trajectory | `#player-history-title`, `#chart-player-history` | `yearly_performance[]` | `renderPlayerHistoryChart()` |
| Attribute Vector radar | `#chart-player-radar` | profile averages | `renderPlayerMetricRadar()` |
| Surface Win Rate Breakdown | `#player-surface-table-body` | `surface_performance[]` | `renderPlayerProfile()` |
| Match filters | `#match-filter-year`, `#match-filter-surface`, `#match-filter-tournament`, `#match-search-input` | `filter_options` | `populateMatchFilterOptions()`, `matchFilterParams()` |
| Match table + pager | `#match-count-badge`, `#match-table-body`, `#match-page-info`, `#match-prev-page`, `#match-next-page` | `/api/matches` | `renderMatchTable()`, `renderMatchPager()`, `gotoMatchPage()` |
| Match modal | `#match-modal`, `#modal-tournament-badge`, `#modal-match-title`, `#modal-match-score`, `#modal-stats-container` | `/api/matches/{id}` | `openMatchModal()`, `closeMatchModal()` |
| Head-to-Head | `#h2h-player1`, `#h2h-player2`, `#h2h-hero-card`, `#h2h-matches-body`, `#h2h-th-p1`, `#h2h-th-p2`, `#h2h-surface-table-body` | `/api/head-to-head` | `renderH2H()`, `renderH2HResult()`, `populateH2HPlayerOptions()` |

Shared helpers: `fmtNum()`, `fmtDateTime()`, `fmtDateOnly()`, `fmtPct()`, `fmtMetric()`,
`escapeHtml()`, `surfaceBadgeClass()`, `playerStatusBadge()`, `playerMetaBadge()`, `dashEl()`,
`h2hMessageCard()`, `h2hClearTables()`, `matchTableMessage()`, `clearPlayerAnalysis()`,
`toggleSidebar()`, `showGuestAuthToast()`.

## B. Streamlit view — `dashboard/` (separate, not the website)

| File | Size | Role |
|---|---|---|
| `dashboard/stream_analytics.py` | 8.7 KB | `load_stream_results()` reads `data/processed/real_atp_stream_results.json`; `render_stream_analytics()` renders a Streamlit page: total events, exact/estimated distinct players, distinct matches, duplicate detection, event-type distribution, surface distribution, top players by participation, top tournaments, sliding-window metrics and a stream timeline |
| `dashboard/app.py` | 60 B | Intended Streamlit entry point: calls `render_stream_analytics()` under `if __name__ == "__main__"` — **but it never imports it**, so `streamlit run dashboard/app.py` fails with `NameError` until the import is added |
| `dashboard/utils.py` | 0 B | Empty placeholder |

This view is independent of the website: it is a second way to *demonstrate* the stream metrics, it
needs the generated JSON file (git-ignored), and it is not linked from the navigation.
---

# File Responsibility Map

Callers listed here are the actual importers/callers found in the repository.

## Frontend

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `frontend/index.html` | Entire website: markup, CSS, JS, Chart.js charts, all six tabs and all render functions | Served by `src/api/tennis_dashboard_api.py` at `/` |
| `frontend/package-lock.json` | Empty npm lockfile stub (no build step) | Nothing |

## Dashboard (separate Streamlit view)

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `dashboard/stream_analytics.py` | Streamlit page rendering the stream metrics JSON | `dashboard/app.py` (import missing), run manually |
| `dashboard/app.py` | Streamlit entry point (calls `render_stream_analytics()`) | `streamlit run dashboard/app.py` |
| `dashboard/utils.py` | Empty placeholder | Nothing |

## Analytics

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/analytics/player_analysis.py` | `season_years()`, `get_player_analysis()`, `load_data()` | `src/api/tennis_dashboard_api.py`; `match_analysis.py` and `head_to_head.py` import `season_years()` |
| `src/analytics/match_analysis.py` | `get_match_analysis()`, `get_match_details()` | `src/api/tennis_dashboard_api.py` (`build_match_list`, `match_detail_page`) |
| `src/analytics/head_to_head.py` | `get_head_to_head()` | `src/api/tennis_dashboard_api.py` (`build_head_to_head`) |
| `src/analytics/surface_analysis.py` | Standalone surface helper | Nothing (not imported) |

## Preprocessing

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/preprocessing/clean_data.py` | Builds `data/processed/tennis_matches_clean.csv` | Run manually; its output feeds analytics, MapReduce and the stream |
| `src/preprocessing/data_profile.py` | Prints a profile of the cleaned CSV | Run manually |

## MapReduce and loading

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/mapreduce/player_wins_mapper.py` / `player_wins_reducer.py` | Job A: wins per player | `hadoop jar hadoop-streaming` |
| `src/mapreduce/surface_analysis_mapper.py` / `surface_analysis_reducer.py` | Job B: statistics per surface | `hadoop jar hadoop-streaming` |
| `src/mapreduce/tournament_mapper.py` / `tournament_reducer.py` | Job C: count + duration per tournament | `hadoop jar hadoop-streaming` |
| `src/load_player_wins.py` | `hdfs dfs -cat` job A output → upsert `player_wins` | Run manually after the job |
| `src/load_surface_analysis.py` | `hdfs dfs -cat` job B output → upsert `surface_analysis` | Run manually after the job |
| `src/load_tournament_analysis.py` | `hdfs dfs -cat` job C output → upsert `tournament_analysis` | Run manually after the job |

## Streaming

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/stream_mining/stream_processor.py` | `StreamProcessor`: orchestration + metrics | `tennis_match_stream.py`, `live_tennis_stream.py` |
| `src/stream_mining/bloom_filter.py` | `BloomFilter` | `stream_processor.py` |
| `src/stream_mining/sliding_window.py` | `SlidingWindow` | `stream_processor.py` |
| `src/stream_mining/flajolet_martin.py` | `FlajoletMartin` | `stream_processor.py` |
| `src/stream_mining/distinct_count.py` | `DistinctCounter` | `stream_processor.py` |
| `src/stream_mining/tennis_match_stream.py` | Historical pipeline: CSV → events → store + JSON | `python -m src.stream_mining.tennis_match_stream` |
| `src/stream_mining/live_tennis_stream.py` | Live poller (15 s) → events + upserted live metrics | `python -m src.stream_mining.live_tennis_stream` |
| `src/stream_mining/live_event_adapter.py` | `create_live_events()`: feed row → events | `live_tennis_stream.py` |
| `src/stream_mining/stream_mongodb.py` | `MongoStreamStore`: writes `stream_events`, `stream_metrics` | `tennis_match_stream.py`, `live_tennis_stream.py` |
| `src/stream_mining/test_*.py` | pytest-style checks for the streaming components | Manual test runs |

## MongoDB

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/mongodb/tennis_queries.py` | `TennisQueries`: read-only queries + index plan | `src/api/tennis_dashboard_api.py` |
| `src/mongodb/test_tennis_queries.py` | Tests for the query layer | Manual |
| `src/verify_mongodb.py` | Prints collections, counts, samples, field checks | Manual |
| `src/database_test.py` | Minimal connection/ping script | Manual |
| `mongodb/README.md` | Empty placeholder | Nothing |

## API

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/api/tennis_dashboard_api.py` | All routes, payload builders, JSON conversion, serves `frontend/index.html`, `main()` runs Uvicorn | `python -m src.api.tennis_dashboard_api` |

## R

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `r_analysis/statistical_analysis.R` | Empty placeholder (0 bytes) | Nothing |
| `r_analysis/visualizations.R` | Empty placeholder (0 bytes) | Nothing |

## Other

| File / Folder | Responsibility | Used By |
| ------------- | -------------- | ------- |
| `src/recommendation/player_similarity.py` | Empty placeholder (0 bytes) — **no recommendation/similarity code exists** | Nothing |
| `hadoop/README.md` | Empty placeholder (0 bytes) | Nothing |
| `data/raw/*.csv` | Raw inputs | `clean_data.py` |
| `data/processed/tennis_matches_clean.csv` | Cleaned fact table | analytics, stream, MapReduce |
| `data/processed/real_atp_stream_results.json` | Generated stream output (git-ignored) | `dashboard/stream_analytics.py` |
| `data/stream/events.csv` | Empty placeholder (git-ignored) | Nothing |
| `.env` | Local credentials, git-ignored | every module that talks to MongoDB or the live feed |
| `.env.example` | Variable names only | Teammates |
---

# Quick Developer Reference

| If you want to change… | Edit / Inspect |
| ---------------------- | -------------- |
| Dashboard UI (cards, layout, wording) | `frontend/index.html` → `<section id="tab-dashboard">` |
| Landing page content | `frontend/index.html` → `<section id="tab-landing">` + `renderLandingCoverage()` |
| Navigation items / page titles | `frontend/index.html` → `switchTab()` and the `#nav-*` buttons |
| "Live Matches" behaviour | `frontend/index.html` → `openLiveMatches()` (currently scrolls to `#dashboard-live-card`) |
| Dashboard numbers | `renderDashKpis()`, `renderDashSurfaces()`, `renderDashTopPlayers()`, `renderDashRecentEvents()`, `renderDashLiveStatus()` |
| Surface chart styling / colours | `frontend/index.html` → `renderDashSurfaces()` (`colors` map, Chart.js options) |
| Player page (selector, KPIs, trajectory, radar, surfaces) | `frontend/index.html` → `renderPlayerAnalysis()`, `renderPlayerProfile()`, `renderPlayerHistoryChart()`, `renderPlayerMetricRadar()` |
| Match filters, table, pagination | `frontend/index.html` → `renderMatchTable()`, `matchFilterParams()`, `renderMatchPager()`, `gotoMatchPage()` |
| Match details modal | `frontend/index.html` → `openMatchModal()`, `closeMatchModal()` |
| Head-to-Head UI | `frontend/index.html` → `renderH2H()`, `renderH2HResult()`, `populateH2HPlayerOptions()` |
| Player analysis (win rate, averages, trajectory, surfaces) | `src/analytics/player_analysis.py` → `get_player_analysis()` |
| Season rule (2019–2024, the 2018-12-31 rows) | `src/analytics/player_analysis.py` → `season_years()` |
| Match analysis (filters, projection, sort) | `src/analytics/match_analysis.py` → `get_match_analysis()` |
| Match detail fields | `src/analytics/match_analysis.py` → `get_match_details()` |
| Head-to-head calculation | `src/analytics/head_to_head.py` → `get_head_to_head()` |
| Surface analytics helper (currently unused) | `src/analytics/surface_analysis.py` → `get_surface_analysis()` |
| Data cleaning | `src/preprocessing/clean_data.py` |
| Dataset inspection | `src/preprocessing/data_profile.py` |
| Player-wins MapReduce | `src/mapreduce/player_wins_mapper.py` / `player_wins_reducer.py` |
| Surface MapReduce | `src/mapreduce/surface_analysis_mapper.py` / `surface_analysis_reducer.py` |
| Tournament MapReduce | `src/mapreduce/tournament_mapper.py` / `tournament_reducer.py` |
| HDFS output paths / MongoDB load | `src/load_player_wins.py`, `src/load_surface_analysis.py`, `src/load_tournament_analysis.py` |
| Stream orchestration / metrics | `src/stream_mining/stream_processor.py` → `StreamProcessor` |
| Bloom Filter size / hashing | `src/stream_mining/bloom_filter.py` |
| Sliding window size | `src/stream_mining/sliding_window.py` + `WINDOW_SIZE` in the two pipeline scripts |
| Flajolet–Martin hashes / estimator | `src/stream_mining/flajolet_martin.py` |
| Historical event generation | `src/stream_mining/tennis_match_stream.py` → `create_match_events()` |
| Live feed polling interval / URL | `src/stream_mining/live_tennis_stream.py` (`API_URL`, `POLL_INTERVAL`) |
| Live event shape | `src/stream_mining/live_event_adapter.py` → `create_live_events()` |
| Stream writes / indexes | `src/stream_mining/stream_mongodb.py` |
| MongoDB queries or indexes | `src/mongodb/tennis_queries.py` (`TennisQueries`, `INDEX_PLAN`) |
| API routes | `src/api/tennis_dashboard_api.py` → `ROUTES` |
| API response shape for a dashboard card | `src/api/tennis_dashboard_api.py` → the matching `build_*()` function |
| API error handling for a dashboard card | `read_only_endpoint()` (wraps all `/api/dashboard/*` builders) |
| Match pagination limits | `MATCH_LIST_LIMIT`, `MATCH_LIST_MAX_LIMIT`, `MATCH_LIST_COLUMNS` in the API |
| Server host/port | `src/api/tennis_dashboard_api.py` → `main()` |
| Database name | `DATABASE_NAME` in `src/mongodb/tennis_queries.py` and `MongoStreamStore.__init__` |
| Credentials (never commit) | `.env` (git-ignored); names documented in `.env.example` |
| Dependencies | `requirements.txt` |
| Streamlit demo view | `dashboard/stream_analytics.py` (fix the missing import in `dashboard/app.py` first) |
| R analysis | Nothing to edit yet — `r_analysis/*.R` are empty |

---

# Implementation Status

## Currently visible / working (reachable from the website)

| Feature | Entry point | Verified behaviour |
|---|---|---|
| Home data coverage | Home tab | 15,980 matches · 842 players · 477 tournaments · 3 surfaces |
| Dashboard KPIs | Dashboard tab | Matches / Players / Tournaments / Surfaces cards populate from MongoDB |
| Matches by Surface | Dashboard tab | Doughnut chart + legend with counts and percentages |
| Top Players by Wins | Dashboard tab | Top 10 from the `player_wins` collection |
| Recent Matches | Dashboard tab | 15 newest matches, one row per match |
| Live ATP Matches | Dashboard tab / sidebar "Live Matches" | Shows the live status; "No live ATP matches currently available" when idle |
| Players list + selection | Players tab | 842 real players; retry state on API failure |
| Player profile, trajectory, radar, surfaces | Players tab | Career + 2019–2024 trajectory + surface win rates |
| Matches filters, search, pagination | Matches tab | "Showing 100 of 15980", season/surface/tournament/player filters, substring search |
| Match details modal | Matches tab | Full stats for both players |
| Head-to-Head | Head-to-Head tab | Real meetings, win record, per-surface breakdown, empty state when no meetings |

## Implemented but separate (works independently, not wired to the website)

| Component | What it is | How to run it | Why it is not in the UI |
|---|---|---|---|
| MapReduce jobs + loaders | 3 aggregation jobs + HDFS→MongoDB loaders | `hadoop jar … -mapper/-reducer`, then `python src/load_*.py` | Results reach MongoDB; the UI never triggers the jobs |
| Historical stream pipeline | CSV → 31,960 events → MongoDB + JSON | `python -m src.stream_mining.tennis_match_stream` | Feeds `stream_events`; the UI only reads recent rows |
| Live stream pipeline | 15 s ATP polling → live events + upserted metrics | `python -m src.stream_mining.live_tennis_stream` | Feeds the live card's data; the UI never starts it |
| Bloom Filter, sliding window, Flajolet–Martin, exact counter | Stream algorithms | Used inside `StreamProcessor` | Their metrics are in `stream_metrics` but no widget displays them |
| `/api/dashboard/stream`, `/event-types`, `/pipeline`, `/api/health` | Diagnostic endpoints | `curl` | Not called by the frontend |
| Streamlit stream view | Second dashboard for the stream metrics | `streamlit run dashboard/app.py` (broken today, see below) | Separate UI, not linked from the site |
| `src/mongodb/test_tennis_queries.py`, `src/stream_mining/test_*.py` | Tests | Manual / pytest | Not part of the app |

## Incomplete / experimental (files that exist but contain nothing or do not run)

| File | Status | Note |
|---|---|---|
| `src/recommendation/player_similarity.py` | 0 bytes | No recommendation or player-similarity algorithm exists. Nothing imports it. |
| `r_analysis/statistical_analysis.R` | 0 bytes | No R statistical analysis implemented. |
| `r_analysis/visualizations.R` | 0 bytes | No R visualisation implemented. |
| `dashboard/utils.py` | 0 bytes | Empty placeholder. |
| `dashboard/app.py` | 60 B, missing import | Calls `render_stream_analytics()` without importing it → `NameError` at start. One-line fix. |
| `hadoop/README.md`, `mongodb/README.md` | 0 bytes | Empty placeholders kept for folder structure. |
| `data/stream/events.csv` | 0 bytes, git-ignored | Placeholder for ad-hoc event exports; unused. |
| `frontend/package-lock.json` | stub | No npm dependency is used by the frontend. |
| Flajolet–Martin accuracy | experimental | ≈1,900 estimated distinct players vs 842 exact — usable as a demonstration, not as a product metric. |
| Loader scalability | limited | Loaders read only `part-00000`; additional reducer output files would be ignored. |
| Hard-coded HDFS paths | limited | `/user/giselledmello/tennis/...` in all three loaders — must be changed for another user. |

---
---

# Appendix

## API reference (every route in `src/api/tennis_dashboard_api.py`)

| Method | Path | Params | Returns | Used by the frontend |
|---|---|---|---|---|
| GET | `/` | – | `frontend/index.html` | yes (page load) |
| GET | `/api/health` | – | status, database, collection counts | no |
| GET | `/api/dashboard/overview` | – | dataset meta, kpis, kpi_notes, collection_counts, events_by_source | yes |
| GET | `/api/dashboard/surfaces` | – | `matches_by_surface`, `performance`, `stream_events_by_surface` | yes (chart) |
| GET | `/api/dashboard/top-players` | `limit` | `top_players[]` | yes |
| GET | `/api/dashboard/stream` | – | latest historical stream metrics | no |
| GET | `/api/dashboard/event-types` | – | event-type distribution | no |
| GET | `/api/dashboard/recent-events` | `limit` (default 15, max 50) | `events[]` | yes |
| GET | `/api/dashboard/live` | `limit` (default 20) | `has_live_matches`, `message`, metrics, events, pipeline | yes |
| GET | `/api/dashboard/pipeline` | – | pipeline stage status | no |
| GET | `/api/players` | – | `players[]`, `count` | yes |
| GET | `/api/players/{player_name}` | path param | career + `yearly_performance[]` + `surface_performance[]` | yes |
| GET | `/api/matches` | `year`, `surface`, `tournament`, `player`, `search`, `limit`, `offset` | `matches[]`, totals, pagination, `filter_options` | yes |
| GET | `/api/matches/{match_id}` | path param | full match detail | yes |
| GET | `/api/head-to-head` | `player1`, `player2` | totals, wins, `matches[]`, `surface_breakdown[]` | yes |
| GET | `/api/players/`, `/api/matches/` | – | JSON guidance errors | no |

All `/api/dashboard/*` handlers are wrapped in `read_only_endpoint()`, which turns unexpected
exceptions into JSON errors. `read_limit()` clamps limits to `[1, maximum]`.

## Running the pieces

```bash
# website + API (must be run from the project root)
.venv/bin/python -m src.api.tennis_dashboard_api        # http://127.0.0.1:8000

# rebuild the cleaned dataset
.venv/bin/python src/preprocessing/clean_data.py

# analytics demos (each module has a __main__ block)
.venv/bin/python src/analytics/player_analysis.py
.venv/bin/python src/analytics/match_analysis.py

# stream pipelines
.venv/bin/python -m src.stream_mining.tennis_match_stream
.venv/bin/python -m src.stream_mining.live_tennis_stream

# database checks
.venv/bin/python src/verify_mongodb.py
```

## Key environment variables

`MONGODB_URI`, `MONGODB_DATABASE` (informational — the code hard-codes
`DATABASE_NAME = "tennis_analytics"`), `LIVE_TENNIS_API_KEY` (optional, live feed). Values live in
`.env`, which is git-ignored; only names appear in `.env.example`.
