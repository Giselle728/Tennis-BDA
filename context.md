# Tennis Match Analytics & Performance Analysis System

> **How to read this document.** This file is the technical companion to `README.md`.
> `README.md` is the product page; this document explains what is *actually implemented*
> underneath the user interface: the dataset, the Hadoop/MapReduce jobs, the MongoDB
> collections, the streaming algorithms, the API and the head-to-head algorithm.
> Every number and every claim below was read from the code and from the running system.

---

## 1. Project Overview

**What it is.** A tennis analytics platform that turns a historical ATP match dataset into
player statistics, surface statistics, tournament statistics, match-level detail and
head-to-head records, and presents them through a web dashboard.

**The problem it solves.** A single player, a single match or a single tournament is easy to
read, but answering "how did this player perform across six seasons?", "which surface suits
this player?" or "who has won more meetings between these two players, and on which surface?"
requires aggregating thousands of match records. Doing that repeatedly, for many different
questions, is what the distributed-processing layer in this project automates.

**What the website lets a user do**

| Page | What the user does there |
|------|--------------------------|
| Home | Reads what the product covers and the real size of the dataset |
| Dashboard | Total matches, unique players, tournaments, surfaces, matches by surface, top players by wins, recent matches, live ATP status |
| Players | Searches a player, opens the profile, reads career and season-by-season statistics |
| Matches | Filters matches by season, surface, tournament or player and opens any match |
| Head-to-Head | Compares two players using their real recorded meetings |
| Live Matches | Checks whether ATP matches are currently in progress |

**Dataset.** ATP men's match results, six seasons, **15,980 matches** after cleaning, covering
842 players and 477 tournaments. The project covers the **2019–2024 seasons**.

**Architecture in one line.** Raw CSV → Python/Pandas preprocessing → cleaned CSV →
HDFS → Hadoop MapReduce → aggregated results → MongoDB → REST API → HTML/JS dashboard → user.

Two independent paths reach MongoDB: the **batch** path (MapReduce aggregation) and the
**stream** path (match events + live ATP feed). The website reads from MongoDB and from the
cleaned CSV; it never recomputes anything in the browser.

---

## 2. Project Objectives

1. **Analyse historical tennis matches** – clean and consolidate six seasons of ATP results into
   one analysable dataset.
---

## 3. Dataset Used

### Source

The dataset ships with the repository, so no external download is needed to run the project.

| File | Contents | Size |
|------|----------|------|
| `data/raw/atp_matches_2019.csv` … `atp_matches_2024.csv` | ATP match results per season | 300 KB – 635 KB each |
| `data/raw/atp_players.csv` | Player master list | 2.4 MB |
| `data/processed/tennis_matches_clean.csv` | Cleaned, merged dataset with derived fields (15,980 rows × 57 columns) | 5.6 MB |
| `data/processed/real_atp_stream_results.json` | Generated stream output (31,960 events). Git-ignored; re-created by `src/stream_mining/tennis_match_stream.py` | 17 MB |

Raw match rows: 2,806 (2019 file) + 1,462 + 2,733 + 2,917 + 2,986 + 3,076 (2024) = **15,980 rows**.

### Record counts (verified against the cleaned CSV and MongoDB)

* Matches: **15,980** (15,980 distinct `match_id` values)
* Players: **842** (distinct names across `winner_name` and `loser_name`)
* Tournaments: **477** (distinct `tourney_name`)
* Surfaces: Hard 9,537 · Clay 4,772 · Grass 1,618 · 53 rows with a missing surface (kept in the
  dataset, ignored by surface analytics – which is why the surface chart shows 9,537/4,772/1,618)

### Important columns

**Players** – `winner_id`, `winner_name`, `winner_seed`, `winner_hand`, `winner_ht`, `winner_ioc`,
`winner_age`, `winner_rank`, `winner_rank_points` and the matching `loser_*` columns.

**Tournament** – `tourney_id`, `tourney_name`, `surface`, `draw_size`, `tourney_level`,
`tourney_date`.

**Match** – `match_num`, `round`, `best_of`, `score`, `minutes`.

**Serve statistics** – `w_ace`, `w_df`, `w_svpt`, `w_1stIn`, `w_1stWon`, `w_2ndWon`, `w_bpSaved`,
`w_bpFaced` and the matching `l_*` columns.

**Derived (added by preprocessing)** – `match_id`, `year`, `w_1st_serve_pct`, `l_1st_serve_pct`,
`w_1st_serve_won_pct`, `l_1st_serve_won_pct`, `w_bp_conversion_pct`, `l_bp_conversion_pct`.

### Season normalisation – read this carefully

The `year` column produced by preprocessing comes from the **tournament start date**
(`tourney_date`). **85 rows have the raw date `2018-12-31`**: the season-opening events that begin
the 2019 season (Brisbane, Doha, Pune). Following the start date they look like "2018", but they
are 2019-season matches.

The project therefore **does not treat 2018 as a season**. A single helper,
`season_years()` in `src/analytics/player_analysis.py`, derives the real season from the
tournament id first (`2019-M020` → 2019) and from the `match_id` second (`2019-M020_271` → 2019),
falling back to the dataset's own `year` column only if neither id can be parsed. No year is
hardcoded and no row is invented or deleted; the 85 rows simply belong to the 2019 season.

This is why the Matches page shows these season totals:

| Season | Matches |
|--------|---------|
| 2019 | 2,806 (2,721 + the 85 season-opening rows) |
| 2020 | 1,462 |
| 2021 | 2,733 |
| 2022 | 2,917 |
| 2023 | 2,986 |
| 2024 | 3,076 |
| **Total** | **15,980** |

`season_years()` is imported and reused by `src/analytics/match_analysis.py` and
`src/analytics/head_to_head.py`, so the Players page, the Matches page and the Head-to-Head page
can never disagree about which season a match belongs to.

---

## 4. Data Preprocessing

Implemented in `src/preprocessing/clean_data.py`, run from the project root:

```bash
.venv/bin/python src/preprocessing/clean_data.py
```

| Step | What the code does |
|------|--------------------|
| 1. Load | Globs `data/raw/atp_matches_*.csv` and concatenates them into one DataFrame |
| 2. Duplicates | `df.drop_duplicates()` – removes exact duplicate rows |
| 3. Dates | `pd.to_datetime(tourney_date, format="%Y%m%d", errors="coerce")` – invalid dates become missing instead of crashing |
| 4. Text | Strips whitespace from 12 text columns and title-cases `surface` |
| 5. Surfaces | Any value that is not `Hard`, `Clay` or `Grass` becomes missing (rows are **kept**) |
| 6. Numeric | 30+ numeric columns converted with `pd.to_numeric(..., errors="coerce")` |
| 7. `match_id` | `tourney_id + "_" + match_num`, e.g. `2024-7696_400`. Unique per match (15,980 unique values) |
| 8. `year` | `tourney_date.dt.year` (start-date year – see season normalisation above) |
| 9. Derived percentages | first serve %, first-serve points won %, break-point conversion %, for winner and loser |
| 10. Validation | Any derived percentage outside 0–100 becomes missing (impossible values removed, rows kept) |
| 11. Sort | Sorted by `tourney_date`, `tourney_name`, `match_num` |
| 12. Output | `data/processed/tennis_matches_clean.csv` |

Formulas used for the derived percentage columns:

```
w_1st_serve_pct        = w_1stIn / w_svpt * 100
w_1st_serve_won_pct    = w_1stWon / w_1stIn * 100
w_bp_conversion_pct   = (w_bpFaced - w_bpSaved) / w_bpFaced * 100
```

`src/preprocessing/data_profile.py` is a small helper used to inspect the raw dataset shape and
column types before cleaning.

---

## 5. Overall System Architecture

### Batch path (MapReduce aggregation)

```
Raw Tennis Dataset                  data/raw/atp_matches_2019..2024.csv, atp_players.csv
        ↓
Data Preprocessing                  src/preprocessing/clean_data.py (Pandas)
        ↓
Cleaned Match Dataset               data/processed/tennis_matches_clean.csv (15,980 rows)
        ↓
HDFS                                hdfs dfs -put ... /user/giselledmello/tennis/
        ↓
Hadoop MapReduce                    src/mapreduce/*_mapper.py + *_reducer.py (3 jobs)
        ↓
Aggregated Analytics                /user/giselledmello/tennis/output/<job>/part-00000
        ↓
MongoDB Atlas                       src/load_player_wins.py, load_surface_analysis.py,
                                     load_tournament_analysis.py (hdfs dfs -cat → bulk upsert)
        ↓
FastAPI-style REST Backend          src/api/tennis_dashboard_api.py (Starlette + Uvicorn)
        ↓
Frontend Dashboard                  frontend/index.html (HTML/CSS/JS + Chart.js)
        ↓
User
```

### Stream path (match events + live feed)

```
data/processed/tennis_matches_clean.csv
        ↓
src/stream_mining/tennis_match_stream.py
        (one MATCH_WIN and one MATCH_LOSS event per match → 31,960 events)
        ↓
src/stream_mining/stream_processor.py
        (Bloom Filter · exact DistinctCounter · Flajolet–Martin · SlidingWindow)
        ↓
src/stream_mining/stream_mongodb.py → MongoDB collections stream_events + stream_metrics
        ↓
src/api/tennis_dashboard_api.py → GET /api/dashboard/live, /recent-events, /stream

Live feed (parallel):
https://api.livetennisapi.com → src/stream_mining/live_tennis_stream.py (15 s polling, tour=atp)
        → live_event_adapter.py (normalises feed rows into the same event shape)
        → StreamProcessor → MongoDB (source="live", metrics upserted as live_stream_current)
        → GET /api/dashboard/live
```

### Which page reads which source

| Page / endpoint | Reads from |
|-----------------|-----------|
| Dashboard KPIs, surface chart, top players | MongoDB (`/api/dashboard/overview`, `/surfaces`, `/top-players`) |
| Dashboard recent matches | MongoDB `stream_events` via `/api/dashboard/recent-events` |
| Dashboard live card | MongoDB live metrics + `/api/dashboard/live` |
| Players + player profile | Cleaned CSV via `src/analytics/player_analysis.py` |
| Matches + match details | Cleaned CSV via `src/analytics/match_analysis.py` |
| Head-to-Head | Cleaned CSV via `src/analytics/head_to_head.py` |

---

## 6. Technologies Used

| Technology | Why it is used in this project |
|------------|---------------------------------|
| **Python 3.10** | Language of the whole analytics layer (preprocessing, mappers/reducers, loaders, stream processing, API). |
| **Pandas** | Cleaning and deriving 15,980 match rows, plus every player/match/head-to-head query. |
| **NumPy** | Numeric helpers inside the API layer. |
| **Hadoop HDFS** | Distributed storage of the cleaned dataset and of the MapReduce output (`/user/giselledmello/tennis/...`), so several machines can read the same data. |
| **Hadoop MapReduce** | Distributed aggregation of player wins, surface statistics and tournament statistics. Implemented as Hadoop-streaming jobs that call the Python mapper/reducer scripts in `src/mapreduce/`. |
| **MongoDB (Atlas)** | Document store for the aggregated results and stream events; indexed for the read patterns of the API. |
| **PyMongo** | Driver used by the loaders, the stream store and the read-only query layer. |
| **Starlette + Uvicorn** | The REST API and the static frontend are served by one ASGI application, so the page and the API share an origin and no CORS configuration is needed. (The project imports `starlette.routing.Route` directly; `fastapi` is installed in the environment but is not imported by the application.) |
| **python-dotenv** | Loads `MONGODB_URI` / `LIVE_TENNIS_API_KEY` from `.env` so no credential is hardcoded. |
| **certifi** | Supplies the CA bundle that lets PyMongo verify the MongoDB TLS certificate. |
| **requests** | HTTP client for the live ATP match feed. |
| **HTML / CSS / JavaScript** | The frontend is a single self-contained page (`frontend/index.html`) with Tailwind-style utility classes, Font Awesome icons and Chart.js. |
| **Chart.js** | Draws the "Matches by Surface" doughnut chart. |
| **Streamlit (optional)** | `dashboard/stream_analytics.py` renders the stream-processing results from the generated JSON in a separate notebook-style view. It is optional and not part of the website. |
| **Hadoop Streaming (Python)** | The MapReduce mappers/reducers are plain Python scripts that read stdin and write stdout, run by `hadoop jar ... hadoop-streaming.jar`. |
| **JavaScript `Intl` number formatting** | Formats large numbers (`15,980`) consistently in the UI. |

### Verified library versions

Python 3.10.1 in the project virtual environment, with `starlette 1.7.0`, `uvicorn 0.53.0`,
`pandas 2.3.3`, `numpy 2.2.6`, `pymongo 4.18.1`, `certifi 2026.7.22`, `python-dotenv 1.2.3`,
`requests 2.34.2`, `streamlit 1.64.0`. See `requirements.txt`.
Formulas:

---

## 7. MapReduce Algorithms

Three jobs are implemented. Each is a pair of Python scripts in `src/mapreduce/` that are executed
as Hadoop-streaming mappers and reducers, and each reads rows from the cleaned CSV on HDFS.

Standard invocation (paths match the ones hardcoded in the loaders):

```bash
# 1. upload the cleaned dataset
hdfs dfs -mkdir -p /user/giselledmello/tennis/data
hdfs dfs -put -f data/processed/tennis_matches_clean.csv /user/giselledmello/tennis/data/

# 2. run a job
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input  /user/giselledmello/tennis/data/tennis_matches_clean.csv \
  -mapper "python3 src/mapreduce/player_wins_mapper.py" \
  -reducer "python3 src/mapreduce/player_wins_reducer.py" \
  -output /user/giselledmello/tennis/output/player_wins

# 3. load the result into MongoDB
.venv/bin/python src/load_player_wins.py
```

Hadoop performs the shuffle and sort: it groups all mapper output lines with the same key
(the player / surface / tournament name) and sends each group to one reducer call, sorted by key.
That is why every reducer in this project keeps a `current_key` and only flushes when the key
changes.

### A. Player Wins

Files: `src/mapreduce/player_wins_mapper.py`, `src/mapreduce/player_wins_reducer.py`
Collection: `player_wins` (391 documents)

* **Input** – one CSV row per match from the cleaned dataset on HDFS.
* **Mapper** – reads the header with `csv.DictReader`, takes `winner_name` and emits
  `winner_name \t 1`. Rows without a winner are skipped. The mapper performs **no** counting logic;
  it only emits the key and a constant value, which is the standard pattern that keeps the mapper
  cheap and lets Hadoop do the grouping.
* **Shuffle / sort** – Hadoop partitions by key and sorts by key, so all lines for one player are
  consecutive.
* **Reducer** – keeps `current_player` and `current_wins`; for every line it adds the count, and
  when the player changes it prints `player \t total` and starts a new accumulator. The final
  player is flushed after the loop.
* **Output** – `player_name \t win_count`, e.g. `Medvedev, Daniil\t166`.
* **Purpose** – powers "Top Players by Wins" on the Dashboard.
* **Loaded by** – `src/load_player_wins.py`, which runs `hdfs dfs -cat
  /user/giselledmello/tennis/output/player_wins/part-00000` and `bulk_write`s `UpdateOne`
  upserts keyed on `player_name`.

Example document:

```json
{ "player_name": "Daniil Medvedev", "win_count": 166 }
```

### B. Surface Analysis

Files: `src/mapreduce/surface_analysis_mapper.py`, `src/mapreduce/surface_analysis_reducer.py`
Collection: `surface_analysis` (3 documents: Clay, Grass, Hard)

* **Input** – the cleaned CSV.
* **Mapper** – parses each line with `csv.reader`, skips the header and any malformed row
  (length mismatch), reads `surface`, and returns `None` for rows with no surface. For every match
  it computes the **average of the winner's and the loser's value** for aces, double faults,
  first-serve %, break-point conversion %, plus the duration in minutes, and emits
  `surface \t 1 \t avg_aces \t avg_df \t avg_first_serve \t avg_bp \t minutes`.
  A missing statistic is emitted as `-1` so the reducer can tell "missing" from "zero".
* **Shuffle / sort** – all rows of one surface go to one reducer, ordered by surface.
* **Reducer** – accumulates `match_count` plus a running sum and a valid-sample count for each
  statistic, ignoring `-1` sentinels, and emits
  `surface \t match_count \t avg_aces \t avg_df \t avg_first_serve \t avg_bp \t avg_duration`
  rounded to two decimals.
* **Output** – one line per surface.
* **Purpose** – the "Matches by Surface" chart and the surface statistics shown in the project.
* **Loaded by** – `src/load_surface_analysis.py` (same HDFS-read + upsert pattern).

Example document:

```json
{ "surface": "Clay", "match_count": 4772, "avg_aces": 4.08,
  "avg_double_faults": 2.71, "avg_first_serve_pct": 62.8,
  "avg_break_point_conversion_pct": 43.15, "avg_duration_minutes": 115.38 }
```

### C. Tournament Analysis

Files: `src/mapreduce/tournament_mapper.py`, `src/mapreduce/tournament_reducer.py`
Collection: `tournament_analysis` (477 documents)

* **Input** – the cleaned CSV.
* **CSV parsing approach** – this job uses `csv.reader` and **fixed column positions** taken from
  the header layout (column 1 = `tourney_name`, column 26 = `minutes`), and it guards the parse
  with `if len(row) <= 26: continue` plus a header check on `row[0]`. This is the fastest approach
  for a fixed 57-column schema, at the cost of being tied to that column order.
* **Mapper** – emits `tournament_name \t 1 \t duration`, where an unparsable duration becomes `-1`.
* **Shuffle / sort** – grouped by tournament name.
* **Reducer** – counts matches and averages only the durations that are not `-1`, then prints
  `tournament \t match_count \t avg_duration`.
* **Output** – one line per tournament.
* **Purpose** – tournament-level match activity and duration statistics.
* **Loaded by** – `src/load_tournament_analysis.py`.

Example document:

```json
{ "tournament": "ATP Rio de Janeiro", "match_count": 31, "avg_duration_minutes": 135.06 }
```

---

## 8. Streaming / Stream Processing

This part **is implemented** – `src/stream_mining/` – and it runs today, but the website only
exposes the user-relevant outcome of it (recent matches and live match status). The internal
metrics stay documented here.

### What a stream event is

A stream event is **one player's result in one match**, not the match itself. Each match produces
exactly two events:

| Event | Meaning | Key fields |
|-------|---------|-----------|
| `MATCH_WIN` | the winner's result in that match | `event_id = <match_id>_winner`, `player`, `opponent`, `role="winner"`, ranking, ranking points, aces, double faults, first-serve %, score, round, surface, tournament, minutes |
| `MATCH_LOSS` | the loser's result in that match | `event_id = <match_id>_loser`, `player`, `opponent`, `role="loser"`, same match fields |

15,980 matches × 2 = **31,960 events**. `event_id` is deterministic (`<match_id>_<role>`), which is
what makes duplicate detection possible.

### Historical stream

`src/stream_mining/tennis_match_stream.py` reads `tennis_matches_clean.csv` in file order and
feeds one event at a time into `StreamProcessor`, writing all events and the final metric snapshot
to `data/processed/real_atp_stream_results.json` and to MongoDB (`source="historical"`).

### Live stream

`src/stream_mining/live_tennis_stream.py` polls
`https://api.livetennisapi.com/api/public/v1/matches` every **15 seconds**, filters the response
to ATP matches locally, converts rows with `live_event_adapter.create_live_events()` into the same
event shape, and runs them through the same `StreamProcessor`. Live metrics are **upserted** under
the key `live_stream_current` instead of appending a new document every cycle.

### StreamProcessor

`src/stream_mining/stream_processor.py` combines the four algorithms below plus a `Counter` for
event-type frequency. Defaults: `window_size=100`, `bloom_capacity=100_000`,
`bloom_error_rate=0.01`, `fm_hashes=32`.

| Metric field | Meaning |
|--------------|---------|
| `total_events` | events processed (31,960) |
| `duplicate_events` / `duplicate_rate` | events the Bloom Filter rejected as already seen |
| `invalid_events` | events that failed validation and were skipped |
| `exact_distinct_players` | **exact** distinct count from `DistinctCounter` (a Python `set`) – 842 |
| `estimated_distinct_players` | **approximate** count from Flajolet–Martin |
| `exact_distinct_matches` | **exact** distinct `match_id` count – 15,980 |
| `active_window_size` / `configured_window_size` | events currently held in the sliding window (100) |
| `event_type_counts` | `{"MATCH_WIN": 15980, "MATCH_LOSS": 15980}` |
| `top_players_by_events`, `top_matches_by_events` | most active players / matches |
| `first_timestamp`, `last_timestamp` | event time range |

### Duplicate handling

Every incoming event is hashed into the Bloom Filter **before** it is counted. If the filter says
"probably seen", the event increments `duplicate_events` and is skipped; otherwise it is inserted
and processed. This keeps 31,960 events idempotent across repeated runs.

### Bloom Filter — `src/stream_mining/bloom_filter.py`

**Purpose.** Memory-efficient membership testing: decide whether an event id has been seen before,
using a fixed-size bit array instead of storing every id.

**How it works.**

1. A bit array of size `m` is created, all bits zero.
2. `k` hash functions are derived from SHA-256 digests (`hashlib`), so the implementation uses a
   real cryptographic hash rather than `hash()`, whose value is randomised per process.
3. **Insertion** – hash the item `k` times and set each resulting bit to 1.
4. **Membership test** – hash again; if **all** `k` bits are 1 the item is reported as *probably
   present*, otherwise it is definitely absent.
5. `m` and `k` are computed from the requested capacity and error rate:
   `m = -n·ln(p) / (ln2)²`, `k = (m/n)·ln2`.

**False positives.** Yes – a Bloom Filter **can** report an item as present when it is not, and it
can never report a present item as absent (assuming no bit-array overflow). A false positive means
a genuinely new event could be counted as a duplicate, so a slightly *higher* duplicate count is
possible. The filter stores only bits, never the original items.

**Why it is useful in streaming.** Keeping every event id in a set costs memory proportional to the
number of events. The filter costs a fixed amount of memory chosen in advance, which is the right
trade-off when the question is only "have I seen this id before?".

### Sliding Window — `src/stream_mining/sliding_window.py`

**Purpose.** Keep *recent* context only, so "what happened lately" can be answered without storing
the whole stream.

**How it works.** A `collections.deque` with `maxlen=100`. Adding an event when the deque is full
**automatically evicts the oldest** event, so the window always holds the newest 100 events.
`active_window_size` reports how many are currently held.

**Why it is useful here.** It gives the metrics document a "current activity" figure, and it
demonstrates that a stream consumer can bound its memory while still answering recent questions.
It is *not* used to compute career totals – those are exact counts over the whole stream.

### Flajolet–Martin — `src/stream_mining/flajolet_martin.py`

**Purpose.** Estimate the number of distinct values in a stream using **constant memory**.

**How it works.**

1. Each value is hashed `num_hashes` times (default 32) and each hash is converted to an integer.
2. For each hash, the number of **trailing zero bits** is counted. The intuition: a random 64-bit
   number has a 1/2 chance of ending in `0` zeros, 1/4 of ending in `00`, and so on, so longer runs
   of trailing zeros are rarer.
3. With `n` distinct values and `k` hashes, the expected maximum trailing-zero count grows roughly
   like `log2(n)`. **Inverting that relationship gives the estimate**: `estimate ≈ 2^max_trailing_zeros`.
4. To make the estimate stable, the implementation groups the hashes, averages the group estimates
   and takes the **median** of those averages instead of relying on a single hash.

**Why approximate counting saves memory.** A set of every distinct player id grows without bound
(`O(n)`), while Flajolet–Martin keeps a handful of integers per hash and never grows with `n`.

**Exact vs estimated – important distinction.** The metrics document stores
`exact_distinct_players` (a real `set`, therefore exact) **and** `estimated_distinct_players`
(Flajolet–Martin, therefore approximate). The two are reported separately and must never be
presented as the same kind of number. On the current dataset the estimator is far off
(≈1,900 estimated vs 842 exact) because the stream is short relative to the hash range – a useful,
honest demonstration of *when* approximate counting should not be trusted.

### Exact distinct counting — `src/stream_mining/distinct_count.py`

`DistinctCounter` wraps a Python `set` and returns whether an item was new. It is deliberately kept
next to the approximate counter so the two can be compared directly.

---

## 9. MongoDB Database

**Database:** `tennis_analytics` (the constant `DATABASE_NAME` in `src/mongodb/tennis_queries.py`).

Verified collection contents:

| Collection | Documents | Purpose |
|------------|-----------|---------|
| `player_wins` | 391 | MapReduce result: win count per player |
| `surface_analysis` | 3 | MapReduce result: per-surface aggregates |
| `tournament_analysis` | 477 | MapReduce result: per-tournament aggregates |
| `stream_events` | 31,960 | one document per processed stream event |
| `stream_metrics` | 3 | metric snapshots (`historical`, `live` → `live_stream_current`) |

### `player_wins`

* **Fields** – `player_name`, `win_count`.
* **Populated by** – `src/load_player_wins.py` (HDFS output → `bulk_write` of `UpdateOne` upserts
  on `player_name`).
* **Read by** – `TennisQueries.get_top_players_by_wins(limit)`; served at `/api/dashboard/top-players`.
* **Indexed** – `win_count` descending.

### `surface_analysis`

* **Fields** – `surface`, `match_count`, `avg_aces`, `avg_double_faults`, `avg_first_serve_pct`,
  `avg_break_point_conversion_pct`, `avg_duration_minutes`.
* **Populated by** – `src/load_surface_analysis.py`.
* **Read by** – `get_matches_by_surface()` and `get_surface_performance()`; served at
  `/api/dashboard/surfaces`.
* **Indexed** – none required (3 documents, always read in full).

### `tournament_analysis`

* **Fields** – `tournament`, `match_count`, `avg_duration_minutes`.
* **Populated by** – `src/load_tournament_analysis.py`.
* **Read by** – `get_tournament_analysis(limit)`; also used for the Matches page filter list.
* **Indexed** – `match_count` descending.

### `stream_events`

* **Fields** – `event_id`, `event_type`, `player`, `player_id`, `opponent`, `role`, `match_id`,
  `tournament`, `tournament_id`, `surface`, `round`, `score`, `minutes`, `ranking`,
  `ranking_points`, `aces`, `double_faults`, `first_serve_pct`, `year`, `timestamp`, `source`,
  `sequence`, `stale`, `stored_at`, `event_data` (the full source payload).
* **Populated by** – `src/stream_mining/stream_mongodb.py` (historical and live pipelines).
* **Read by** – `get_recent_stream_events(limit)`, `get_top_players_by_stream_events()`,
  `get_event_type_distribution()`, `get_current_live_events()`.
* **Indexed** – `source`, `surface`, `tournament`, and the compound `source + timestamp`
  (descending) used by the "recent" query.

### `stream_metrics`

* **Fields** – all the metrics listed in section 8, plus `source`, `timestamp` and `metric_key`.
* **Populated by** – `MongoStreamStore`, which upserts the live snapshot under
  `metric_key="live_stream_current"` and inserts/updates the historical snapshot.
* **Read by** – `get_latest_stream_metrics()`, `get_current_live_metrics()`;
  served at `/api/dashboard/stream` and `/api/dashboard/live`.
* **Indexed** – `source`, `metric_key`.

### How the API reads MongoDB

`src/mongodb/tennis_queries.py` is a **read-only** query layer. `TennisQueries` opens the client
with the CA bundle from `certifi`, and `ensure_analysis_indexes()` creates the indexes above on
first use. The API never writes to MongoDB: loaders and stream pipelines are the only writers.

---

## 10. FastAPI Backend

Entry point: `src/api/tennis_dashboard_api.py` (Starlette ASGI app, served by Uvicorn).
It also serves `frontend/index.html`, so page and API share one origin.

```bash
.venv/bin/python -m src.api.tennis_dashboard_api     # http://127.0.0.1:8000
```

| Method | Endpoint | Parameters | Response | Purpose | Data source |
|--------|----------|-----------|----------|---------|-------------|
| GET | `/` | – | `frontend/index.html` | Serves the single-page frontend | static file |
| GET | `/api/health` | – | `status`, `database`, per-collection counts | Liveness + collection check | MongoDB |
| GET | `/api/dashboard/overview` | – | `dataset` (first/last timestamp, last processed), `kpis` (total_matches, unique_players, stream_events, surfaces), `kpi_notes`, `collection_counts`, `events_by_source` | Dashboard KPI cards, landing "data coverage", dataset meta line | MongoDB |
| GET | `/api/dashboard/surfaces` | – | per-surface `match_count` + percentages | "Matches by Surface" chart | MongoDB `surface_analysis` |
| GET | `/api/dashboard/top-players` | `limit` (default 10) | ranked `player_name`, `win_count` | "Top Players by Wins" | MongoDB `player_wins` |
| GET | `/api/dashboard/stream` | – | latest historical stream metrics snapshot | Stream metrics (kept for the technical/debug view) | MongoDB `stream_metrics` |
| GET | `/api/dashboard/event-types` | – | `MATCH_WIN` / `MATCH_LOSS` counts | Event-type distribution | MongoDB `stream_events` |
| GET | `/api/dashboard/recent-events` | `limit` (default 15) | newest events with player, opponent, tournament, surface, score, timestamp | Dashboard "Recent Matches" table | MongoDB `stream_events` |
| GET | `/api/dashboard/live` | – | `has_live_matches`, `message`, live `metrics`, `events`, `pipeline` | "Live ATP Matches" card | MongoDB live metrics + events |
| GET | `/api/dashboard/pipeline` | – | step-by-step status of each pipeline stage | Diagnostic view of the whole pipeline | all collections |
| GET | `/api/players` | – | `players[]` with `player_name`, `matches`, `wins`, `losses`, `win_rate` | Populates the player selectors (842 players) | cleaned CSV via `player_analysis` |
| GET | `/api/players/{player_name}` | path: URL-encoded player name | career summary + `yearly_performance[]` (2019–2024) + serve/break-point averages | Player profile page | cleaned CSV via `player_analysis` |
| GET | `/api/matches` | `year`, `surface`, `tournament`, `player`, `search`, `limit`, `offset` | `matches[]`, `total_count`, pagination info, `filter_options` (years, surfaces, tournaments, players) | Matches table with filters + totals | cleaned CSV via `match_analysis` |
| GET | `/api/matches/{match_id}` | path: match id | full match detail (winner/loser, score, duration, aces, double faults, serve %, bp conversion) | Match details modal | cleaned CSV via `match_analysis` |
| GET | `/api/head-to-head` | `player1`, `player2` (required) | totals, win record, win rates, newest-first `matches[]`, `surface_breakdown[]` | Head-to-Head page | cleaned CSV via `head_to_head` |

### Error handling

* Missing or blank `player1` / `player2` → **400**
* Same player selected twice (compared case-insensitively) → **400**
* Unknown player name → **404** with a hint pointing at `/api/players`
* Players exist but have no recorded meetings → **200** with an empty state and a note
* Cleaned dataset missing (API started from the wrong directory) → **503**
* Any other backend failure → **500** with an `error_type`, never a fabricated result

All `/api/dashboard/*` endpoints are wrapped in `read_only_endpoint(...)`, which turns unexpected
exceptions into a JSON error instead of an HTML error page.

---

## 11. Head-to-Head Algorithm

Implemented in `src/analytics/head_to_head.py` and exposed by
`GET /api/head-to-head?player1=…&player2=…`.

**The frontend never generates head-to-head values.** The page only picks two names, sends them to
the backend and renders the JSON it gets back. There is no simulated data and no client-side maths.

Exact flow:

1. The user picks **Player 1** and **Player 2** from the dropdowns, which are populated from
   `GET /api/players` (all 842 real players).
2. The frontend calls `GET /api/head-to-head?player1=<A>&player2=<B>`. If both names are equal the
   request is blocked client-side *and* rejected server-side with 400.
3. The API validates the parameters and checks that both players exist in the dataset
   (`player_list_rows()`); unknown names return 404.
4. `get_head_to_head(a, b)` loads `data/processed/tennis_matches_clean.csv` and reassigns the
   season with the shared `season_years()` helper (2019–2024, see section 3).
5. **Only matches containing both players are selected**, with either orientation:
   `(winner=A and loser=B) OR (winner=B and loser=A)`. Each such row is one meeting, so reversing
   the two names returns the same meetings with the win counts mirrored.
6. Winner and loser are identified per row (`winner_name`, `loser_name`) – no inference from score.
7. Total meetings are counted, then wins for each player are counted
   (`player_a_won = winner_name == A`), and the win rates follow from those counts.
8. Matches are sorted by date, newest first, for the "Recent Meetings" list.
9. Surface-specific statistics are computed with a `groupby("surface")` aggregation that counts
   meetings and A's wins on each surface.
10. The API reshapes the result into JSON: totals, win record, win rates, `matches[]`,
    `surface_breakdown[]`, dataset metadata and a generation timestamp.
11. The frontend renders the result: hero card with both names and win rates, a share bar, the
    meetings table and the surface table.

If the two players never met, the endpoint returns **200** with `total_meetings: 0` and an empty
match list, and the page shows "No recorded meetings" instead of inventing numbers.

Worked example (verified against the API):

```
GET /api/head-to-head?player1=Novak Djokovic&player2=Daniil Medvedev
→ 13 meetings, Djokovic 8 – 5 Medvedev
GET /api/head-to-head?player1=Daniil Medvedev&player2=Novak Djokovic
→ 13 meetings, Medvedev 5 – 8 Djokovic   (same meetings, mirrored wins)
```

---

## 12. Frontend Modules

The frontend is a single self-contained page, `frontend/index.html`: HTML + Tailwind-utility CSS +
vanilla JavaScript + Chart.js + Font Awesome, served by the same ASGI app as the API.

### Home (landing)

Product hero, six feature cards describing what the platform does, and a live "Data Coverage" strip
(matches, players, tournaments, surfaces, date range) read from `/api/dashboard/overview` – so the
numbers on the landing page are the real ones, not hardcoded.

### Dashboard

* KPI cards – Total Matches, Unique Players, Tournaments, Surfaces
* **Matches by Surface** – Chart.js doughnut chart from `/api/dashboard/surfaces`, with counts and
  percentages in the legend
* **Top Players by Wins** – ranked list from `/api/dashboard/top-players`
* **Recent Matches** – the 15 newest matches (winner, opponent, tournament, surface, score, date)
  from `/api/dashboard/recent-events`; each match appears once because the table renders the
  `MATCH_WIN` row of each match pair
* **Live ATP Matches** – current live-match status from `/api/dashboard/live`
* A refresh button and a "last refreshed" timestamp; a dataset meta line shows matches, players and
  date range

### Players

Searchable selector populated from `/api/players` (842 players). On selection the page requests
`/api/players/{name}` and shows:

* Career totals – matches, wins, losses, win rate
* Serve and pressure averages – aces, double faults, first-serve %, first-serve points won %,
  break-point conversion, average match duration, latest ranking
* **Performance Trajectory** – a per-season bar/line chart built from `yearly_performance`, i.e.
  matches, wins and win rate for 2019–2024. The seasons are dynamic (driven by the data), so the
  chart always matches the dataset.

### Matches

Filter controls for **season**, **surface**, **tournament** and **player name search**, a
"Showing X of Y matches" counter, a paginated table (100 rows per page) and a totals line per
season. Clicking a row opens the match details.

### Match Details

Modal with tournament, season, surface, round, winner, loser, score, duration, and the serve /
break-point numbers of both players for that match.

### Head-to-Head

Two player selectors, a VS header, and on submit a request to `/api/head-to-head`. The result shows
each player's wins and win rate, total meetings, a win-share bar, the meetings list (tournament,
season, surface, round, winner, score) newest first, and a per-surface breakdown. Selecting the same
player twice shows an inline message and does not call the API.

### Live Matches

Reads the live snapshot through `/api/dashboard/live`. When no ATP match is in progress it shows
"No live ATP matches currently available", which is the normal, expected state at most times. When
matches are live it shows their count and the last check time.

---

## 13. Algorithms Used

| Algorithm | Purpose | Where Used |
|-----------|---------|------------|
| MapReduce (map → shuffle/sort → reduce) | Distributed aggregation | `src/mapreduce/*` → player wins, surface statistics, tournament statistics |
| Bloom Filter | Approximate membership / duplicate detection | `src/stream_mining/bloom_filter.py`, used by `StreamProcessor` |
| Sliding Window | Recent-event analysis with bounded memory | `src/stream_mining/sliding_window.py`, used by `StreamProcessor` |
| Flajolet–Martin | Approximate distinct counting | `src/stream_mining/flajolet_martin.py`, used by `StreamProcessor` |
| Exact distinct counting (hash set) | Exact cardinality for comparison/verification | `src/stream_mining/distinct_count.py` |
| H2H filtering + counting | Two-player comparison | `src/analytics/head_to_head.py` |
| Season normalization | Correct season assignment (2019–2024) | `season_years()` in `src/analytics/player_analysis.py` |
| Pagination / offset filtering | Serve the 15,980-row match table | `src/analytics/match_analysis.py`, `/api/matches` |

Not implemented (placeholder files that exist in the repository but contain **no code**, so nothing
in this project depends on them): `src/recommendation/player_similarity.py` (cosine similarity),
`r_analysis/statistical_analysis.R` and `r_analysis/visualizations.R`, `hadoop/README.md`,
`mongodb/README.md`.

---

## 14. Important BDA Concepts Demonstrated

| Concept | How this project uses it |
|---------|---------------------------|
| **Distributed storage (HDFS)** | The cleaned 15,980-row dataset and every MapReduce output live in HDFS under `/user/giselledmello/tennis/`. Any node in the cluster can read them, and the loaders pull results with `hdfs dfs -cat`. |
| **Distributed processing (MapReduce)** | Three aggregation jobs run across the cluster. The mappers emit small key-value records, Hadoop groups and sorts them by key, and the reducers aggregate one key group at a time. This is what produces `player_wins`, `surface_analysis` and `tournament_analysis`. |
| **NoSQL (MongoDB)** | Analytics results are stored as documents (`{player_name, win_count}`), which matches the shape the API returns, needs no joins or schema migration when a new field is added, and scales by sharding. |
| **ETL / data preprocessing** | Six raw CSVs are merged, de-duplicated, type-normalised, enriched with `match_id`, percentages and season labels, and validated in `clean_data.py`. |
| **Streaming data** | Match results are modelled as an event stream (31,960 events) and a live ATP feed is consumed every 15 seconds. Events are processed sequentially, one at a time, without loading the whole history. |
| **Windowing** | A 100-event sliding window bounds memory and describes recent activity. |
| **Approximate algorithms** | Bloom Filter and Flajolet–Martin trade exactness for constant memory, and the code keeps exact counts side by side so the error is measurable. |
| **Aggregation** | Sum/count/mean aggregations happen once in the reducer and are stored, so the API only reads small pre-aggregated collections. |
| **Analytics API** | A read-only REST layer exposes every result to the frontend; the API never writes to the database and never invents numbers. |
| **Visualization** | Chart.js doughnut for surfaces, custom DOM/canvas charts for the player trajectory and the head-to-head share bar, with data formatted via `Intl.NumberFormat`. |

---

## 15. End-to-End Examples

### Example A – Head-to-Head (user request)

```
User opens the Head-to-Head page
        ↓
Frontend loads /api/players and fills both selectors (842 players)
        ↓
User picks "Novak Djokovic" and "Daniil Medvedev"
        ↓
Frontend calls GET /api/head-to-head?player1=…&player2=…
        ↓
API validates the parameters and that both players exist
        ↓
get_head_to_head() loads the cleaned CSV and filters rows where both players appear
        ↓
Backend counts meetings and wins, sorts newest first, groups by surface
        ↓
JSON returned: totals, win record, matches[], surface_breakdown[]
        ↓
Frontend renders the comparison — no client-side calculation
```

### Example B – MapReduce (batch aggregation)

```
15,980 cleaned matches uploaded to HDFS
        ↓
Mapper (player_wins_mapper.py): "Medvedev, Daniil\t1" per row, one line per match
        ↓
Hadoop shuffles + sorts by player name
        ↓
Reducer (player_wins_reducer.py): sums the counts for each player
        ↓
Output: /user/giselledmello/tennis/output/player_wins/part-00000
        ↓
src/load_player_wins.py: hdfs dfs -cat … → bulk upsert into MongoDB player_wins (391 docs)
        ↓
GET /api/dashboard/top-players?limit=10 → "Top Players by Wins" on the Dashboard
```

### Example C – Stream processing (event pipeline)

```
tennis_matches_clean.csv read row by row
        ↓
Each match converted into MATCH_WIN + MATCH_LOSS events (31,960 total)
        ↓
Bloom Filter rejects duplicates → exact DistinctCounter updates → Flajolet–Martin updates
        ↓
SlidingWindow keeps the newest 100 events; Counter tallies event types
        ↓
Events + metric snapshot written to MongoDB (stream_events, stream_metrics)
        ↓
Dashboard "Recent Matches" and "Live ATP Matches" read those collections
```

---

## 16. Viva / Teacher Explanation

### How to explain this project in 2 minutes

"Our project analyses six seasons of ATP men's match results – 15,980 matches, 842 players and 477
tournaments, covering the 2019 to 2024 seasons. The user opens a website and can look at a
player's career and season-by-season numbers, filter and inspect any match, see how hard, clay and
grass differ statistically, see top players by wins and recent results, compare any two players
head-to-head, and check whether ATP matches are live right now. Underneath the website there is a
big-data pipeline: we clean the raw CSVs with Pandas, upload the cleaned file to HDFS, run three
Hadoop MapReduce jobs to aggregate player wins, surface statistics and tournament statistics, load
those results into MongoDB, and serve everything to the browser through a read-only REST API. We
also run a stream pipeline that turns every match into win/loss events and processes them with a
Bloom Filter for duplicate detection, a sliding window and Flajolet–Martin approximate distinct
counting, plus a live ATP feed polled every 15 seconds. Everything the website shows is real data –
there is no simulated or hardcoded number in the analytics."

### How to explain the architecture

"Think of it as a line: raw CSV files, then a Python/Pandas cleaning step that merges them and adds
derived columns, then the cleaned CSV goes into HDFS for distributed storage, then MapReduce jobs
run on it, then the aggregated results are loaded into MongoDB collections, then a Python REST API
reads those collections and the cleaned CSV, and finally the single-page frontend renders JSON as
tables and charts. There is a second, parallel path: match rows become stream events, the stream
processor applies the Bloom Filter, the sliding window and Flajolet–Martin, and the events and
metrics land in MongoDB too, which is what powers the recent-matches and live-matches features."

### How to explain MapReduce

"We used Hadoop MapReduce for the aggregation because we want to show the same computation running
across a cluster rather than on one machine. For example, for player wins the mapper reads each
match row and emits just the winner's name with a value of 1 – the mapper does no counting. Hadoop
then does the shuffle and sort, grouping all the lines with the same key together. The reducer
receives one player at a time and simply sums the values, printing the total when the key changes.
The same pattern gives us surface statistics – the mapper averages the winner's and loser's serve
numbers per match and the reducer averages those per surface – and tournament statistics – the
mapper emits the tournament name, 1 and the duration, and the reducer counts matches and averages
the duration. Outputs go to HDFS and are then loaded into MongoDB with small loader scripts."

### How to explain MongoDB

"We store the results in MongoDB because the data is naturally document-shaped – a player's win
record is `{player_name, win_count}` – and because our API returns those objects directly, so there
are no joins and no schema migration when we add a field. We have five collections: three produced
by MapReduce (`player_wins`, `surface_analysis`, `tournament_analysis`) and two produced by the
stream pipelines (`stream_events` and `stream_metrics`). The API layer is strictly read-only;
loaders and stream pipelines are the only writers, and we added indexes on the fields we query, for
example a compound index on `source` and `timestamp` descending for the recent-events query."

### How to explain stream processing

"We model every match as two events, one for the winner and one for the loser, so 15,980 matches
become 31,960 events with deterministic ids. Each event is processed sequentially by
`StreamProcessor`. First a Bloom Filter checks whether the event id has been seen – it is a bit
array with several hash functions, so it uses constant memory instead of storing every id, and it
can give false positives but never false negatives. Then an exact distinct counter (a Python set)
counts unique players and unique matches, while a Flajolet–Martin counter estimates the number of
distinct players in constant memory by hashing values and counting trailing zeros, with the median
of several hash groups for stability. A 100-event sliding window keeps only the newest events so
recent activity can be reported with bounded memory. We also poll a public live ATP feed every 15
seconds, convert those rows into the same event shape and process them through the same pipeline,
upserting the live metrics under a fixed key. Importantly we keep the exact count and the estimate
as separate fields, because they are not the same thing."

### How to explain head-to-head

"The head-to-head page never calculates anything in the browser. The user picks two players from a
dropdown that is filled from the real player list, and the frontend sends both names to
`GET /api/head-to-head`. The backend loads the cleaned dataset, normalises the season with the
shared season helper, and selects only the rows where both players appear – either orientation, so
the order of the names does not change which meetings are found. The winner and loser are read
directly from the winner and loser columns, so we count meetings, wins and win rates, sort the
meetings newest first, and group them by surface to get the surface breakdown. If the two players
have never met, the API returns an empty result and the page says so instead of inventing numbers."

---

## 17. Likely Viva Questions

1. **Why did you use Hadoop?**
   To demonstrate distributed processing on real data: the aggregations run as MapReduce jobs
   across the cluster instead of a single-machine loop, and the same code keeps working if the
   dataset grows.

2. **Why HDFS?**
   It stores the cleaned dataset and the job outputs in one place every node can read, with block
   storage and replication, instead of copying CSVs between machines.

3. **Why MapReduce?**
   It expresses these aggregations (count wins per player, averages per surface, counts per
   tournament) as independent, parallelisable map and reduce steps, which is exactly the shape of
   the problem.

4. **What is a mapper?**
   A mapper processes one input record and emits small key–value pairs. In our jobs it emits the key
   (player, surface or tournament) with a partial value; it does no grouping and no final counting.

5. **What is a reducer?**
   A reducer receives all values produced for one key (after shuffle and sort) and computes the
   final aggregate for that key – a sum, a count or an average – and writes it out.

6. **What happens during shuffle and sort?**
   Hadoop groups the mapper output by key and sorts it, so all lines with the same key are
   consecutive and go to a single reducer. That is why our reducers keep a `current_key` and flush
   whenever it changes.

7. **Why MongoDB instead of MySQL?**
   The results are document-shaped objects the API returns as-is, so there are no joins and no fixed
   schema to migrate; it also scales by sharding and suits event-style data.

8. **Why FastAPI/Starlette for the backend?**
   A small, fast REST layer in Python that fits the rest of the stack, and serving the HTML page from
   the same app means the frontend and the API share an origin, so no CORS setup is needed.

9. **What is a Bloom Filter?**
   A probabilistic set represented by a bit array. Each item is hashed several times and those bits
   are set; testing re-hashes and checks the bits. It uses fixed, small memory instead of storing
   every item.

10. **Can Bloom Filters have false positives?**
    Yes. "Probably seen" can be wrong. They cannot produce false negatives provided the bit array
    has not overflowed. A false positive here would mean a new event counted as a duplicate.

11. **What is a sliding window?**
    A fixed-size buffer holding only the newest N events; when a new event arrives and the window is
    full, the oldest is dropped. Ours keeps 100 events for recent activity with bounded memory.

12. **Why use approximate algorithms?**
    Exact counting needs memory proportional to the number of distinct values, which does not scale
    for an unbounded stream. Approximate algorithms keep memory constant at the price of error.

13. **What is Flajolet–Martin?**
    An approximate distinct-counting algorithm: hash each value, count trailing zero bits, and use
    the fact that the maximum trailing-zero count grows like log2(n) to estimate cardinality. We use
    32 hashes and take the median of grouped averages to reduce variance.

14. **What is NoSQL?**
    A database category that does not require a fixed relational schema; it stores and indexes
    documents for specific query patterns, which suits aggregated results and event data.

15. **Difference between exact and approximate counting?**
    Exact returns the true value with memory that grows with the number of distinct items. Approximate
    returns an estimate within an error bound with constant memory. We report both separately and
    never label an estimate as exact.

16. **How is head-to-head calculated?**
    The backend selects rows where both players appear (either orientation), counts meetings, reads
    the winner from the winner column to count wins, sorts newest first and groups by surface. The
    frontend only displays the result.

17. **How is the dataset cleaned?**
    Six raw CSVs are concatenated, duplicates dropped, dates parsed, text trimmed, surfaces
    normalised, numeric columns coerced, `match_id` created from `tourney_id + match_num`, derived
    percentages computed and validated (outside 0–100 becomes missing), then sorted and saved.

18. **How many matches are in the dataset?**
    15,980 after cleaning, and 31,960 stream events because each match produces a win and a loss
    event.

19. **What years are covered?**
    The 2019 to 2024 seasons. The raw start-date year column labels the 85 matches that began on
    31 December 2018 as "2018", so `season_years()` derives the season from the tournament and match
    ids instead and assigns those rows to 2019.

20. **What happens when two players have no head-to-head meetings?**
    The API returns 200 with `total_meetings: 0` and empty lists, and the page shows a "no recorded
    meetings" message. Nothing is estimated.

21. **How does the frontend communicate with the backend?**
    With plain `fetch()` calls to JSON endpoints under `/api/...` on the same origin. All analytics
    maths happens on the server; JavaScript only renders.

22. **Why MongoDB Atlas?**
    It is a managed, replicated cluster reachable from anywhere, so the same database works locally
    and for the demo without managing servers, and it satisfies the NoSQL requirement.

23. **What happens if there are no live ATP matches?**
    The card shows "No live ATP matches currently available", the normal state when no ATP match is
    in progress. The pipeline still runs and live metrics stay at zero.

24. **Why does the dashboard show "Recent Matches" instead of stream events?**
    Each match produces two internal events; the page renders the winner row of each pair so the user
    sees one real match per row, while event-level data stays available through the API.

25. **Why does the tournament mapper use fixed column positions?**
    The cleaned schema is fixed at 57 columns, so reading column 1 and column 26 directly is faster
    than building a dictionary per row. It is guarded by a length check and a header check, so
    malformed rows are skipped rather than mis-parsed.

26. **What is the difference between the raw and cleaned dataset?**
    Raw holds six per-season files plus a player list; cleaned is one merged CSV with de-duplicated
    rows, typed columns, a unique `match_id`, a `year` column and six derived percentage columns.

27. **How would the system scale to more seasons?**
    Add the new CSV to `data/raw/`, re-run `clean_data.py`, re-upload to HDFS, re-run the three
    MapReduce jobs and the loaders. Nothing else changes, because the loaders upsert by key.

28. **What is idempotency in your pipeline?**
    Re-running the stream pipeline does not double-count: every event has a deterministic
    `event_id`, and the Bloom Filter detects events already seen, which is why the duplicate counter
    exists.

29. **Why is the API read-only?**
    So the presentation layer can never corrupt the analytics. Only loaders and stream pipelines
    write to MongoDB, and an API failure returns an error instead of a plausible but wrong number.

30. **Which parts are computed live versus pre-computed?**
    Player, match and head-to-head statistics are computed per request from the cleaned CSV with
    Pandas; player wins, surface and tournament statistics are pre-computed by MapReduce and read
    from MongoDB; recent matches and live status are read from the stream collections.
---

## 18. File / Folder Guide

| File / Folder | Purpose |
|---------------|---------|
| `frontend/index.html` | The entire frontend: HTML, CSS and JavaScript for all six pages, plus Chart.js rendering |
| `src/api/tennis_dashboard_api.py` | Read-only REST API + serves the frontend (Starlette + Uvicorn) |
| `src/mongodb/tennis_queries.py` | Read-only MongoDB query layer, index plan, `TennisQueries` |
| `src/mongodb/test_tennis_queries.py` | Tests for the query layer |
| `src/analytics/player_analysis.py` | Player list, player summary, season-by-season performance, `season_years()` |
| `src/analytics/match_analysis.py` | Match filtering, pagination, match details |
| `src/analytics/head_to_head.py` | Head-to-head computation |
| `src/analytics/surface_analysis.py` | Standalone Pandas helper for surface-level statistics (not wired into the API) |
| `src/preprocessing/clean_data.py` | Cleaning pipeline → `data/processed/tennis_matches_clean.csv` |
| `src/preprocessing/data_profile.py` | Raw dataset shape/type inspection helper |
| `src/mapreduce/player_wins_mapper.py` / `..._reducer.py` | MapReduce job A – wins per player |
| `src/mapreduce/surface_analysis_mapper.py` / `..._reducer.py` | MapReduce job B – per-surface statistics |
| `src/mapreduce/tournament_mapper.py` / `..._reducer.py` | MapReduce job C – per-tournament statistics |
| `src/load_player_wins.py` | Reads HDFS output → upserts `player_wins` |
| `src/load_surface_analysis.py` | Reads HDFS output → upserts `surface_analysis` |
| `src/load_tournament_analysis.py` | Reads HDFS output → upserts `tournament_analysis` |
| `src/stream_mining/stream_processor.py` | Combines Bloom Filter, exact counter, Flajolet–Martin, sliding window |
| `src/stream_mining/bloom_filter.py` | Bloom Filter implementation |
| `src/stream_mining/sliding_window.py` | Sliding window implementation |
| `src/stream_mining/flajolet_martin.py` | Flajolet–Martin approximate distinct counting |
| `src/stream_mining/distinct_count.py` | Exact distinct counting (set-based) |
| `src/stream_mining/tennis_match_stream.py` | Historical pipeline: CSV → events → MongoDB + JSON output |
| `src/stream_mining/live_tennis_stream.py` | Live ATP feed poller (15 s) → same pipeline |
| `src/stream_mining/live_event_adapter.py` | Normalises live-feed rows into stream events |
| `src/stream_mining/stream_mongodb.py` | Writes `stream_events` and `stream_metrics` |
| `src/stream_mining/test_*.py` | Tests for the streaming components and MongoDB store |
| `src/verify_mongodb.py` | Verification script: collections, indexes, sample documents |
| `src/database_test.py` | Small MongoDB connectivity check |
| `dashboard/stream_analytics.py` | Optional Streamlit view of the stream-processing results |
| `dashboard/app.py` | Entry point that renders the Streamlit view |
| `dashboard/utils.py` | Helpers for the Streamlit view |
| `data/raw/*.csv` | Raw ATP match and player data (committed, ~5.5 MB total) |
| `data/processed/tennis_matches_clean.csv` | Cleaned dataset (committed, 5.6 MB) |
| `data/processed/real_atp_stream_results.json` | Generated stream output (git-ignored, 17 MB) |
| `data/stream/events.csv` | Placeholder for ad-hoc event exports (git-ignored, currently empty) |
| `.env` | Local credentials – **never committed** (git-ignored) |
| `.env.example` | Placeholder variable names for teammates |
| `.gitignore` | Keeps secrets, virtualenvs, caches and generated output out of git |
| `requirements.txt` | Pinned Python dependencies |
| `context.md` | This document |
| `README.md` | Project overview for GitHub |
| `src/recommendation/player_similarity.py` | **Empty placeholder** – no cosine-similarity code is implemented |
| `r_analysis/*.R` | **Empty placeholders** – no R analysis is implemented |
| `hadoop/README.md`, `mongodb/README.md` | **Empty placeholders** – kept for folder structure |

No secrets are stored in any tracked file. The only credentials live in `.env`, which is ignored.
---

## 19. Running the Project

### Prerequisites

* **Python 3.10+** (developed on 3.10.1)
* **MongoDB** – a local instance or an Atlas cluster
* **Java + Hadoop** (only to re-run the MapReduce jobs and loaders; not needed to run the website)

### 1. Clone and create a virtual environment

```bash
git clone <your-private-repo-url> Tennis-BDA
cd Tennis-BDA

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# then edit .env and fill in:
#   MONGODB_URI=          your MongoDB connection string
#   MONGODB_DATABASE=     tennis_analytics
#   LIVE_TENNIS_API_KEY=  (optional, only for the live feed)
```

`.env` is git-ignored. Never commit it.

### 3. Prepare the dataset

The cleaned dataset is committed, so this step is optional. To rebuild it from the raw files:

```bash
.venv/bin/python src/preprocessing/clean_data.py     # → data/processed/tennis_matches_clean.csv
```

### 4. Load MongoDB (optional if the database is already populated)

Upload the cleaned CSV to HDFS and run the three jobs (paths must match the loaders):

```bash
hdfs dfs -mkdir -p /user/giselledmello/tennis/data /user/giselledmello/tennis/output
hdfs dfs -put -f data/processed/tennis_matches_clean.csv /user/giselledmello/tennis/data/

STREAM_JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
INPUT=/user/giselledmello/tennis/data/tennis_matches_clean.csv

hadoop jar $STREAM_JAR -input $INPUT \
  -mapper "python3 src/mapreduce/player_wins_mapper.py" \
  -reducer "python3 src/mapreduce/player_wins_reducer.py" \
  -output /user/giselledmello/tennis/output/player_wins

hadoop jar $STREAM_JAR -input $INPUT \
  -mapper "python3 src/mapreduce/surface_analysis_mapper.py" \
  -reducer "python3 src/mapreduce/surface_analysis_reducer.py" \
  -output /user/giselledmello/tennis/output/surface_analysis

hadoop jar $STREAM_JAR -input $INPUT \
  -mapper "python3 src/mapreduce/tournament_mapper.py" \
  -reducer "python3 src/mapreduce/tournament_reducer.py" \
  -output /user/giselledmello/tennis/output/tournament_analysis

.venv/bin/python src/load_player_wins.py
.venv/bin/python src/load_surface_analysis.py
.venv/bin/python src/load_tournament_analysis.py
```

Stream pipelines (optional – the dashboard already reads whatever is stored):

```bash
.venv/bin/python -m src.stream_mining.tennis_match_stream   # historical events + metrics
.venv/bin/python -m src.stream_mining.live_tennis_stream    # live ATP poller (Ctrl+C to stop)
```

### 5. Start the website

**Must be run from the project root** – the analytics modules resolve
`data/processed/tennis_matches_clean.csv` relative to the working directory:

```bash
.venv/bin/python -m src.api.tennis_dashboard_api
```

Then open <http://127.0.0.1:8000>. The same process serves the API and the frontend, so no second
server and no CORS configuration are needed.

### 6. Optional: Streamlit view of the stream results

```bash
.venv/bin/streamlit run dashboard/app.py
```

### 7. Useful checks

```bash
curl http://127.0.0.1:8000/api/health
.venv/bin/python src/verify_mongodb.py          # collections, indexes, sample documents
```

---

## 20. Known Limitations

1. **The dataset is historical.** It ends on 18 December 2024; there are no results after that date,
   so "recent" means recent *within the dataset*, not today.
2. **Live data depends on a third party.** Live ATP status comes from a public match feed polled
   every 15 seconds. If the feed is down, unreachable or rate-limited, the card shows the last known
   state (usually zero live matches), not an error.
3. **Zero live matches is normal.** Most of the time no ATP match is in progress, so "No live ATP
   matches currently available" is an expected message, not a failure.
4. **Flajolet–Martin is approximate.** On this dataset the estimate (~1,900) differs a lot from the
   exact count (842) because the stream is short relative to the hash range. The exact count is the
   one used for the UI.
5. **Bloom Filters can produce false positives**, so the duplicate count can be marginally too high.
   In the current run it reports 3 duplicates out of 31,960 events.
6. **Only ATP men's matches** are covered – no WTA data.
7. **Ranks are stored as the rank at the time of the match**, so `latest_rank` in a player profile is
   the rank in their most recent recorded match, not a current ranking.
8. **Serve statistics are incomplete for some matches** (retired matches, walkovers), so averages
   are computed only over rows where the value exists, and matches with a missing surface are
   excluded from surface analytics (53 rows).
9. **MapReduce job paths and the local user name are hardcoded** in the loaders
   (`/user/giselledmello/tennis/...`); another user must update them.
10. **One Hadoop path per job** (`part-00000`): with more data and multiple reducers the loaders
    would need to read all parts, not just the first.
11. **A single-node/single-machine demo setup** is what the code is written for; real horizontal
    scaling would require a multi-node cluster and partitioned loaders.
12. **The Streamlit view depends on the generated JSON file**, which is git-ignored, so a fresh
    clone must re-run the historical stream pipeline before using `dashboard/app.py`.

## 📸 Project Outputs

### Dashboard
<img width="1440" height="809" alt="Screenshot 2026-10-03 at 10 19 21 PM" src="https://github.com/user-attachments/assets/0a7385bf-2061-4744-9369-e4a402ca6672" />

### Player Analytics
<img width="1431" height="808" alt="Screenshot 2026-10-03 at 10 20 00 PM" src="https://github.com/user-attachments/assets/f0e12c10-ffb7-4430-b129-4c382ca008b4" />

### Players Analytics 
<img width="1440" height="810" alt="Screenshot 2026-10-03 at 10 20 35 PM" src="https://github.com/user-attachments/assets/edb3e429-a9a2-43a4-bc4b-c49b743c5a0d" />


### Matches Analytics
<img width="1428" height="808" alt="Screenshot 2026-10-03 at 10 21 07 PM" src="https://github.com/user-attachments/assets/9610b3a5-76cd-4671-8d84-5f8525966e07" />

### Player VS Player Analytics
<img width="1440" height="811" alt="Screenshot 2026-10-03 at 10 22 15 PM" src="https://github.com/user-attachments/assets/06803aad-19d8-4750-9947-7b5312bbf88d" />
