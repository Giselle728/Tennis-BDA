# Tennis Match Analytics & Performance Analysis System

A web platform for exploring ATP tennis data: player performance, match history, head-to-head
records, surface and tournament statistics, and live ATP match status.

Under the hood it is a full big-data pipeline: Pandas preprocessing → **HDFS** → **Hadoop
MapReduce** → **MongoDB** → a read-only REST API → an interactive dashboard.

**Full technical documentation: [context.md](context.md)** – dataset, architecture, every
MapReduce job, the streaming algorithms, the MongoDB collections, all API endpoints and a viva
Q&A guide.

---

## Overview

15,980 ATP men's matches, 842 players, 477 tournaments, 2019–2024 seasons. Every number the site
shows comes from the cleaned dataset or from MongoDB – nothing is simulated in the browser.

## Features

| Feature | Description |
|---------|-------------|
| **Player Performance** | Player wins, losses, win rate, career and season-by-season performance, serve and break-point averages |
| **Match Analytics** | Filter 15,980 matches by season, surface, tournament or player, with full match details |
| **Head-to-Head** | Compare any two players using their real recorded meetings, with a per-surface breakdown |
| **Surface Analysis** | Hard, clay and grass compared by match volume, aces, double faults, first serve, break points and duration |
| **Tournament Insights** | Tournament-level match activity and average match duration |
| **Live Matches** | Current ATP match availability from a live feed |
| **Dashboard** | KPIs, matches by surface, top players by wins, recent matches and live status |

## Architecture

```
Raw ATP CSVs (6 seasons)
        ↓  src/preprocessing/clean_data.py   (Pandas: merge, de-duplicate, derive, validate)
Cleaned dataset (15,980 rows)
        ↓  hdfs dfs -put
HDFS  /user/giselledmello/tennis/
        ↓  src/mapreduce/*_mapper.py + *_reducer.py   (3 Hadoop-streaming jobs)
Aggregated analytics (player wins, surfaces, tournaments)
        ↓  src/load_*.py   (hdfs dfs -cat → MongoDB upserts)
MongoDB  tennis_analytics
        ↓  src/api/tennis_dashboard_api.py   (read-only REST API, also serves the frontend)
Web dashboard  frontend/index.html
        ↓
User

Parallel stream path: matches → MATCH_WIN / MATCH_LOSS events → StreamProcessor
(Bloom Filter · exact distinct count · Flajolet–Martin · sliding window) → MongoDB,
plus a live ATP feed polled every 15 seconds.
```

## Technologies

| Technology | Role |
|------------|------|
| Python 3.10 | Analytics, mappers/reducers, loaders, stream processing, API |
| Pandas / NumPy | Cleaning and per-request analytics |
| Hadoop HDFS | Distributed storage of the dataset and job outputs |
| Hadoop MapReduce | Distributed aggregation (player wins, surfaces, tournaments) |
| MongoDB | Document store for analytics results and stream events |
| Starlette + Uvicorn | Read-only REST API and static frontend, same origin |
| HTML / CSS / JavaScript + Chart.js | The single-page dashboard |
| Streamlit | Optional secondary view of the stream-processing results |

## Dataset

| File | Rows | Notes |
|------|------|-------|
| `data/raw/atp_matches_2019..2024.csv` | 15,980 | raw per-season results |
| `data/raw/atp_players.csv` | 65,989 | player master list |
| `data/processed/tennis_matches_clean.csv` | 15,980 × 57 | cleaned, with `match_id`, `year` and derived serve/break-point percentages |

Seasons covered: **2019–2024**. 85 matches played on 31 December 2018 open the 2019 season and are
counted in 2019 (see [context.md](context.md) for the season-normalization rule).

## Main Modules

```
src/preprocessing/   cleaning pipeline
src/mapreduce/       3 mapper/reducer jobs
src/analytics/       players, matches, head-to-head, season normalization
src/mongodb/         read-only query layer
src/load_*.py        HDFS output → MongoDB loaders
src/stream_mining/   StreamProcessor, Bloom Filter, sliding window, Flajolet–Martin, live feed
src/api/             REST API + static frontend server
frontend/            the single-page dashboard
dashboard/           optional Streamlit stream-analytics view
```

## How to Run

```bash
git clone <repo-url> Tennis-BDA && cd Tennis-BDA

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # add your MONGODB_URI (and LIVE_TENNIS_API_KEY if needed)

.venv/bin/python -m src.api.tennis_dashboard_api
```

Open <http://127.0.0.1:8000>. Run it **from the project root** so the dataset path resolves.

Optional: rebuild the dataset (`.venv/bin/python src/preprocessing/clean_data.py`), re-run the
MapReduce jobs and loaders, or start the Streamlit view
(`.venv/bin/streamlit run dashboard/app.py`). See [context.md](context.md) for the full pipeline
instructions.

`.env` is git-ignored; never commit credentials.

## Documentation

- **[context.md](context.md)** – complete project context: dataset, preprocessing, architecture,
  MapReduce jobs, stream-processing algorithms, MongoDB collections, API reference, head-to-head
  algorithm, end-to-end examples, viva talking points and 30 likely viva questions.
- **[.env.example](.env.example)** – required environment variable names.
