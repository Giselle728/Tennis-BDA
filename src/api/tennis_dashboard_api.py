"""
Thin, read-only JSON API for the Tennis Analytics dashboard.

Every /api/dashboard/* endpoint delegates to the existing MongoDB
query layer (src/mongodb/tennis_queries.py). No analytics algorithm
is re-implemented here, nothing is written to MongoDB, and no values
are fabricated: missing data is returned as null and the dashboard
renders it as "unavailable".

The /api/players endpoints delegate to the existing analytics
module (src/analytics/player_analysis.py) so the Players page shows
exactly what get_player_analysis() computes.

It also serves the frontend (frontend/index.html) so the page and
the API share one origin (no CORS configuration needed).

Run from the project root:

    .venv/bin/python -m src.api.tennis_dashboard_api

Then open:

    http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import uvicorn
from starlette.applications import Starlette
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Route

# Read-only reuse of the existing player analytics module so the
# Players page shows exactly what get_player_analysis() computes.
from src.analytics.player_analysis import (
    get_player_analysis,
    load_data,
)

# Read-only reuse of the existing match analytics module so the
# Matches page shows exactly what get_match_analysis() /
# get_match_details() return.
from src.analytics.match_analysis import (
    get_match_analysis,
    get_match_details,
)

# Read-only reuse of the existing head-to-head analytics module so the
# Head-to-Head page shows real meetings from the dataset instead of
# values calculated in the browser.
from src.analytics.head_to_head import (
    get_head_to_head,
)

from src.mongodb.tennis_queries import (
    HISTORICAL_SOURCE,
    LIVE_METRICS_KEY,
    LIVE_SOURCE,
    TennisQueries,
)

# Read-only reuse of the live pipeline configuration so the
# dashboard can display it without hardcoding the values.
from src.stream_mining.live_tennis_stream import (
    POLL_INTERVAL,
    WINDOW_SIZE,
)


# =========================================================
# Configuration
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

FRONTEND_INDEX = BASE_DIR / "frontend" / "index.html"

HISTORICAL_CSV = (
    BASE_DIR / "data" / "processed" / "tennis_matches_clean.csv"
)

HOST = "127.0.0.1"
PORT = 8000


# =========================================================
# MongoDB query layer (single shared client)
# =========================================================

_queries: TennisQueries | None = None


def get_queries() -> TennisQueries:
    """
    Return the shared read-only query layer, creating it on
    first use.

    Creating it lazily means the API still starts when MongoDB
    is unreachable; every endpoint then answers with a clear
    error instead of the server refusing to boot.
    """

    global _queries

    if _queries is None:

        _queries = TennisQueries()

    return _queries


# =========================================================
# JSON helpers
# =========================================================

def jsonable(value: Any) -> Any:
    """
    Convert MongoDB/BSON friendly values into JSON safe values.

    datetimes become ISO-8601 strings and nested lists/dicts are
    converted recursively. Nothing is rounded or invented.
    """

    if isinstance(value, dict):

        return {
            str(key): jsonable(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):

        return [jsonable(item) for item in value]

    if isinstance(value, datetime):

        return value.isoformat()

    if isinstance(value, date):

        return value.isoformat()

    return value


def read_only_endpoint(
    builder: Callable[..., dict]
):
    """
    Wrap a payload builder as a read-only GET endpoint.

    The builder receives the query layer and the request, and
    only performs reads. Any failure is reported as HTTP 503 so
    the dashboard can show an honest "backend unavailable" state.
    """

    async def endpoint(request):

        try:

            payload = builder(get_queries(), request)

        except Exception as error:

            return JSONResponse(
                {
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
                status_code=503
            )

        return JSONResponse(jsonable(payload))

    return endpoint


def read_limit(request, default: int, maximum: int) -> int:
    """
    Read ?limit= from the query string with safe bounds.
    """

    raw_value = request.query_params.get("limit")

    if raw_value is None:

        return default

    try:

        limit = int(raw_value)

    except (TypeError, ValueError):

        return default

    return max(1, min(limit, maximum))


# =========================================================
# Payload builders (read-only)
# =========================================================

def build_health(queries, request) -> dict:
    """
    Simple connectivity check for the dashboard.
    """

    counts = queries.get_collection_counts()

    return {
        "status": "ok",
        "database": queries.db.name,
        "collections": counts,
    }


def build_overview(queries, request) -> dict:
    """
    KPI values for the top of the dashboard.

    Every number comes from the query layer; nothing is hardcoded
    in the API or in the frontend.
    """

    counts = queries.get_collection_counts()

    events_by_source = queries.get_event_counts_by_source()

    events_by_source_map = {
        row["source"]: row["event_count"]
        for row in events_by_source
    }

    surfaces = queries.get_matches_by_surface()

    time_range = queries.get_event_time_range(None)

    latest_historical = queries.get_latest_stream_metrics(
        HISTORICAL_SOURCE
    )

    total_matches = queries.count_distinct_matches(
        HISTORICAL_SOURCE
    )

    unique_players = queries.count_distinct_players(
        HISTORICAL_SOURCE
    )

    stream_events = events_by_source_map.get(
        HISTORICAL_SOURCE,
        0
    )

    surface_names = [row["surface"] for row in surfaces]

    return {
        "dataset": {
            "connected": True,
            "database": queries.db.name,
            "source": HISTORICAL_SOURCE,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "first_timestamp": time_range["first_timestamp"],
            "last_timestamp": time_range["last_timestamp"],
            "last_processed_at": (
                latest_historical.get("timestamp")
                if latest_historical
                else None
            ),
        },
        "kpis": {
            "total_matches": total_matches,
            "unique_players": unique_players,
            "stream_events": stream_events,
            "surfaces": len(surfaces),
        },
        "kpi_notes": {
            "total_matches": "Distinct match_id values in stream_events",
            "unique_players": "Distinct player names (historical events)",
            "stream_events": "Processed by StreamProcessor, stored in MongoDB",
            "surfaces": (
                " · ".join(surface_names)
                if surface_names
                else "No surface data"
            ),
        },
        "collection_counts": counts,
        "events_by_source": events_by_source,
    }


def build_surfaces(queries, request) -> dict:
    """
    Surface distribution plus the MapReduce surface averages.
    """

    return {
        "matches_by_surface": queries.get_matches_by_surface(),
        "performance": queries.get_surface_performance(),
        "stream_events_by_surface": queries.get_stream_events_by_surface(),
    }


def build_top_players(queries, request) -> dict:
    """
    Ranked list of the players with the most match wins.
    """

    limit = read_limit(request, 10, 25)

    return {
        "top_players": queries.get_top_players_by_wins(limit=limit),
        "limit": limit,
        "collection": "player_wins",
        "note": (
            "Most match wins in the dataset. This is not a "
            "ranking of player quality."
        ),
    }


def build_stream_metrics(queries, request) -> dict:
    """
    Module 4 stream-mining metrics.

    The preferred source is the snapshot written by the
    historical stream run (stream_metrics, source="historical"),
    because the Flajolet-Martin estimate and the duplicate /
    invalid counters can only be produced by StreamProcessor
    itself.

    If no snapshot exists, only the values that the query layer
    can derive from stream_events are returned and the rest are
    null, so the dashboard shows them as unavailable instead of
    inventing a number.
    """

    snapshot = queries.get_latest_stream_metrics(HISTORICAL_SOURCE)

    if snapshot:

        return {
            "metrics_source": "stream_metrics snapshot",
            "snapshot_timestamp": snapshot.get("timestamp"),
            "total_events": snapshot.get("total_events"),
            "duplicate_events": snapshot.get("duplicate_events"),
            "duplicate_rate": snapshot.get("duplicate_rate"),
            "invalid_events": snapshot.get("invalid_events"),
            "exact_distinct_players": snapshot.get(
                "exact_distinct_players"
            ),
            "estimated_distinct_players": snapshot.get(
                "estimated_distinct_players"
            ),
            "exact_distinct_matches": snapshot.get(
                "exact_distinct_matches"
            ),
            "active_window_size": snapshot.get("active_window_size"),
            "configured_window_size": snapshot.get(
                "configured_window_size"
            ),
            "event_type_counts": snapshot.get("event_type_counts"),
            "first_timestamp": snapshot.get("first_timestamp"),
            "last_timestamp": snapshot.get("last_timestamp"),
        }

    return {
        "metrics_source": "computed from stream_events",
        "snapshot_timestamp": None,
        "total_events": sum(
            row["event_count"]
            for row in queries.get_event_counts_by_source()
            if row["source"] == HISTORICAL_SOURCE
        ),
        "duplicate_events": None,
        "duplicate_rate": None,
        "invalid_events": None,
        "exact_distinct_players": queries.count_distinct_players(
            HISTORICAL_SOURCE
        ),
        "estimated_distinct_players": None,
        "exact_distinct_matches": queries.count_distinct_matches(
            HISTORICAL_SOURCE
        ),
        "active_window_size": None,
        "configured_window_size": None,
        "event_type_counts": {
            row["event_type"]: row["count"]
            for row in queries.get_event_type_distribution()
        },
        "first_timestamp": None,
        "last_timestamp": None,
    }


def build_event_types(queries, request) -> dict:
    """
    MATCH_WIN / MATCH_LOSS distribution in stream_events.
    """

    distribution = queries.get_event_type_distribution()

    return {
        "distribution": distribution,
        "total_events": sum(row["count"] for row in distribution),
    }


def build_recent_events(queries, request) -> dict:
    """
    Most recent stream events (newest first).
    """

    limit = read_limit(request, 15, 50)

    return {
        "events": queries.get_recent_stream_events(limit=limit),
        "limit": limit,
    }


def build_live(queries, request) -> dict:
    """
    Live ATP stream status.

    Live events exist only when the Live Tennis API returned ATP
    matches. When it does not, the dashboard receives an empty
    list and reports that no live ATP matches are available.
    """

    limit = read_limit(request, 20, 50)

    metrics = queries.get_current_live_metrics()

    events = queries.get_current_live_events(limit=limit)

    return {
        "has_live_matches": len(events) > 0,
        "message": (
            "Live ATP matches are being streamed right now."
            if events
            else "No live ATP matches currently available"
        ),
        "metrics": metrics,
        "events": events,
        "event_count": len(events),
        "source": LIVE_SOURCE,
        "metric_key": LIVE_METRICS_KEY,
        "pipeline": {
            "provider": "Live Tennis API",
            "poll_interval_seconds": POLL_INTERVAL,
            "window_size": WINDOW_SIZE,
            "metrics_available": metrics is not None,
            "last_update": (
                metrics.get("timestamp") if metrics else None
            ),
        },
    }



def build_pipeline(queries, request) -> dict:
    """
    Compact status of the end-to-end data pipeline.
    """

    counts = queries.get_collection_counts()

    events_by_source = {
        row["source"]: row["event_count"]
        for row in queries.get_event_counts_by_source()
    }

    historical_metrics = queries.get_latest_stream_metrics(
        HISTORICAL_SOURCE
    )

    total_matches = queries.count_distinct_matches(
        HISTORICAL_SOURCE
    )

    stream_events = events_by_source.get(HISTORICAL_SOURCE, 0)

    window_size = (
        historical_metrics.get("configured_window_size")
        if historical_metrics
        else None
    )

    return {
        "steps": [
            {
                "key": "dataset",
                "label": "Historical ATP Data",
                "status": "ok" if total_matches else "empty",
                "detail": f"{total_matches:,} matches",
            },
            {
                "key": "preprocessing",
                "label": "Preprocessing",
                "status": "ok" if HISTORICAL_CSV.exists() else "missing",
                "detail": (
                    HISTORICAL_CSV.name
                    if HISTORICAL_CSV.exists()
                    else "cleaned CSV not found"
                ),
            },
            {
                "key": "mapreduce",
                "label": "MapReduce Analytics",
                "status": "ok" if counts.get("player_wins") else "empty",
                "detail": (
                    f"{counts.get('player_wins', 0)} player rows · "
                    f"{counts.get('surface_analysis', 0)} surfaces · "
                    f"{counts.get('tournament_analysis', 0)} tournaments"
                ),
            },
            {
                "key": "mongodb",
                "label": "MongoDB",
                "status": "ok",
                "detail": f"{len(counts)} collections · tennis_analytics",
            },
            {
                "key": "stream",
                "label": "Stream Processing",
                "status": "ok" if stream_events else "empty",
                "detail": (
                    f"{stream_events:,} events · window "
                    f"{window_size if window_size else 'n/a'}"
                ),
            },
            {
                "key": "dashboard",
                "label": "Dashboard",
                "status": "live",
                "detail": "read-only API view",
            },
        ],
        "collection_counts": counts,
    }


# =========================================================
# Players page data (reuses src/analytics/player_analysis.py)
# =========================================================

PLAYERS_DATASET = "data/processed/tennis_matches_clean.csv"

MAX_PLAYER_NAME_LENGTH = 120


def dataframe_records(frame: pd.DataFrame) -> list:
    """
    Convert a pandas DataFrame into JSON safe records.

    pandas' own encoder is used so NaN becomes null and numpy
    numbers become plain JSON numbers. No value is rounded,
    rescaled or invented.
    """

    return json.loads(frame.to_json(orient="records"))


def plain_value(value: Any) -> Any:
    """
    Convert pandas/numpy values into plain JSON values.

    numpy scalars are not JSON serialisable and NaN is not valid
    JSON, so both are normalised here (NaN becomes null).
    """

    if isinstance(value, pd.DataFrame):

        return dataframe_records(value)

    if isinstance(value, np.bool_):

        return bool(value)

    if isinstance(value, np.integer):

        return int(value)

    if isinstance(value, (np.floating, float)):

        number = float(value)

        return None if math.isnan(number) else number

    if isinstance(value, dict):

        return {
            str(key): plain_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):

        return [plain_value(item) for item in value]

    return value


def player_list_rows() -> list:
    """
    Build the player list from the cleaned ATP dataset.

    Counts use exactly the same definitions as
    get_player_analysis(): a match is any row where the player
    appears as winner_name or loser_name, matches = wins + losses
    and win_rate = wins / matches * 100.
    """

    dataframe = load_data()

    wins_by_player = dataframe["winner_name"].dropna().value_counts()

    losses_by_player = dataframe["loser_name"].dropna().value_counts()

    rows = []

    for player_name in set(wins_by_player.index) | set(losses_by_player.index):

        wins = int(wins_by_player.get(player_name, 0))

        losses = int(losses_by_player.get(player_name, 0))

        matches = wins + losses

        rows.append({
            "player_name": str(player_name),
            "matches": matches,
            "wins": wins,
            "losses": losses,
            "win_rate": (
                round(wins / matches * 100, 2)
                if matches
                else 0.0
            ),
        })

    rows.sort(
        key=lambda row: (
            -row["wins"],
            -row["matches"],
            row["player_name"],
        )
    )

    return rows


def build_players() -> dict:
    """
    Player list for the Players page (GET /api/players).

    The list is read from the same dataset the analytics layer
    uses, so every player returned here is also resolvable by
    /api/players/{player_name}.
    """

    players = player_list_rows()

    return {
        "players": players,
        "count": len(players),
        "dataset": PLAYERS_DATASET,
        "note": (
            "Same dataset and same win/loss definitions as "
            "src/analytics/player_analysis.get_player_analysis()"
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def players_page(request):
    """
    GET /api/players

    Delegates to the analytics dataset (not MongoDB), so the
    Players page keeps working even when the MongoDB cluster is
    unreachable. Errors are returned as HTTP 503 with a message.
    """

    try:

        payload = build_players()

    except FileNotFoundError as error:

        return JSONResponse(
            {
                "error": (
                    "Dataset not found. Start the API from the "
                    "project root so that data/processed/"
                    "tennis_matches_clean.csv resolves."
                ),
                "detail": str(error),
                "error_type": "FileNotFoundError",
            },
            status_code=503
        )

    except Exception as error:

        return JSONResponse(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            status_code=503
        )

    return JSONResponse(jsonable(payload))


async def player_detail_page(request):
    """
    GET /api/players/{player_name}

    Returns the result of the existing
    src/analytics/player_analysis.get_player_analysis() function
    unchanged, with proper status codes for bad input, unknown
    players and data/backend errors.
    """

    player_name = (
        request.path_params.get("player_name") or ""
    ).strip()

    if not player_name:

        return JSONResponse(
            {
                "error": "Missing player name.",
                "hint": (
                    "Use /api/players/{player_name}, e.g. "
                    "/api/players/Novak%20Djokovic"
                ),
            },
            status_code=400
        )

    if len(player_name) > MAX_PLAYER_NAME_LENGTH:

        return JSONResponse(
            {
                "error": "Player name is too long.",
                "max_length": MAX_PLAYER_NAME_LENGTH,
            },
            status_code=400
        )

    try:

        result = get_player_analysis(player_name)

    except FileNotFoundError as error:

        return JSONResponse(
            {
                "error": (
                    "Dataset not found. Start the API from the "
                    "project root so that data/processed/"
                    "tennis_matches_clean.csv resolves."
                ),
                "detail": str(error),
                "error_type": "FileNotFoundError",
            },
            status_code=503
        )

    except Exception as error:

        return JSONResponse(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            status_code=503
        )

    if result is None:

        return JSONResponse(
            {
                "error": f"Player not found: {player_name}",
                "player_name": player_name,
                "hint": (
                    "GET /api/players lists every player name "
                    "available in the dataset."
                ),
            },
            status_code=404
        )

    return JSONResponse(jsonable(plain_value(result)))


async def players_missing_name_page(request):
    """
    GET /api/players/ (no name at all) answers with a clear 400.
    """

    return JSONResponse(
        {
            "error": "Missing player name.",
            "hint": (
                "Use /api/players/{player_name}, e.g. "
                "/api/players/Novak%20Djokovic"
            ),
        },
        status_code=400
    )


# =========================================================
# Matches page data (reuses src/analytics/match_analysis.py)
# =========================================================

MATCHES_DATASET = PLAYERS_DATASET

# Only the columns the Matches page table displays. Every one of
# them is produced by get_match_analysis(); nothing is recalculated.
MATCH_LIST_COLUMNS = [
    "match_id",
    "year",
    "tourney_name",
    "surface",
    "winner_name",
    "loser_name",
    "score",
]

MATCH_LIST_LIMIT = 100
MATCH_LIST_MAX_LIMIT = 500

MAX_MATCH_ID_LENGTH = 60


def read_int_param(request, name: str) -> "int | None":
    """
    Read an optional integer query parameter (?year=2024).

    Raises ValueError for a non numeric value so the endpoint can
    answer with HTTP 400 instead of silently ignoring the filter.
    """

    raw_value = request.query_params.get(name)

    if raw_value is None or raw_value.strip() == "":

        return None

    try:

        return int(raw_value)

    except (TypeError, ValueError):

        raise ValueError(
            f"Query parameter '{name}' must be an integer, "
            f"got '{raw_value}'."
        )


# =========================================================
# Tournament name variants
# =========================================================

# The cleaned dataset stores the same event under more than one
# spelling, for example the US Open in 2019 ("US Open") and from
# 2020 on ("Us Open"). Counting or aggregating a tournament is done
# on the case and whitespace insensitive name so every year of the
# event is included whatever spelling it was recorded with. The
# list is a small, explicit set so ordinary tournaments (Indian
# Wells Masters, Miami Masters, ...) keep their own names.


def normalise_name(value: Any) -> str:
    """Lower case, whitespace collapsed tournament name."""

    return " ".join(str(value).strip().lower().split())


TOURNAMENT_ALIASES = {
    "us open": "Us Open",
}


def tournament_filter_value(name: str) -> str:
    """
    Map a tournament name to the spelling used when filtering.
    """

    return TOURNAMENT_ALIASES.get(
        normalise_name(name),
        str(name).strip(),
    )


def tournament_names(frame: pd.DataFrame) -> pd.Series:
    """
    Tournament names of a DataFrame in their filter spelling.
    """

    return frame["tourney_name"].map(tournament_filter_value)


def normalise_name_of_series(values: pd.Series) -> pd.Series:
    """
    Case and whitespace insensitive version of a name column.
    """

    return values.astype(str).str.strip().str.lower().str.replace(
        r"\s+", " ", regex=True
    )


def read_offset(request) -> int:
    """
    Read ?offset= (the row the requested page starts at).

    Reuses read_int_param() so a non numeric offset is reported
    exactly like a bad ?year=, and a negative offset falls back to
    the first page.
    """

    offset = read_int_param(request, "offset")

    return max(0, offset or 0)


def build_match_filter_options() -> dict:
    """
    The filter values that really exist in the cleaned dataset.

    Years, surfaces and tournament names are read from the same
    get_match_analysis() rows the table displays, so the Matches
    page cannot offer a value that returns nothing (no "Indoor"
    surface, no "ATP Finals" event) and cannot hide one that does
    exist. Tournament names keep the dataset's spelling
    ("Us Open", "Wimbledon") with the spellings of the same event
    merged, so one option always covers every year of that event.
    """

    frame = get_match_analysis()

    # Counting and aggregating a tournament must include every year
    # of that event, so the dataset's spelling variants ("US Open"
    # in 2019, "Us Open" from 2020) are merged onto one filter value.
    frame = frame.assign(
        tourney_name=tournament_names(frame)
    )

    tournament_values = sorted(
        {
            str(name).strip()
            for name in tournament_names(frame).dropna()
            if str(name).strip()
        },
        key=lambda name: name.lower(),
    )

    year_counts = frame["year"].dropna().value_counts()

    surface_counts = frame["surface"].dropna().value_counts()

    return {
        "years": sorted(
            (int(year) for year in year_counts.index),
            reverse=True,
        ),
        "surfaces": sorted(
            str(surface) for surface in surface_counts.index
        ),
        "tournaments": tournament_values,
        "dataset": MATCHES_DATASET,
        "note": (
            "Years, surfaces and tournaments are the values that "
            "exist in the dataset. The Matches page sends back the "
            "exact tournament name shown here."
        ),
    }


def build_match_list(request) -> dict:
    """
    Match rows for the Matches page (GET /api/matches).

    Filtering is delegated to the existing
    src/analytics/match_analysis.get_match_analysis() function.

    The optional free text 'search' parameter is applied to the
    rows that function returns, because it only supports exact
    player names while the page's search box is a substring search.
    """

    year = read_int_param(request, "year")

    surface = request.query_params.get("surface") or None
    tournament = request.query_params.get("tournament") or None
    player = request.query_params.get("player") or None
    search = (request.query_params.get("search") or "").strip()

    dataframe = get_match_analysis(
        year=year,
        tournament=tournament,
        surface=surface,
        player=player,
    )

    # The same event can appear under more than one spelling in the
    # dataset ("US Open" in 2019, "Us Open" from 2020), so the rows
    # are labelled with the filter spelling before they are counted,
    # searched or listed.
    dataframe = dataframe.assign(
        tourney_name=tournament_names(dataframe)
    )

    if tournament:

        tournament = tournament_filter_value(tournament)

        dataframe = dataframe[
            normalise_name_of_series(dataframe["tourney_name"])
            == normalise_name(tournament)
        ]

    if search:

        needle = search.lower()

        matches_search = (
            dataframe["winner_name"]
            .str.lower()
            .str.contains(needle, regex=False, na=False)
            |
            dataframe["loser_name"]
            .str.lower()
            .str.contains(needle, regex=False, na=False)
            |
            dataframe["tourney_name"]
            .str.lower()
            .str.contains(needle, regex=False, na=False)
        )

        dataframe = dataframe[matches_search]

    total_count = int(len(dataframe))

    # Newest matches first, so the limited page shows recent tennis.
    dataframe = dataframe.sort_values(
        by=["year", "match_id"],
        ascending=False,
    )

    limit = read_limit(request, MATCH_LIST_LIMIT, MATCH_LIST_MAX_LIMIT)

    offset = read_offset(request)

    # Only one page of rows is ever returned, so the page can browse
    # every match of the dataset without placing all 15,980 rows in
    # the DOM.
    matches = dataframe_records(
        dataframe[MATCH_LIST_COLUMNS].iloc[offset:offset + limit]
    )

    total_pages = (
        math.ceil(total_count / limit)
        if total_count
        else 0
    )

    # Every row is on a numbered page, so a deep link (?offset=) that
    # points past the end would otherwise show a blank table.
    if total_pages:

        offset = min(offset, (total_pages - 1) * limit)

        matches = dataframe_records(
            dataframe[MATCH_LIST_COLUMNS].iloc[offset:offset + limit]
        )

    return {
        "matches": matches,
        "count": len(matches),
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "page": (offset // limit + 1) if total_pages else 1,
        "total_pages": total_pages,
        "has_previous_page": offset > 0,
        "has_next_page": total_pages > 0 and offset + len(matches) < total_count,
        "filters": {
            "year": year,
            "surface": surface,
            "tournament": tournament_filter_value(tournament) if tournament else None,
            "player": player,
            "search": search or None,
        },
        "filter_options": build_match_filter_options(),
        "dataset": MATCHES_DATASET,
        "note": (
            "Filtering delegated to "
            "src/analytics/match_analysis.get_match_analysis(); "
            "'search' narrows the returned rows to winner, loser or "
            "tournament name substrings. 'filter_options' lists the "
            "years, surfaces and tournaments that exist in the "
            "dataset, and limit/offset page through the result set."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def matches_page(request):
    """
    GET /api/matches

    Real match rows from the cleaned ATP dataset, filtered by the
    query parameters the Matches page sends. Read-only.
    """

    try:

        payload = build_match_list(request)

    except ValueError as error:

        return JSONResponse({"error": str(error)}, status_code=400)

    except FileNotFoundError as error:

        return JSONResponse(
            {
                "error": (
                    "Dataset not found. Start the API from the "
                    "project root so that data/processed/"
                    "tennis_matches_clean.csv resolves."
                ),
                "detail": str(error),
                "error_type": "FileNotFoundError",
            },
            status_code=503
        )

    except Exception as error:

        return JSONResponse(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            status_code=503
        )

    return JSONResponse(jsonable(payload))


async def match_detail_page(request):
    """
    GET /api/matches/{match_id}

    Returns the result of the existing
    src/analytics/match_analysis.get_match_details() function
    unchanged as JSON.
    """

    match_id = (request.path_params.get("match_id") or "").strip()

    if not match_id:

        return JSONResponse(
            {
                "error": "Missing match id.",
                "hint": (
                    "Use /api/matches/{match_id}, e.g. "
                    "/api/matches/2024-0301_271"
                ),
            },
            status_code=400
        )

    if len(match_id) > MAX_MATCH_ID_LENGTH:

        return JSONResponse(
            {
                "error": "Match id is too long.",
                "max_length": MAX_MATCH_ID_LENGTH,
            },
            status_code=400
        )

    try:

        details = get_match_details(match_id)

    except FileNotFoundError as error:

        return JSONResponse(
            {
                "error": (
                    "Dataset not found. Start the API from the "
                    "project root so that data/processed/"
                    "tennis_matches_clean.csv resolves."
                ),
                "detail": str(error),
                "error_type": "FileNotFoundError",
            },
            status_code=503
        )

    except Exception as error:

        return JSONResponse(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            status_code=503
        )

    if details is None:

        return JSONResponse(
            {
                "error": f"Match not found: {match_id}",
                "match_id": match_id,
                "hint": (
                    "GET /api/matches lists real match_id values "
                    "from the dataset."
                ),
            },
            status_code=404
        )

    return JSONResponse(jsonable(plain_value(details)))


async def matches_missing_id_page(request):
    """
    GET /api/matches/ (no id at all) answers with a clear 400.
    """

    return JSONResponse(
        {
            "error": "Missing match id.",
            "hint": (
                "Use /api/matches/{match_id}, e.g. "
                "/api/matches/2024-0301_271"
            ),
        },
        status_code=400
    )


# =========================================================
# Head-to-Head page data (reuses src/analytics/head_to_head.py)
# =========================================================

HEAD_TO_HEAD_DATASET = PLAYERS_DATASET

EMPTY_HEAD_TO_HEAD = {
    "total_matches": 0,
    "player1_wins": 0,
    "player2_wins": 0,
}


def read_player_param(request, name: str) -> str:
    """
    Read one of the two head-to-head player names from the query
    string (?player1=...&player2=...).

    Raises ValueError when the name is missing or too long, so the
    endpoint answers with HTTP 400 instead of guessing a player.
    """

    player_name = (
        request.query_params.get(name) or ""
    ).strip()

    if not player_name:

        raise ValueError(
            f"Missing '{name}'. Pass both players, e.g. "
            "/api/head-to-head?player1=Novak%20Djokovic"
            "&player2=Daniil%20Medvedev"
        )

    if len(player_name) > MAX_PLAYER_NAME_LENGTH:

        raise ValueError(
            f"'{name}' is too long (max {MAX_PLAYER_NAME_LENGTH} "
            "characters)."
        )

    return player_name


def build_head_to_head(player1: str, player2: str) -> dict:
    """
    Real head-to-head record between two players of the dataset.

    Every meeting where both players appear is counted once, so the
    comparison does not depend on the order of the two names; the win
    counts do follow the requested order. Nothing is derived from win
    rates, rankings or estimates: the numbers come from the match rows
    of the cleaned dataset, per season (2019-2024) and per surface.
    """

    result = get_head_to_head(player1, player2)

    if result is None:

        return {
            "player1": player1,
            "player2": player2,
            **EMPTY_HEAD_TO_HEAD,
            "matches": [],
            "surface_breakdown": [],
            "dataset": HEAD_TO_HEAD_DATASET,
            "note": (
                "No recorded meetings between these players in the "
                "2019-2024 dataset."
            ),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    matches = dataframe_records(
        result["match_history"].rename(
            columns={
                "tourney_date": "date",
                "tourney_name": "tournament",
                "winner_name": "winner",
                "loser_name": "loser",
            }
        )[
            [
                "match_id",
                "date",
                "year",
                "tournament",
                "surface",
                "round",
                "winner",
                "loser",
                "score",
            ]
        ]
    )

    surface_breakdown = [
        {
            "surface": str(row["surface"]),
            "matches": int(row["matches"]),
            "player1_wins": int(row["player_a_wins"]),
            "player2_wins": int(row["player_b_wins"]),
        }
        for row in dataframe_records(result["surface_h2h"])
    ]

    return {
        "player1": player1,
        "player2": player2,

        "total_matches": int(result["total_matches"]),
        "player1_wins": int(result["player_a_wins"]),
        "player2_wins": int(result["player_b_wins"]),

        "player1_win_rate": float(result["player_a_win_rate"]),
        "player2_win_rate": float(result["player_b_win_rate"]),

        # Newest meeting first.
        "matches": matches,

        "surface_breakdown": surface_breakdown,

        "dataset": HEAD_TO_HEAD_DATASET,
        "note": (
            "Every match of the cleaned dataset where both players "
            "participated, counted by the recorded winner. Seasons "
            "use season_years() (2019-2024) and matches are returned "
            "newest first."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def head_to_head_page(request):
    """
    GET /api/head-to-head?player1=...&player2=...

    Real head-to-head record from the cleaned dataset. Read-only:
    nothing is written to MongoDB and nothing is estimated.
    """

    try:

        player1 = read_player_param(request, "player1")
        player2 = read_player_param(request, "player2")

    except ValueError as error:

        return JSONResponse(
            {"error": str(error)},
            status_code=400
        )

    if normalise_name(player1) == normalise_name(player2):

        return JSONResponse(
            {
                "error": "Please select two different players.",
                "player1": player1,
            },
            status_code=400
        )

    known_players = {
        str(row["player_name"])
        for row in player_list_rows()
    }

    for name in (player1, player2):

        if name not in known_players:

            return JSONResponse(
                {
                    "error": f"Player not found: {name}",
                    "hint": (
                        "GET /api/players lists every player name "
                        "available in the dataset."
                    ),
                },
                status_code=404
            )

    try:

        payload = build_head_to_head(player1, player2)

    except FileNotFoundError as error:

        return JSONResponse(
            {
                "error": (
                    "Dataset not found. Start the API from the "
                    "project root so that data/processed/"
                    "tennis_matches_clean.csv resolves."
                ),
                "detail": str(error),
                "error_type": "FileNotFoundError",
            },
            status_code=503
        )

    except Exception as error:

        return JSONResponse(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            status_code=503
        )

    return JSONResponse(jsonable(payload))


# =========================================================
# Routes
# =========================================================

async def index_page(request):
    """
    Serve the dashboard single-page frontend.
    """

    if not FRONTEND_INDEX.exists():

        return JSONResponse(
            {"error": f"Frontend not found at {FRONTEND_INDEX}"},
            status_code=404
        )

    return FileResponse(FRONTEND_INDEX)


ROUTES = [
    Route("/", index_page),
    Route("/api/health", read_only_endpoint(build_health)),
    Route("/api/dashboard/overview", read_only_endpoint(build_overview)),
    Route("/api/dashboard/surfaces", read_only_endpoint(build_surfaces)),
    Route("/api/dashboard/top-players", read_only_endpoint(build_top_players)),
    Route("/api/dashboard/stream", read_only_endpoint(build_stream_metrics)),
    Route("/api/dashboard/event-types", read_only_endpoint(build_event_types)),
    Route("/api/dashboard/recent-events", read_only_endpoint(build_recent_events)),
    Route("/api/dashboard/live", read_only_endpoint(build_live)),
    Route("/api/dashboard/pipeline", read_only_endpoint(build_pipeline)),
    Route("/api/players", players_page),
    Route("/api/players/", players_missing_name_page),
    Route("/api/players/{player_name}", player_detail_page),
    Route("/api/matches", matches_page),
    Route("/api/matches/", matches_missing_id_page),
    Route("/api/matches/{match_id}", match_detail_page),
    Route("/api/head-to-head", head_to_head_page),
]


app = Starlette(routes=ROUTES)


# =========================================================
# Entry point
# =========================================================

def main():

    print("=" * 60)
    print("TENNIS ANALYTICS DASHBOARD")
    print("=" * 60)

    try:

        queries = get_queries()

        counts = queries.get_collection_counts()

        print("\nMongoDB connection: PASS")

        print(
            f"stream_events documents: "
            f"{counts.get('stream_events', 0)}"
        )

    except Exception as error:

        print(f"\nMongoDB connection failed: {error}")

        print(
            "The API starts anyway and returns the error "
            "per request."
        )

    print(f"\nDashboard : http://{HOST}:{PORT}")
    print("API       : read-only, MongoDB tennis_analytics")

    # The players endpoints read the cleaned CSV through
    # src/analytics/player_analysis.py, which uses the relative
    # path data/processed/tennis_matches_clean.csv.
    if HISTORICAL_CSV.exists():

        print(f"Players   : /api/players ({HISTORICAL_CSV.name})")
        print(f"Matches   : /api/matches ({HISTORICAL_CSV.name})")

        if Path.cwd() != BASE_DIR:

            print(
                "Warning   : start the API from the project root so "
                "the relative dataset path resolves."
            )

    else:

        print(f"Players   : dataset not found at {HISTORICAL_CSV}")

    print("\nPress Ctrl+C to stop.\n")

    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        log_level="info"
    )


if __name__ == "__main__":
    main()

