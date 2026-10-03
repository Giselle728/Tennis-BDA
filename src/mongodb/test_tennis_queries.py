"""
READ-ONLY verification test for the MongoDB query/analysis layer.

This test only reads data. It never inserts, updates or deletes
documents, never drops or clears collections, and never creates
indexes.

Run from the project root:

python -m src.mongodb.test_tennis_queries
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.mongodb.tennis_queries import TennisQueries


# =========================================================
# Expected values for the current historical dataset
# =========================================================

EXPECTED_STREAM_EVENTS = 31960
EXPECTED_MATCH_WIN = 15980
EXPECTED_MATCH_LOSS = 15980
EXPECTED_DISTINCT_PLAYERS = 842
EXPECTED_DISTINCT_MATCHES = 15980

# Player used for the single-player queries.
SAMPLE_PLAYER = "Novak Djokovic"


# =========================================================
# Printing helpers
# =========================================================

def print_section(number: int, title: str) -> None:

    print("\n" + "=" * 60)
    print(f"{number}. {title}")
    print("-" * 60)


def print_rows(rows: list, limit: int | None = None) -> None:

    if not rows:

        print("  (no documents)")

        return

    if limit is not None:

        rows = rows[:limit]

    for row in rows:

        print("  " + json.dumps(row, default=str))


def main() -> None:

    print("=" * 60)
    print("MONGODB QUERY ANALYSIS TEST")
    print("=" * 60)

    queries = TennisQueries()

    print("\nMongoDB connection: PASS")

    # ---------------------------------------------------------
    # Read-only baseline
    #
    # stored_at is written by the pipelines whenever a document
    # is inserted or updated. Recording the start time lets us
    # prove that this test did not modify any document.
    # ---------------------------------------------------------

    test_started = datetime.now(timezone.utc).replace(tzinfo=None)

    events_before = queries.stream_events.count_documents({})

    metrics_before = queries.stream_metrics.count_documents({})

    print(f"stream_events documents  : {events_before}")
    print(f"stream_metrics documents : {metrics_before}")

    assert events_before == EXPECTED_STREAM_EVENTS, (
        f"Expected {EXPECTED_STREAM_EVENTS} stream events, "
        f"found {events_before}"
    )

    # ---------------------------------------------------------
    # Latest historical metrics snapshot, used for cross-checks
    # ---------------------------------------------------------

    snapshot = queries.stream_metrics.find_one(
        {"source": "historical"},
        {
            "_id": 0,
            "total_events": 1,
            "exact_distinct_players": 1,
            "exact_distinct_matches": 1,
            "event_type_counts": 1,
            "top_players_by_events": 1,
        },
        sort=[("timestamp", -1)]
    )

    # ---------------------------------------------------------
    # 1. Top players by wins
    # ---------------------------------------------------------

    print_section(1, "TOP PLAYERS BY WINS (player_wins)")

    top_wins = queries.get_top_players_by_wins(limit=10)

    print_rows(top_wins)

    assert len(top_wins) == 10

    for row in top_wins:

        assert set(row.keys()) == {"player_name", "win_count"}

        assert row["win_count"] > 0

    win_counts = [row["win_count"] for row in top_wins]

    assert win_counts == sorted(win_counts, reverse=True), (
        "Results must be sorted by win_count descending"
    )

    best_document = queries.player_wins.find_one(
        {},
        {"_id": 0, "player_name": 1, "win_count": 1},
        sort=[("win_count", -1)]
    )

    assert top_wins[0]["player_name"] == best_document["player_name"]

    assert top_wins[0]["win_count"] == best_document["win_count"]

    print("Top players by wins: PASS")

    # ---------------------------------------------------------
    # 2. Matches by surface
    # ---------------------------------------------------------

    print_section(2, "MATCHES BY SURFACE (surface_analysis)")

    surface_matches = queries.get_matches_by_surface()

    print_rows(surface_matches)

    assert len(surface_matches) > 0

    for row in surface_matches:

        assert set(row.keys()) == {"surface", "match_count"}

        assert row["match_count"] > 0

    match_counts = [row["match_count"] for row in surface_matches]

    assert match_counts == sorted(match_counts, reverse=True)

    # Cross-check against an independent collection.
    # Each historical match produced exactly two stream events,
    # so (2 x match_count) must equal the events that have a surface.
    events_with_surface = queries.stream_events.count_documents(
        {
            "source": "historical",
            "surface": {"$ne": None},
        }
    )

    assert sum(match_counts) * 2 == events_with_surface, (
        "surface_analysis totals disagree with stream_events"
    )

    print(
        f"Cross-check: 2 x {sum(match_counts)} matches = "
        f"{events_with_surface} stream events: PASS"
    )

    # ---------------------------------------------------------
    # 3. Surface performance
    # ---------------------------------------------------------

    print_section(3, "SURFACE PERFORMANCE (surface_analysis)")

    surface_performance = queries.get_surface_performance()

    print_rows(surface_performance)

    required_metrics = {
        "surface",
        "match_count",
        "avg_aces",
        "avg_double_faults",
        "avg_first_serve_pct",
        "avg_break_point_conversion_pct",
        "avg_duration_minutes",
    }

    assert len(surface_performance) > 0

    for row in surface_performance:

        assert required_metrics.issubset(row.keys())

        assert row["match_count"] > 0

        for metric in required_metrics - {"surface", "match_count"}:

            assert row[metric] > 0, f"{metric} must be positive"

    print("Surface performance: PASS")

    # ---------------------------------------------------------
    # 4. Tournament analysis
    # ---------------------------------------------------------

    print_section(4, "TOURNAMENT ANALYSIS (tournament_analysis)")

    tournaments = queries.get_tournament_analysis(limit=10)

    print_rows(tournaments)

    assert len(tournaments) == 10

    for row in tournaments:

        assert set(row.keys()) == {
            "tournament",
            "match_count",
            "avg_duration_minutes",
        }

        assert row["match_count"] > 0

        assert row["avg_duration_minutes"] > 0

    tournament_counts = [row["match_count"] for row in tournaments]

    assert tournament_counts == sorted(tournament_counts, reverse=True)

    biggest_tournament = queries.tournament_analysis.find_one(
        {},
        {"_id": 0, "match_count": 1},
        sort=[("match_count", -1)]
    )

    assert tournaments[0]["match_count"] == biggest_tournament["match_count"]

    print("Tournament analysis: PASS")


    # ---------------------------------------------------------
    # 5. Player event activity
    # ---------------------------------------------------------

    print_section(5, "PLAYER EVENT ACTIVITY (stream_events, historical)")

    top_players = queries.get_top_players_by_stream_events(limit=10)

    print_rows(top_players)

    assert len(top_players) == 10

    for row in top_players:

        assert set(row.keys()) == {"player", "events"}

        assert row["events"] > 0

    player_events = [row["events"] for row in top_players]

    assert player_events == sorted(player_events, reverse=True)

    # Cross-check against the metric snapshot recorded by the
    # historical stream pipeline. Nothing is hardcoded: the
    # aggregation result is compared with the stored metrics.
    if snapshot and snapshot.get("top_players_by_events"):

        stored_top_players = {
            entry["player"]: entry["events"]
            for entry in snapshot["top_players_by_events"]
        }

        measured = {
            row["player"]: row["events"]
            for row in top_players
        }

        for player, events in stored_top_players.items():

            assert player in measured, f"{player} missing from query result"

            assert measured[player] == events, (
                f"{player}: query says {measured[player]}, "
                f"stored metrics say {events}"
            )

        print("Cross-check against stream_metrics snapshot: PASS")

    # ---------------------------------------------------------
    # 6. Event type distribution
    # ---------------------------------------------------------

    print_section(6, "EVENT TYPE DISTRIBUTION (stream_events, historical)")

    distribution_rows = queries.get_event_type_distribution()

    print_rows(distribution_rows)

    distribution = {
        row["event_type"]: row["count"]
        for row in distribution_rows
    }

    assert distribution.get("MATCH_WIN") == EXPECTED_MATCH_WIN

    assert distribution.get("MATCH_LOSS") == EXPECTED_MATCH_LOSS

    assert sum(distribution.values()) == events_before

    if snapshot and snapshot.get("event_type_counts"):

        assert distribution == snapshot["event_type_counts"], (
            "Event type counts disagree with stream_metrics snapshot"
        )

        print("Cross-check against stream_metrics snapshot: PASS")

    print("Event type distribution: PASS")

    # ---------------------------------------------------------
    # 7. Surface event distribution
    # ---------------------------------------------------------

    print_section(7, "SURFACE EVENT DISTRIBUTION (stream_events)")

    surface_events = queries.get_stream_events_by_surface()

    print_rows(surface_events)

    events_without_surface = queries.count_events_without_surface()

    print(f"  historical events without a surface: {events_without_surface}")

    print("  (these are excluded from the grouping above)")

    assert len(surface_events) > 0

    for row in surface_events:

        assert set(row.keys()) == {"surface", "events", "avg_minutes"}

        assert row["surface"] is not None

        assert row["events"] > 0

        assert row["avg_minutes"] > 0

    surface_event_counts = [row["events"] for row in surface_events]

    assert surface_event_counts == sorted(surface_event_counts, reverse=True)

    # Every historical event must be accounted for.
    assert sum(surface_event_counts) + events_without_surface == events_before

    print("Surface event distribution: PASS")

    # ---------------------------------------------------------
    # 8. Player performance by surface
    # ---------------------------------------------------------

    print_section(
        8,
        f"PLAYER PERFORMANCE BY SURFACE ({SAMPLE_PLAYER})"
    )

    single_player = queries.get_player_surface_performance(SAMPLE_PLAYER)

    print_rows(single_player)

    assert len(single_player) > 0

    for row in single_player:

        assert set(row.keys()) == {
            "player",
            "surface",
            "event_count",
            "wins",
            "losses",
            "win_pct",
        }

        assert row["player"] == SAMPLE_PLAYER

        # Every historical event is either a win or a loss.
        assert row["wins"] + row["losses"] == row["event_count"]

        assert row["win_pct"] == round(
            row["wins"] / row["event_count"] * 100,
            2
        )

    print(f"Single-player grouping rows: {len(single_player)}: PASS")

    print("\n  All players grouped by surface:")

    all_players = queries.get_player_surface_performance()

    assert len(all_players) > len(single_player)

    for row in all_players:

        assert row["wins"] + row["losses"] == row["event_count"]

    print_rows(all_players[:5])

    print(f"  total (player, surface) groups: {len(all_players)}")

    wins_with_surface = queries.stream_events.count_documents(
        {
            "source": "historical",
            "surface": {"$ne": None},
            "event_type": "MATCH_WIN",
        }
    )

    losses_with_surface = queries.stream_events.count_documents(
        {
            "source": "historical",
            "surface": {"$ne": None},
            "event_type": "MATCH_LOSS",
        }
    )

    assert sum(row["wins"] for row in all_players) == wins_with_surface

    assert sum(row["losses"] for row in all_players) == losses_with_surface

    print("Player surface performance: PASS")


    # ---------------------------------------------------------
    # 9. Recent stream events
    # ---------------------------------------------------------

    print_section(9, "RECENT STREAM EVENTS (newest first)")

    recent_events = queries.get_recent_stream_events(limit=5)

    print_rows(recent_events)

    assert len(recent_events) == 5

    recent_fields = {
        "event_id",
        "timestamp",
        "player",
        "opponent",
        "tournament",
        "surface",
        "event_type",
        "score",
        "source",
    }

    for row in recent_events:

        assert set(row.keys()) == recent_fields

        # The heavy event_data blob must not be returned.
        assert "event_data" not in row

    recent_timestamps = [row["timestamp"] for row in recent_events]

    assert recent_timestamps == sorted(recent_timestamps, reverse=True)

    newest_event = queries.stream_events.find_one(
        {},
        {"_id": 0, "timestamp": 1},
        sort=[("timestamp", -1)]
    )

    assert recent_events[0]["timestamp"] == newest_event["timestamp"]

    print("Recent stream events: PASS")

    # ---------------------------------------------------------
    # 10. Current live stream metrics
    # ---------------------------------------------------------

    print_section(10, "CURRENT LIVE STREAM METRICS (stream_metrics)")

    live_metrics = queries.get_current_live_metrics()

    if live_metrics is None:

        print("  No live metrics document found.")

        print("  This is valid: the live pipeline may not have run yet.")

    else:

        print_rows([live_metrics])

        assert live_metrics["source"] == "live"

        assert live_metrics["metric_key"] == "live_stream_current"

        print(
            f"\n  live total_events : "
            f"{live_metrics.get('total_events')}"
        )

        print(
            f"  live window size  : "
            f"{live_metrics.get('configured_window_size')}"
        )

        print("Current live metrics: PASS")

    # ---------------------------------------------------------
    # 11. Current live events
    # ---------------------------------------------------------

    print_section(11, "CURRENT LIVE EVENTS (stream_events, source=live)")

    live_events = queries.get_current_live_events(limit=20)

    assert isinstance(live_events, list)

    for event in live_events:

        assert event["source"] == "live"

        assert "event_data" not in event

    if not live_events:

        print("  No live events stored right now.")

        print(
            "  This is valid: the Live Tennis API had no ATP "
            "match live, and no fake data is created."
        )

    else:

        print_rows(live_events)

        live_timestamps = [event["timestamp"] for event in live_events]

        assert live_timestamps == sorted(live_timestamps, reverse=True)

    print(f"Live events returned: {len(live_events)}: PASS")

    # ---------------------------------------------------------
    # 12. Historical vs live event count
    # ---------------------------------------------------------

    print_section(12, "EVENT COUNTS BY SOURCE (aggregation)")

    source_counts = queries.get_event_counts_by_source()

    print_rows(source_counts)

    counts_by_source = {
        row["source"]: row["event_count"]
        for row in source_counts
    }

    assert counts_by_source.get("historical") == events_before

    # 'live' is discovered from stream_metrics, so it is reported
    # with 0 when no live event has been stored yet.
    assert "live" in counts_by_source

    assert counts_by_source["live"] >= 0

    assert sum(counts_by_source.values()) == events_before

    print("Event counts by source: PASS")


    # ---------------------------------------------------------
    # 13. Dataset cross-checks against the metrics snapshot
    # ---------------------------------------------------------

    print_section(13, "DATASET CROSS-CHECKS (vs stream_metrics snapshot)")

    distinct_players = len(queries.stream_events.distinct("player"))

    distinct_matches = len(queries.stream_events.distinct("match_id"))

    print(f"  distinct players : {distinct_players}")
    print(f"  distinct matches : {distinct_matches}")

    assert distinct_players == EXPECTED_DISTINCT_PLAYERS

    assert distinct_matches == EXPECTED_DISTINCT_MATCHES

    if snapshot:

        assert snapshot["total_events"] == events_before

        assert snapshot["exact_distinct_players"] == distinct_players

        assert snapshot["exact_distinct_matches"] == distinct_matches

        print("  Aggregation results match the stored stream metrics: PASS")

    # ---------------------------------------------------------
    # 14. Optional player summary
    # ---------------------------------------------------------

    print_section(14, f"PLAYER SUMMARY ({SAMPLE_PLAYER})")

    summary = queries.get_player_summary(SAMPLE_PLAYER)

    print_rows([summary] if summary else [])

    assert summary is not None

    assert summary["player"] == SAMPLE_PLAYER

    # One historical event per player per match, so the number of
    # matches equals the number of events for that player.
    assert summary["matches"] == summary["total_events"]

    assert summary["wins"] + summary["losses"] == summary["total_events"]

    assert summary["surfaces_played"] == len(summary["surface_list"])

    assert summary["surfaces_played"] >= 1

    assert summary["tournaments_played"] >= 1

    assert summary["first_event"] <= summary["last_event"]

    assert queries.get_player_summary("No Such Player") is None

    print("Player summary: PASS")

    # ---------------------------------------------------------
    # 15. Indexes (read-only inspection)
    # ---------------------------------------------------------

    print_section(15, "COLLECTION INDEXES (read-only)")

    indexes = queries.get_analysis_indexes()

    for collection_name in ("stream_events", "stream_metrics"):

        print(f"\n  {collection_name}:")

        for index in indexes[collection_name]:

            print(f"    {index['name']} -> {index['key']}")

    for collection_name, collection_indexes in indexes.items():

        names = [index["name"] for index in collection_indexes]

        assert "_id_" in names, f"{collection_name} has no _id_ index"

    stream_event_indexes = indexes["stream_events"]

    stream_event_fields = {
        field
        for index in stream_event_indexes
        for field, _direction in index["key"]
    }

    analysis_fields = {"source", "surface", "tournament", "player", "timestamp"}

    missing = sorted(analysis_fields - stream_event_fields)

    if missing:

        print(
            f"\n  INFO: analysis indexes not present yet: {missing}"
        )

        print(
            "  Run: python -m src.mongodb.tennis_queries  "
            "to create them."
        )

    else:

        print("\n  All analysis indexes used by this module are present.")

    print("Index inspection: PASS")


    # ---------------------------------------------------------
    # 16. Read-only verification
    # ---------------------------------------------------------

    print_section(16, "READ-ONLY VERIFICATION")

    events_after = queries.stream_events.count_documents({})

    metrics_after = queries.stream_metrics.count_documents({})

    print(f"  stream_events  : {events_before} -> {events_after}")

    print(f"  stream_metrics : {metrics_before} -> {metrics_after}")

    assert events_after == events_before, (
        "stream_events count changed during the test"
    )

    assert metrics_after == metrics_before, (
        "stream_metrics count changed during the test"
    )

    # stored_at / timestamp are written whenever a pipeline inserts
    # or updates a document, so no document may be newer than the
    # start of this test.
    written_events = queries.stream_events.count_documents(
        {"stored_at": {"$gte": test_started}}
    )

    written_metrics = queries.stream_metrics.count_documents(
        {"timestamp": {"$gte": test_started}}
    )

    print(
        f"  documents written during this test: "
        f"stream_events={written_events}, "
        f"stream_metrics={written_metrics}"
    )

    assert written_events == 0, (
        "A stream_events document was modified during this test"
    )

    assert written_metrics == 0, (
        "A stream_metrics document was modified during this test"
    )

    print("No documents inserted, modified or deleted: PASS")

    queries.close()

    print("\nMongoDB connection closed.")

    print("\n" + "=" * 60)
    print("MONGODB QUERY ANALYSIS TEST PASSED")
    print("=" * 60)


if __name__ == "__main__":
    main()

