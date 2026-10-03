"""
MongoDB query and analysis layer for the tennis BDA project.

This module demonstrates NoSQL querying on the existing
tennis_analytics database:

    - document filtering and projection        (find)
    - sorting and limiting                     (sort / limit)
    - aggregation pipelines                    ($match, $group, $sort,
                                                $limit, $project, $sum,
                                                $avg, $cond, $addToSet,
                                                $min, $max)
    - simple index management                  (create_index)

The module is READ-ONLY with respect to the tennis data.
No method here inserts, updates or deletes documents.

Collections used:

    player_wins         -> MapReduce player win counts
    surface_analysis    -> MapReduce per-surface averages
    tournament_analysis -> MapReduce per-tournament averages
    stream_events       -> stream-processing events (historical + live)
    stream_metrics      -> stream-processing metric snapshots

Run the index setup (creates only missing indexes):

python -m src.mongodb.tennis_queries
"""

from __future__ import annotations

import os
from typing import Any

import certifi
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient


# =========================================================
# Configuration
# =========================================================

DATABASE_NAME = "tennis_analytics"

# Source labels written by the two streaming pipelines.
HISTORICAL_SOURCE = "historical"
LIVE_SOURCE = "live"

# Key of the single, continuously updated live metrics document.
LIVE_METRICS_KEY = "live_stream_current"

# Event types produced by the existing stream processor.
MATCH_WIN = "MATCH_WIN"
MATCH_LOSS = "MATCH_LOSS"

COLLECTION_NAMES = (
    "player_wins",
    "surface_analysis",
    "tournament_analysis",
    "stream_events",
    "stream_metrics",
)


# =========================================================
# Query Layer
# =========================================================

class TennisQueries:
    """
    Read-only MongoDB query layer for tennis analytics.

    Every public method returns plain Python lists/dicts so the
    results can be printed or serialised directly.
    """

    # Indexes that the analysis queries benefit from.
    # Directions must match the query sort order.
    INDEX_PLAN: dict[str, list[list[tuple[str, int]]]] = {
        "stream_events": [
            [("source", ASCENDING)],
            [("surface", ASCENDING)],
            [("tournament", ASCENDING)],
            [("source", ASCENDING), ("timestamp", DESCENDING)],
        ],
        "stream_metrics": [
            [("source", ASCENDING)],
            [("metric_key", ASCENDING)],
        ],
        "player_wins": [
            [("win_count", DESCENDING)],
        ],
        "tournament_analysis": [
            [("match_count", DESCENDING)],
        ],
    }

    def __init__(self, database_name: str = DATABASE_NAME):

        load_dotenv()

        mongo_uri = os.getenv("MONGODB_URI")

        if not mongo_uri:

            raise RuntimeError(
                "MONGODB_URI not found in .env file."
            )

        self.client = MongoClient(
            mongo_uri,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=10000
        )

        # Verify the Atlas connection immediately.
        self.client.admin.command("ping")

        self.db = self.client[database_name]

        self.player_wins = self.db["player_wins"]
        self.surface_analysis = self.db["surface_analysis"]
        self.tournament_analysis = self.db["tournament_analysis"]
        self.stream_events = self.db["stream_events"]
        self.stream_metrics = self.db["stream_metrics"]

    def close(self):

        self.client.close()

    # =====================================================
    # Indexes
    # =====================================================

    @staticmethod
    def _index_key(spec: list[tuple[str, int]]) -> tuple:

        return tuple(
            (field, int(direction))
            for field, direction in spec
        )

    def _existing_index_keys(self, collection) -> set:
        """
        Return the key patterns of the indexes that already exist.

        Used to avoid creating an index twice (MongoDB would
        accept the call, but creating duplicates is wasteful).
        """

        existing = set()

        for index in collection.list_indexes():

            existing.add(
                tuple(
                    (field, int(direction))
                    for field, direction in index["key"].items()
                )
            )

        return existing

    def ensure_analysis_indexes(self) -> dict[str, list[str]]:
        """
        Create the indexes used by the analysis queries.

        Only missing indexes are created. Existing indexes are
        never dropped or replaced, and index creation does not
        change any document.

        Returns a mapping of collection -> newly created index names.
        """

        created: dict[str, list[str]] = {}

        for collection_name, specs in self.INDEX_PLAN.items():

            collection = self.db[collection_name]

            existing = self._existing_index_keys(collection)

            created_names = []

            for spec in specs:

                if self._index_key(spec) in existing:
                    continue

                created_names.append(
                    collection.create_index(spec)
                )

            created[collection_name] = created_names

        return created

    def get_analysis_indexes(self) -> dict[str, list[dict[str, Any]]]:
        """
        Read-only snapshot of the indexes on the analysis collections.

        Demonstrates the difference between a single-field index
        and a compound index.
        """

        summary: dict[str, list[dict[str, Any]]] = {}

        for collection_name in COLLECTION_NAMES:

            collection = self.db[collection_name]

            summary[collection_name] = [
                {
                    "name": index["name"],
                    "key": [
                        [field, int(direction)]
                        for field, direction in index["key"].items()
                    ],
                    "unique": bool(index.get("unique", False)),
                }
                for index in collection.list_indexes()
            ]

        return summary

    # =====================================================
    # Read-only helpers used by the dashboard API
    # =====================================================

    def count_distinct_players(
        self,
        source: str | None = None
    ) -> int:
        """
        Number of distinct players seen in stream_events.

        Optionally restricted to one source ('historical' / 'live').
        """

        query = {"source": source} if source else {}

        return len(
            self.stream_events.distinct("player", query)
        )

    def count_distinct_matches(
        self,
        source: str | None = None
    ) -> int:
        """
        Number of distinct matches seen in stream_events.

        Optionally restricted to one source ('historical' / 'live').
        """

        query = {"source": source} if source else {}

        return len(
            self.stream_events.distinct("match_id", query)
        )

    def get_latest_stream_metrics(
        self,
        source: str = HISTORICAL_SOURCE
    ) -> dict | None:
        """
        Most recent stream metrics snapshot for one source.

        Read-only lookup of the snapshot written by the stream
        pipeline (StreamProcessor.get_metrics()).
        """

        return self.stream_metrics.find_one(
            {"source": source},
            {"_id": 0},
            sort=[("timestamp", DESCENDING)]
        )

    def get_event_time_range(
        self,
        source: str | None = None
    ) -> dict:
        """
        First and last event timestamp in stream_events.
        """

        query = {"source": source} if source else {}

        first = self.stream_events.find_one(
            query,
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", ASCENDING)]
        )

        last = self.stream_events.find_one(
            query,
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", DESCENDING)]
        )

        return {
            "first_timestamp": first["timestamp"] if first else None,
            "last_timestamp": last["timestamp"] if last else None,
        }

    def get_collection_counts(self) -> dict[str, int]:
        """
        Document count of every analysis collection.

        Used by the dashboard data-pipeline status card.
        """

        return {
            name: self.db[name].count_documents({})
            for name in COLLECTION_NAMES
        }

    # =====================================================
    # 1. Top players by match wins
    # =====================================================

    def get_top_players_by_wins(self, limit: int = 10) -> list[dict]:
        """
        Top players by number of matches won.

        Source: player_wins (MapReduce output).
        Pipeline: $match -> $sort -> $limit -> $project
        """

        pipeline = [
            {"$match": {"win_count": {"$gt": 0}}},

            {"$sort": {"win_count": DESCENDING}},

            {"$limit": limit},

            {
                "$project": {
                    "_id": 0,
                    "player_name": 1,
                    "win_count": 1,
                }
            },
        ]

        return list(self.player_wins.aggregate(pipeline))

    # =====================================================
    # 2. Matches by surface
    # =====================================================

    def get_matches_by_surface(self) -> list[dict]:
        """
        Number of matches per surface.

        Source: surface_analysis (MapReduce output).
        """

        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "surface": 1,
                    "match_count": 1,
                }
            },

            {"$sort": {"match_count": DESCENDING}},
        ]

        return list(self.surface_analysis.aggregate(pipeline))

    # =====================================================
    # 3. Surface performance
    # =====================================================

    def get_surface_performance(self) -> list[dict]:
        """
        Per-surface averages calculated by the MapReduce job.

        Source: surface_analysis.

        The averages are queried, not recalculated, because the
        stream/raw collections do not store break-point or
        first-serve information: only the MapReduce job has it.
        """

        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "surface": 1,
                    "match_count": 1,
                    "avg_aces": 1,
                    "avg_double_faults": 1,
                    "avg_first_serve_pct": 1,
                    "avg_break_point_conversion_pct": 1,
                    "avg_duration_minutes": 1,
                }
            },

            {"$sort": {"match_count": DESCENDING}},
        ]

        return list(self.surface_analysis.aggregate(pipeline))

    # =====================================================
    # 4. Tournament analysis
    # =====================================================

    def get_tournament_analysis(self, limit: int = 10) -> list[dict]:
        """
        Tournaments with the most matches and their average duration.

        Source: tournament_analysis (MapReduce output).
        Pipeline: $match -> $sort -> $limit -> $project
        """

        pipeline = [
            {"$match": {"match_count": {"$gt": 0}}},

            {"$sort": {"match_count": DESCENDING}},

            {"$limit": limit},

            {
                "$project": {
                    "_id": 0,
                    "tournament": 1,
                    "match_count": 1,
                    "avg_duration_minutes": 1,
                }
            },
        ]

        return list(self.tournament_analysis.aggregate(pipeline))


    # =====================================================
    # 5. Player event activity
    # =====================================================

    def get_top_players_by_stream_events(
        self,
        limit: int = 10
    ) -> list[dict]:
        """
        Players with the most stream events.

        Source: stream_events, historical events only.

        Pipeline: $match -> $group -> $sort -> $limit -> $project

        One historical match produces one event per player, so
        'events' is also the number of matches the player appears in.
        """

        pipeline = [
            {"$match": {"source": HISTORICAL_SOURCE}},

            {
                "$group": {
                    "_id": "$player",
                    "events": {"$sum": 1},
                }
            },

            {"$sort": {"events": DESCENDING, "_id": ASCENDING}},

            {"$limit": limit},

            {
                "$project": {
                    "_id": 0,
                    "player": "$_id",
                    "events": 1,
                }
            },
        ]

        return list(self.stream_events.aggregate(pipeline))

    # =====================================================
    # 6. Event type distribution
    # =====================================================

    def get_event_type_distribution(self) -> list[dict]:
        """
        Count of every event type present in stream_events.

        Pipeline: $match -> $group -> $sort -> $project
        """

        pipeline = [
            {"$match": {"source": HISTORICAL_SOURCE}},

            {
                "$group": {
                    "_id": "$event_type",
                    "count": {"$sum": 1},
                }
            },

            # Secondary sort on _id keeps the output deterministic
            # when two event types have the same count.
            {"$sort": {"count": DESCENDING, "_id": ASCENDING}},

            {
                "$project": {
                    "_id": 0,
                    "event_type": "$_id",
                    "count": 1,
                }
            },
        ]

        return list(self.stream_events.aggregate(pipeline))

    # =====================================================
    # 7. Surface event distribution
    # =====================================================

    def get_stream_events_by_surface(self) -> list[dict]:
        """
        Stream events per surface, plus the average match duration
        computed directly from the raw stream events.

        Source: stream_events, historical events only.

        Events without a surface are excluded
        ({"$match": {"surface": {"$ne": None}}}), because grouping
        on a missing value would produce a meaningless bucket.
        The number of excluded events is reported separately by
        count_events_without_surface().

        Pipeline: $match -> $group -> $sort -> $project
        The average uses $avg over the 'minutes' field.
        """

        pipeline = [
            {
                "$match": {
                    "source": HISTORICAL_SOURCE,
                    "surface": {"$ne": None},
                }
            },

            {
                "$group": {
                    "_id": "$surface",
                    "events": {"$sum": 1},
                    "avg_minutes": {"$avg": "$minutes"},
                }
            },

            {"$sort": {"events": DESCENDING}},

            {
                "$project": {
                    "_id": 0,
                    "surface": "$_id",
                    "events": 1,
                    "avg_minutes": 1,
                }
            },
        ]

        results = list(self.stream_events.aggregate(pipeline))

        # Round only for display; the aggregation stays simple.
        for row in results:

            if row["avg_minutes"] is not None:

                row["avg_minutes"] = round(
                    row["avg_minutes"],
                    2
                )

        return results

    def count_events_without_surface(self) -> int:
        """
        Number of historical events that have no surface value.

        Documented explicitly because get_stream_events_by_surface()
        skips these documents.
        """

        return self.stream_events.count_documents(
            {
                "source": HISTORICAL_SOURCE,
                "surface": None,
            }
        )


    # =====================================================
    # 8. Player performance by surface
    # =====================================================

    def get_player_surface_performance(
        self,
        player_name: str | None = None
    ) -> list[dict]:
        """
        Per-player, per-surface event counts with wins and losses.

        Source: stream_events, historical events only.

        Pipeline: $match -> $group (with $cond) -> $sort -> $project

        $cond is used for conditional aggregation: an event only
        adds to 'wins' when its event_type is MATCH_WIN, and only
        to 'losses' when it is MATCH_LOSS.

        If player_name is given, the pipeline filters to that
        player. Otherwise every player is returned.

        Events without a surface are excluded.
        """

        match_stage: dict[str, Any] = {
            "source": HISTORICAL_SOURCE,
            "surface": {"$ne": None},
        }

        if player_name:

            match_stage["player"] = player_name

        pipeline = [
            {"$match": match_stage},

            {
                "$group": {
                    "_id": {
                        "player": "$player",
                        "surface": "$surface",
                    },

                    "event_count": {"$sum": 1},

                    "wins": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$event_type", MATCH_WIN]},
                                1,
                                0,
                            ]
                        }
                    },

                    "losses": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$event_type", MATCH_LOSS]},
                                1,
                                0,
                            ]
                        }
                    },
                }
            },

            {"$sort": {"event_count": DESCENDING, "_id.player": ASCENDING}},

            {
                "$project": {
                    "_id": 0,
                    "player": "$_id.player",
                    "surface": "$_id.surface",
                    "event_count": 1,
                    "wins": 1,
                    "losses": 1,
                }
            },
        ]

        results = list(self.stream_events.aggregate(pipeline))

        # A win percentage is correct here because every historical
        # event is either MATCH_WIN or MATCH_LOSS for one player, so
        # wins + losses is exactly the number of matches played on
        # that surface. Division by zero is handled explicitly.
        for row in results:

            matches_played = row["wins"] + row["losses"]

            if matches_played > 0:

                row["win_pct"] = round(
                    row["wins"] / matches_played * 100,
                    2
                )

            else:

                row["win_pct"] = 0.0

        return results

    # =====================================================
    # 9. Recent stream events
    # =====================================================

    def get_recent_stream_events(
        self,
        limit: int = 10,
        source: str | None = None
    ) -> list[dict]:
        """
        Most recent stream events ordered by timestamp.

        Source: stream_events.

        This uses a normal document query (find + projection +
        sort + limit) rather than an aggregation pipeline, because
        no grouping is required.

        'event_data' is deliberately excluded from the projection:
        the individual fields above it are enough for analysis.
        """

        query: dict[str, Any] = {}

        if source:

            query["source"] = source

        projection = {
            "_id": 0,
            "event_id": 1,
            "timestamp": 1,
            "player": 1,
            "opponent": 1,
            "tournament": 1,
            "surface": 1,
            "event_type": 1,
            "score": 1,
            "source": 1,
        }

        return list(
            self.stream_events
            .find(query, projection)
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )


    # =====================================================
    # 10. Current live stream metrics
    # =====================================================

    def get_current_live_metrics(self) -> dict | None:
        """
        The single, continuously updated live metrics document.

        Source: stream_metrics
        Filter: source="live", metric_key="live_stream_current"

        The live pipeline upserts this document on every polling
        cycle, so it always holds the current live stream state.

        Returns None when no live metrics document exists yet.
        It never fabricates values.
        """

        return self.stream_metrics.find_one(
            {
                "source": LIVE_SOURCE,
                "metric_key": LIVE_METRICS_KEY,
            },
            {"_id": 0}
        )

    # =====================================================
    # 11. Current live events
    # =====================================================

    def get_current_live_events(self, limit: int = 20) -> list[dict]:
        """
        Live stream events ordered by timestamp (newest first).

        Source: stream_events where source="live".

        An empty list is a valid result: it simply means no ATP
        match was live while the live pipeline was running.
        """

        projection = {
            "_id": 0,
            "event_id": 1,
            "timestamp": 1,
            "player": 1,
            "opponent": 1,
            "tournament": 1,
            "surface": 1,
            "event_type": 1,
            "source": 1,
        }

        return list(
            self.stream_events
            .find({"source": LIVE_SOURCE}, projection)
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )

    # =====================================================
    # 12. Historical vs live event count
    # =====================================================

    def get_event_counts_by_source(self) -> list[dict]:
        """
        Number of stored stream events per source.

        Pipeline: $group

        Only sources that really exist in stream_events are
        returned by the pipeline. The list of source labels is
        therefore completed from the source values recorded in
        stream_metrics, so a source with zero events (for example
        'live') is reported with a count of 0 instead of being
        silently dropped.

        No counts are hardcoded.
        """

        pipeline = [
            {
                "$group": {
                    "_id": "$source",
                    "event_count": {"$sum": 1},
                }
            },
        ]

        counts = {
            row["_id"]: row["event_count"]
            for row in self.stream_events.aggregate(pipeline)
            if row["_id"] is not None
        }

        # Sources are discovered from the data itself.
        known_sources = set(counts)

        known_sources.update(self.stream_metrics.distinct("source"))

        return [
            {
                "source": source,
                "event_count": counts.get(source, 0),
            }
            for source in sorted(known_sources)
        ]


    # =====================================================
    # Optional: player summary
    # =====================================================

    def get_player_summary(self, player_name: str) -> dict | None:
        """
        Consolidated summary for one player.

        Pipeline: $match -> $group with
            $sum / $cond / $addToSet / $min / $max

        Returns None when the player has no historical events.
        """

        pipeline = [
            {
                "$match": {
                    "source": HISTORICAL_SOURCE,
                    "player": player_name,
                }
            },

            {
                "$group": {
                    "_id": "$player",

                    "total_events": {"$sum": 1},

                    "wins": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$event_type", MATCH_WIN]},
                                1,
                                0,
                            ]
                        }
                    },

                    "losses": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$event_type", MATCH_LOSS]},
                                1,
                                0,
                            ]
                        }
                    },

                    "matches": {"$addToSet": "$match_id"},
                    "surfaces": {"$addToSet": "$surface"},
                    "tournaments": {"$addToSet": "$tournament"},

                    "first_event": {"$min": "$timestamp"},
                    "last_event": {"$max": "$timestamp"},
                }
            },
        ]

        results = list(self.stream_events.aggregate(pipeline))

        if not results:

            return None

        row = results[0]

        surfaces = sorted(
            surface
            for surface in row["surfaces"]
            if surface
        )

        tournaments = sorted(
            tournament
            for tournament in row["tournaments"]
            if tournament
        )

        matches_played = row["wins"] + row["losses"]

        return {
            "player": player_name,
            "matches": len(row["matches"]),
            "total_events": row["total_events"],
            "wins": row["wins"],
            "losses": row["losses"],
            "win_pct": (
                round(row["wins"] / matches_played * 100, 2)
                if matches_played > 0
                else 0.0
            ),
            "surfaces_played": len(surfaces),
            "surface_list": surfaces,
            "tournaments_played": len(tournaments),
            "first_event": row["first_event"],
            "last_event": row["last_event"],
        }


# =========================================================
# Index setup entry point
# =========================================================

def main():

    print("=" * 60)
    print("TENNIS MONGODB ANALYSIS LAYER")
    print("=" * 60)

    queries = TennisQueries()

    print("\nMongoDB connection: PASS")

    created = queries.ensure_analysis_indexes()

    print("\nIndex check (only missing indexes are created):")

    for collection_name, names in created.items():

        if names:

            for name in names:

                print(f"  created: {name} ({collection_name})")

        else:

            print(f"  {collection_name}: all analysis indexes already exist")

    print("\nIndexes now available:")

    for collection_name, indexes in queries.get_analysis_indexes().items():

        print(f"\n  {collection_name}:")

        for index in indexes:

            print(f"    {index['name']} -> {index['key']}")

    queries.close()

    print("\nMongoDB connection closed.")


if __name__ == "__main__":
    main()

