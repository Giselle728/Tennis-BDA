import os
from datetime import datetime, timezone
from typing import Any

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


class MongoStreamStore:
    """
    Handles MongoDB storage for stream-processing results.

    Collections:
        stream_events  -> individual processed stream events
        stream_metrics -> aggregate stream-processing metrics
    """

    def __init__(
        self,
        database_name: str = "tennis_analytics"
    ):
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

        # Verify connection immediately.
        self.client.admin.command("ping")

        self.db = self.client[database_name]

        self.events = self.db["stream_events"]
        self.metrics = self.db["stream_metrics"]

        self._create_indexes()

    # =====================================================
    # Indexes
    # =====================================================

    def _create_indexes(self):
        """
        Create indexes useful for stream queries.
        """

        # event_id uniquely identifies a processed event.
        self.events.create_index(
            "event_id",
            unique=True
        )

        # Useful for match-based queries.
        self.events.create_index(
            "match_id"
        )

        # Useful for player analytics.
        self.events.create_index(
            "player"
        )

        # Useful for chronological queries.
        self.events.create_index(
            "timestamp"
        )

        # Useful for event-type analysis.
        self.events.create_index(
            "event_type"
        )

        # Metrics are queried chronologically.
        self.metrics.create_index(
            "timestamp"
        )

        self.metrics.create_index(
            "source"
        )

        # Used to keep a single up-to-date "live" metrics document.
        self.metrics.create_index(
            "metric_key"
        )

    # =====================================================
    # Normalize Event
    # =====================================================

    @staticmethod
    def build_event_document(
        event: dict[str, Any],
        source: str = "unknown"
    ) -> dict[str, Any]:
        """
        Build the normalized MongoDB document for one stream event.

        Both stream types are supported:

        Historical ATP events use:
            tourney_name / tourney_id

        Live API events use:
            tournament / tournament_id

        The normalized document always exposes the common field
        names (tournament, tournament_id) while event_data keeps
        the original event untouched.
        """

        tournament = (
            event.get("tournament")
            or event.get("tourney_name")
        )

        tournament_id = (
            event.get("tournament_id")
            or event.get("tourney_id")
        )

        return {
            "event_id": event.get("event_id"),
            "source": source,

            "match_id": event.get("match_id"),
            "timestamp": event.get("timestamp"),

            "player": event.get("player"),
            "opponent": event.get("opponent"),

            "event_type": event.get("event_type"),

            "surface": event.get("surface"),
            "tournament": tournament,
            "tournament_id": tournament_id,
            "round": event.get("round"),

            "role": event.get("role"),

            # Live API fields
            "player_id": event.get("player_id"),
            "player_1": event.get("player_1"),
            "player_2": event.get("player_2"),
            "player_1_id": event.get("player_1_id"),
            "player_2_id": event.get("player_2_id"),

            "sets": event.get("sets"),
            "games": event.get("games"),
            "points": event.get("points"),
            "server": event.get("server"),

            "sequence": event.get("sequence"),
            "stale": event.get("stale"),

            # Historical ATP fields
            "year": event.get("year"),
            "score": event.get("score"),
            "minutes": event.get("minutes"),
            "ranking": event.get("ranking"),
            "ranking_points": event.get("ranking_points"),
            "aces": event.get("aces"),
            "double_faults": event.get("double_faults"),

            # Keep the original normalized event as well.
            "event_data": event,

            "stored_at": datetime.now(timezone.utc)
        }

    # =====================================================
    # Store One Event
    # =====================================================

    def save_event(
        self,
        event: dict[str, Any],
        source: str = "unknown"
    ) -> bool:
        """
        Store one processed stream event.

        Uses upsert so rerunning the same historical stream
        does not create duplicate documents.
        """

        event_id = event.get("event_id")

        if not event_id:
            raise ValueError(
                "Stream event must contain event_id."
            )

        document = self.build_event_document(event, source)

        self.events.replace_one(
            {"event_id": event_id},
            document,
            upsert=True
        )

        return True

    # =====================================================
    # Store Multiple Events
    # =====================================================

    def save_events(
        self,
        events: list[dict[str, Any]],
        source: str = "unknown"
    ) -> int:
        """
        Store multiple stream events efficiently.

        Returns the number of events processed.
        """

        if not events:
            return 0

        operations = []

        for event in events:

            event_id = event.get("event_id")

            if not event_id:
                continue

            document = self.build_event_document(
                event,
                source
            )

            operations.append(
                UpdateOne(
                    {"event_id": event_id},
                    {"$set": document},
                    upsert=True
                )
            )

        if operations:
            self.events.bulk_write(
                operations,
                ordered=False
            )

        return len(operations)

    # =====================================================
    # Store Stream Metrics
    # =====================================================

    def save_metrics(
        self,
        metrics: dict[str, Any],
        source: str = "unknown",
        upsert: bool = False,
        metric_key: str | None = None
    ) -> str:
        """
        Store stream-processing metrics.

        Default behavior (upsert=False) inserts a NEW snapshot
        document. This is what the historical pipeline uses, so
        every historical run keeps its own metrics snapshot.

        Live behavior (upsert=True) keeps ONE up-to-date document
        per source/metric_key, so a stream that polls every 15
        seconds does not create thousands of metrics documents.

        Returns the stored document id as a string.
        """

        document = {
            "source": source,
            "timestamp": datetime.now(timezone.utc),

            "total_events": metrics.get(
                "total_events",
                0
            ),

            "duplicate_events": metrics.get(
                "duplicate_events",
                0
            ),

            "duplicate_rate": metrics.get(
                "duplicate_rate",
                0
            ),

            "invalid_events": metrics.get(
                "invalid_events",
                0
            ),

            "exact_distinct_players": metrics.get(
                "exact_distinct_players",
                0
            ),

            "estimated_distinct_players": metrics.get(
                "estimated_distinct_players",
                0
            ),

            "exact_distinct_matches": metrics.get(
                "exact_distinct_matches",
                0
            ),

            "active_window_size": metrics.get(
                "active_window_size",
                0
            ),

            "configured_window_size": metrics.get(
                "configured_window_size",
                0
            ),

            "event_type_counts": metrics.get(
                "event_type_counts",
                {}
            ),

            "top_players_by_events": metrics.get(
                "top_players_by_events",
                []
            ),

            "top_matches_by_events": metrics.get(
                "top_matches_by_events",
                []
            ),

            "first_timestamp": metrics.get(
                "first_timestamp"
            ),

            "last_timestamp": metrics.get(
                "last_timestamp"
            )
        }

        if upsert:
            # Keep a single, continuously updated document.
            key = metric_key or f"{source}_current"

            document["metric_key"] = key

            self.metrics.update_one(
                {
                    "source": source,
                    "metric_key": key
                },
                {"$set": document},
                upsert=True
            )

            stored = self.metrics.find_one(
                {
                    "source": source,
                    "metric_key": key
                },
                {"_id": 1}
            )

            return str(stored["_id"]) if stored else ""

        # Default: insert a new metrics snapshot document.
        result = self.metrics.insert_one(document)

        return str(result.inserted_id)

    # =====================================================
    # Collection Counts
    # =====================================================

    def get_event_count(self) -> int:
        return self.events.count_documents({})

    def get_metrics_count(self) -> int:
        return self.metrics.count_documents({})

    # =====================================================
    # Close Connection
    # =====================================================

    def close(self):
        self.client.close()