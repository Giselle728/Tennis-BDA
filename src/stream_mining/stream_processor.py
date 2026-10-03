
"""
Sequential tennis event stream processor.

Combines:
- Bloom Filter
- Exact distinct counting
- Flajolet-Martin approximate counting
- Sliding window
- Event-type frequency counting
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .bloom_filter import BloomFilter
from .distinct_count import DistinctCounter
from .flajolet_martin import FlajoletMartin
from .sliding_window import SlidingWindow


class StreamProcessor:
    """Process tennis events sequentially."""

    def __init__(
        self,
        window_size: int = 100,
        bloom_capacity: int = 100_000,
        bloom_error_rate: float = 0.01,
        fm_hashes: int = 32,
    ) -> None:
        self.window_size = window_size
        self.bloom_capacity = bloom_capacity
        self.bloom_error_rate = bloom_error_rate
        self.fm_hashes = fm_hashes

        self._initialize_components()
        self._initialize_metrics()

    def _initialize_components(self) -> None:
        """Initialize all stream mining components."""
        self.bloom_filter = BloomFilter(
            capacity=self.bloom_capacity,
            error_rate=self.bloom_error_rate,
        )

        self.distinct_players = DistinctCounter()
        self.distinct_matches = DistinctCounter()

        self.flajolet_martin = FlajoletMartin(
            num_hashes=self.fm_hashes
        )

        self.sliding_window = SlidingWindow(
            max_size=self.window_size
        )

    def _initialize_metrics(self) -> None:
        """Initialize processing metrics."""
        self.total_events = 0
        self.duplicate_events = 0
        self.invalid_events = 0

        self.event_type_counts: Counter[str] = Counter()
        self.player_event_counts: Counter[str] = Counter()
        self.match_event_counts: Counter[str] = Counter()

        self.first_timestamp: str | None = None
        self.last_timestamp: str | None = None

    @staticmethod
    def _clean_value(value: Any) -> str:
        """Convert a value to a trimmed string."""
        if value is None:
            return ""

        return str(value).strip()

    @staticmethod
    def _create_event_id(event: dict[str, Any]) -> str:
        """
        Use an explicit event ID when available.

        Otherwise, create a deterministic hash from event fields.
        """
        explicit_id = event.get("event_id")

        if explicit_id is not None:
            explicit_id = str(explicit_id).strip()

            if explicit_id:
                return explicit_id

        fields = [
            "timestamp",
            "match_id",
            "player",
            "event_type",
            "point",
            "score",
        ]

        raw_data = "|".join(
            StreamProcessor._clean_value(
                event.get(field)
            )
            for field in fields
        )

        return hashlib.sha256(
            raw_data.encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _get_player(event: dict[str, Any]) -> str:
        """Extract the player name."""
        for field in (
            "player",
            "player_name",
            "participant",
        ):
            value = event.get(field)

            if value is not None and str(value).strip():
                return str(value).strip()

        return "Unknown"

    @staticmethod
    def _get_match_id(event: dict[str, Any]) -> str:
        """Extract the match ID."""
        for field in (
            "match_id",
            "match",
            "matchId",
        ):
            value = event.get(field)

            if value is not None and str(value).strip():
                return str(value).strip()

        return "Unknown"

    @staticmethod
    def _get_event_type(event: dict[str, Any]) -> str:
        """Extract the event type."""
        for field in (
            "event_type",
            "event",
            "type",
        ):
            value = event.get(field)

            if value is not None and str(value).strip():
                return str(value).strip().upper()

        return "UNKNOWN"

    @staticmethod
    def _get_timestamp(event: dict[str, Any]) -> str:
        """Extract the timestamp."""
        for field in (
            "timestamp",
            "time",
            "datetime",
        ):
            value = event.get(field)

            if value is not None and str(value).strip():
                return str(value).strip()

        return datetime.now().isoformat(
            timespec="seconds"
        )

    @staticmethod
    def _is_valid_event(event: Any) -> bool:
        """Check whether the event is a non-empty dictionary."""
        return isinstance(event, dict) and bool(event)

    def process_event(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Process one event sequentially."""
        if not self._is_valid_event(event):
            self.invalid_events += 1

            return {
                "processed": False,
                "reason": "Invalid event",
            }

        event = dict(event)

        event_id = self._create_event_id(event)
        player = self._get_player(event)
        match_id = self._get_match_id(event)
        event_type = self._get_event_type(event)
        timestamp = self._get_timestamp(event)

        already_seen = self.bloom_filter.might_contain(
            event_id
        )

        if already_seen:
            self.duplicate_events += 1

        self.bloom_filter.add(event_id)

        self.distinct_players.add(player)
        self.distinct_matches.add(match_id)

        self.flajolet_martin.add(player)

        self.event_type_counts[event_type] += 1
        self.player_event_counts[player] += 1
        self.match_event_counts[match_id] += 1

        self.sliding_window.add(event)

        self.total_events += 1

        if self.first_timestamp is None:
            self.first_timestamp = timestamp

        self.last_timestamp = timestamp

        return {
            "processed": True,
            "event_id": event_id,
            "probably_duplicate": already_seen,
            "player": player,
            "match_id": match_id,
            "event_type": event_type,
            "timestamp": timestamp,
        }

    def process_events(
        self,
        events: Iterable[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Process multiple events sequentially."""
        results = []

        for event in events:
            results.append(
                self.process_event(event)
            )

        return results

    def process_csv(
        self,
        file_path: str | Path,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        """Process events from a CSV file."""
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(
                f"CSV file not found: {path}"
            )

        processed_rows = 0

        with path.open(
            mode="r",
            encoding=encoding,
            newline="",
        ) as file:
            reader = csv.DictReader(file)

            if reader.fieldnames is None:
                raise ValueError(
                    "CSV file must contain a header row"
                )

            for row in reader:
                self.process_event(dict(row))
                processed_rows += 1

        metrics = self.get_metrics()
        metrics["csv_file"] = str(path)
        metrics["csv_rows_read"] = processed_rows

        return metrics

    def get_metrics(self) -> dict[str, Any]:
        """Return JSON-serializable metrics."""
        return {
            "total_events": self.total_events,
            "duplicate_events": self.duplicate_events,
            "duplicate_rate": (
                self.duplicate_events / self.total_events
                if self.total_events > 0
                else 0.0
            ),
            "invalid_events": self.invalid_events,
            "exact_distinct_players": (
                self.distinct_players.count()
            ),
            "estimated_distinct_players": (
                self.flajolet_martin.estimate()
            ),
            "exact_distinct_matches": (
                self.distinct_matches.count()
            ),
            "active_window_size": (
                self.sliding_window.size()
            ),
            "configured_window_size": self.window_size,
            "event_type_counts": dict(
                self.event_type_counts
            ),
            "top_players_by_events": [
                {
                    "player": player,
                    "events": count,
                }
                for player, count in (
                    self.player_event_counts.most_common(10)
                )
            ],
            "top_matches_by_events": [
                {
                    "match_id": match_id,
                    "events": count,
                }
                for match_id, count in (
                    self.match_event_counts.most_common(10)
                )
            ],
            "first_timestamp": self.first_timestamp,
            "last_timestamp": self.last_timestamp,
            "recent_events": (
                self.sliding_window.get_events()
            ),
        }

    def get_json_metrics(self) -> str:
        """Return metrics as formatted JSON."""
        return json.dumps(
            self.get_metrics(),
            indent=2,
            default=str,
        )

    def get_recent_events(self) -> list[Any]:
        """Return events in the current sliding window."""
        return self.sliding_window.get_events()

    def get_event_type_counts(self) -> dict[str, int]:
        """Return event-type counts."""
        return dict(self.event_type_counts)

    def reset(self) -> None:
        """Reset all components and metrics."""
        self._initialize_components()
        self._initialize_metrics()