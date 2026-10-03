"""
Test the complete stream mining implementation.

Run from the project root:

python -m src.stream_mining.test_stream_mining
"""

from __future__ import annotations

import json

from .bloom_filter import BloomFilter
from .distinct_count import DistinctCounter
from .flajolet_martin import FlajoletMartin
from .sliding_window import SlidingWindow
from .stream_processor import StreamProcessor


def test_bloom_filter() -> None:
    bloom = BloomFilter(
        capacity=100,
        error_rate=0.01,
    )

    bloom.add("event_1")
    bloom.add("event_2")

    assert bloom.might_contain("event_1")
    assert bloom.might_contain("event_2")

    print("Bloom Filter test passed")


def test_distinct_counter() -> None:
    counter = DistinctCounter()

    counter.add("Djokovic")
    counter.add("Alcaraz")
    counter.add("Djokovic")

    assert counter.count() == 2

    print("Distinct Counter test passed")


def test_flajolet_martin() -> None:
    estimator = FlajoletMartin(
        num_hashes=32
    )

    for index in range(1000):
        estimator.add(f"player_{index}")

    estimate = estimator.estimate()

    assert estimate > 0

    print(
        "Flajolet-Martin test passed"
        f" | Estimate: {estimate}"
    )


def test_sliding_window() -> None:
    window = SlidingWindow(max_size=3)

    window.add("event_1")
    window.add("event_2")
    window.add("event_3")
    window.add("event_4")

    assert window.size() == 3
    assert window.get_events() == [
        "event_2",
        "event_3",
        "event_4",
    ]

    print("Sliding Window test passed")


def test_stream_processor() -> None:
    processor = StreamProcessor(
        window_size=3
    )

    events = [
        {
            "event_id": "event_1",
            "timestamp": "2024-01-01 10:00:01",
            "match_id": "match_1",
            "player": "Novak Djokovic",
            "event_type": "ACE",
        },
        {
            "event_id": "event_2",
            "timestamp": "2024-01-01 10:00:02",
            "match_id": "match_1",
            "player": "Carlos Alcaraz",
            "event_type": "POINT_WON",
        },
        {
            "event_id": "event_1",
            "timestamp": "2024-01-01 10:00:03",
            "match_id": "match_1",
            "player": "Novak Djokovic",
            "event_type": "ACE",
        },
        {
            "event_id": "event_3",
            "timestamp": "2024-01-01 10:00:04",
            "match_id": "match_2",
            "player": "Jannik Sinner",
            "event_type": "DOUBLE_FAULT",
        },
    ]

    results = processor.process_events(events)
    metrics = processor.get_metrics()

    assert len(results) == 4
    assert metrics["total_events"] == 4
    assert metrics["duplicate_events"] >= 1
    assert metrics["exact_distinct_players"] == 3
    assert metrics["exact_distinct_matches"] == 2
    assert metrics["active_window_size"] == 3

    json.dumps(metrics)

    print("Stream Processor test passed")
    print(json.dumps(metrics, indent=2))


def main() -> None:
    test_bloom_filter()
    test_distinct_counter()
    test_flajolet_martin()
    test_sliding_window()
    test_stream_processor()

    print("\nAll stream mining tests passed!")


if __name__ == "__main__":
    main()