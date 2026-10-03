"""
Tests for the live tennis event adapter and live stream processing.

The test first tries to fetch a real ATP live match.
If no ATP match is currently live, it uses a controlled
ATP-shaped test fixture.

This does NOT pretend the fixture is live data.
"""

from __future__ import annotations

from dotenv import load_dotenv

from src.stream_mining.live_event_adapter import create_live_events
from src.stream_mining.live_tennis_stream import fetch_live_atp_matches
from src.stream_mining.stream_processor import StreamProcessor


def create_test_match() -> dict:
    """
    Controlled fallback fixture matching the Live Tennis API structure.

    Used only when no ATP match is currently live.
    """

    return {
        "id": "test_live_match_001",
        "status": "live",
        "tour": "atp",
        "tournament": "Test ATP Tournament",
        "tournament_id": "test_tournament_001",
        "surface": "Hard",
        "round": "Semi-finals",
        "round_code": "SF",
        "format": "best_of_3",
        "players": {
            "p1": {
                "id": "player_001",
                "name": "Test Player One",
                "ranking": 10,
            },
            "p2": {
                "id": "player_002",
                "name": "Test Player Two",
                "ranking": 20,
            },
        },
        "score": {
            "sets": [
                {
                    "p1": 1,
                    "p2": 0,
                }
            ],
            "games": {
                "p1": 4,
                "p2": 3,
            },
            "points": {
                "p1": 30,
                "p2": 15,
            },
            "server": "p1",
            "is_tiebreak": False,
            "sequence": 1,
            "timestamp": "2026-09-20T10:00:00Z",
            "stale": False,
            "observed_age_seconds": 2,
        },
    }


def main() -> None:
    print("=" * 60)
    print("LIVE TENNIS STREAM TEST")
    print("=" * 60)

    load_dotenv()

    # ---------------------------------------------------------
    # Step 1: Try to obtain an actual live ATP match
    # ---------------------------------------------------------

    print("\nChecking Live Tennis API for an ATP match...")

    api_error = None

    try:
        all_matches, live_matches = (
            fetch_live_atp_matches()
        )

    except Exception as error:
        # The test must not fail just because the API
        # request could not be completed.
        api_error = error
        all_matches, live_matches = [], []

    using_real_match = bool(live_matches)

    if api_error is not None:
        print(f"API request failed   : {api_error}")

    print(f"API matches received : {len(all_matches)}")
    print(f"ATP matches          : {len(live_matches)}")

    if using_real_match:
        match = live_matches[0]

        print("\nREAL ATP LIVE MATCH FOUND")
        print(f"Match ID     : {match.get('id')}")
        print(f"Tournament   : {match.get('tournament')}")
        print(f"Surface      : {match.get('surface')}")
        print(f"Round        : {match.get('round')}")

    else:
        match = create_test_match()

        print("\nNo ATP match is live right now.")
        print("Using controlled TEST FALLBACK fixture.")

    # ---------------------------------------------------------
    # Step 2: Convert match into common stream events
    # ---------------------------------------------------------

    events = create_live_events(match)

    print("\nEvents created:", len(events))

    assert len(events) == 2, (
        "A match with two players should create exactly two events."
    )

    assert events[0]["match_id"] == str(match["id"])
    assert events[1]["match_id"] == str(match["id"])

    assert events[0]["player"]
    assert events[1]["player"]

    assert events[0]["event_type"] == "LIVE_MATCH_UPDATE"
    assert events[1]["event_type"] == "LIVE_MATCH_UPDATE"

    print("Event adapter test passed")

    # ---------------------------------------------------------
    # Step 3: Process events through the real StreamProcessor
    # ---------------------------------------------------------

    processor = StreamProcessor(window_size=10)

    for event in events:
        result = processor.process_event(event)

        print(
            f"Processed: {event['player']} | "
            f"probably_duplicate={result['probably_duplicate']}"
        )

        assert result["processed"] is True
        assert result["player"] == event["player"]
        assert result["match_id"] == event["match_id"]
        assert result["event_type"] == "LIVE_MATCH_UPDATE"

        # These are the first occurrences of these event IDs.
        assert result["probably_duplicate"] is False

    # ---------------------------------------------------------
    # Step 4: Verify stream metrics
    # ---------------------------------------------------------

    metrics = processor.get_metrics()

    print("\nSTREAM METRICS")
    print("-" * 60)

    print(
        "Total events              :",
        metrics["total_events"],
    )

    print(
        "Exact distinct players    :",
        metrics["exact_distinct_players"],
    )

    print(
        "Estimated distinct players:",
        metrics["estimated_distinct_players"],
    )

    print(
        "Exact distinct matches    :",
        metrics["exact_distinct_matches"],
    )

    print(
        "Duplicate events          :",
        metrics["duplicate_events"],
    )

    print(
        "Invalid events            :",
        metrics["invalid_events"],
    )

    # ---------------------------------------------------------
    # Step 5: Verify expected metrics
    # ---------------------------------------------------------

    assert metrics["total_events"] == 2

    assert metrics["exact_distinct_players"] == 2

    assert metrics["exact_distinct_matches"] == 1

    assert metrics["invalid_events"] == 0

    assert metrics["duplicate_events"] == 0

    assert metrics["event_type_counts"]["LIVE_MATCH_UPDATE"] == 2

    # ---------------------------------------------------------
    # Step 6: Test duplicate detection
    # ---------------------------------------------------------

    print("\nTesting duplicate event detection...")

    duplicate_result = processor.process_event(events[0])

    print(
        "Duplicate event detected:",
        duplicate_result["probably_duplicate"],
    )

    assert duplicate_result["processed"] is True

    assert duplicate_result["probably_duplicate"] is True

    duplicate_metrics = processor.get_metrics()

    assert duplicate_metrics["duplicate_events"] >= 1

    # ---------------------------------------------------------
    # Final result
    # ---------------------------------------------------------

    print("\n" + "=" * 60)
    print("ALL LIVE TENNIS STREAM TESTS PASSED!")
    print("=" * 60)

    if using_real_match:
        print("Data source: REAL LIVE ATP API")
    else:
        print("Data source: CONTROLLED TEST FALLBACK")


if __name__ == "__main__":
    main()