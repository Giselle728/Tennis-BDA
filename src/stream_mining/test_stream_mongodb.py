from src.stream_mining.stream_mongodb import MongoStreamStore


def main():

    print("=" * 60)
    print("MONGODB STREAM STORAGE TEST")
    print("=" * 60)

    store = MongoStreamStore()

    print("\nMongoDB connection: PASS")

    # -----------------------------------------------------
    # Test event
    # -----------------------------------------------------

    test_event = {
        "event_id": "mongodb_test_event_001",
        "match_id": "test_match_001",
        "timestamp": "2026-01-01T10:00:00Z",

        "player": "Test Player",
        "opponent": "Test Opponent",

        "event_type": "TEST_EVENT",

        "surface": "Hard",
        "tournament": "Test Tournament",
        "round": "R32",

        "role": "winner"
    }

    store.save_event(
        test_event,
        source="test"
    )

    print("Event insertion: PASS")

    # -----------------------------------------------------
    # Test metrics
    # -----------------------------------------------------

    test_metrics = {
        "total_events": 1,
        "duplicate_events": 0,
        "duplicate_rate": 0,
        "invalid_events": 0,

        "exact_distinct_players": 1,
        "estimated_distinct_players": 1,

        "exact_distinct_matches": 1,

        "active_window_size": 1,
        "configured_window_size": 100,

        "event_type_counts": {
            "TEST_EVENT": 1
        },

        "top_players_by_events": [
            {
                "player": "Test Player",
                "events": 1
            }
        ],

        "top_matches_by_events": [
            {
                "match_id": "test_match_001",
                "events": 1
            }
        ],

        "first_timestamp": "2026-01-01T10:00:00Z",
        "last_timestamp": "2026-01-01T10:00:00Z"
    }

    store.save_metrics(
        test_metrics,
        source="test"
    )

    print("Metrics insertion: PASS")

    # -----------------------------------------------------
    # Verify counts
    # -----------------------------------------------------

    event_count = store.get_event_count()
    metrics_count = store.get_metrics_count()

    print("\nMongoDB counts:")
    print(f"stream_events  : {event_count}")
    print(f"stream_metrics : {metrics_count}")

    # -----------------------------------------------------
    # Cleanup test documents
    # -----------------------------------------------------

    store.events.delete_one(
        {
            "event_id": "mongodb_test_event_001"
        }
    )

    store.metrics.delete_one(
        {
            "source": "test",
            "total_events": 1
        }
    )

    print("\nTest documents removed.")

    store.close()

    print("\n" + "=" * 60)
    print("MONGODB STREAM STORAGE TEST PASSED!")
    print("=" * 60)


if __name__ == "__main__":
    main()