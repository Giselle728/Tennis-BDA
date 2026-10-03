"""
MongoDB integration test for the LIVE tennis stream.

Covered flow:

    controlled live-style fixture
        -> create_live_events()
        -> StreamProcessor.process_event()
        -> MongoStreamStore.save_events() / save_event()
        -> MongoStreamStore.save_metrics()

The test never requires a real ATP match to be live. It uses a
controlled fixture for the write/upsert test and it only removes
the documents that it created itself.

The 31,960 historical stream_events documents are never touched.

Run from the project root:

python -m src.stream_mining.test_live_stream_mongodb
"""

from __future__ import annotations

from src.stream_mining.live_event_adapter import create_live_events
from src.stream_mining.live_tennis_stream import fetch_live_atp_matches
from src.stream_mining.stream_mongodb import MongoStreamStore
from src.stream_mining.stream_processor import StreamProcessor
from src.stream_mining.test_live_tennis_stream import create_test_match


# Unique identifiers so that only THIS test's documents are removed.
TEST_MATCH_ID = "test_live_mongodb_match_001"
TEST_METRICS_KEY = "live_stream_mongodb_test"
TEST_HISTORICAL_EVENT_ID = "test_historical_normalization_001"


def create_live_fixture() -> dict:
    """
    Controlled ATP-shaped fixture used for the MongoDB test.

    This is TEST data and is never presented as real live data.
    """

    match = create_test_match()

    match["id"] = TEST_MATCH_ID

    return match


def create_historical_fixture() -> dict:
    """
    Historical-style event using tourney_name / tourney_id.

    Used to prove the MongoDB normalization stays backward
    compatible with the historical pipeline.
    """

    return {
        "event_id": TEST_HISTORICAL_EVENT_ID,
        "match_id": "test_historical_normalization_match",
        "timestamp": "2024-01-01T10:00:00Z",

        "player": "Historical Test Player",
        "opponent": "Historical Test Opponent",

        "event_type": "MATCH_WIN",
        "role": "winner",

        "surface": "Clay",

        # Historical ATP field names.
        "tourney_name": "Historical Test Tournament",
        "tourney_id": "hist_test_001",

        "year": 2024,
        "score": "6-4 6-3",
        "minutes": 95,
        "ranking": 5,
        "ranking_points": 4321,
        "aces": 7,
        "double_faults": 2,
    }


def main() -> None:

    print("=" * 60)
    print("LIVE STREAM MONGODB INTEGRATION TEST")
    print("=" * 60)

    # ---------------------------------------------------------
    # Step 1: Live API status (informational only)
    # ---------------------------------------------------------

    print("\nChecking Live Tennis API (informational only)...")

    try:
        all_matches, atp_matches = fetch_live_atp_matches()

        print(f"API matches received : {len(all_matches)}")
        print(f"ATP matches          : {len(atp_matches)}")

        if atp_matches:
            print("A real ATP match is live (not used for this test).")
        else:
            print("No ATP match is live right now.")

    except Exception as error:

        print(f"API request failed   : {error}")

    print(
        "\nUsing a CONTROLLED TEST FIXTURE for the write tests."
    )

    # ---------------------------------------------------------
    # Step 2: MongoDB connection
    # ---------------------------------------------------------

    store = MongoStreamStore()

    print("\nMongoDB connection: PASS")

    events_before = store.get_event_count()
    metrics_before = store.get_metrics_count()

    historical_events_before = store.events.count_documents(
        {"source": "historical"}
    )

    historical_metrics_before = store.metrics.count_documents(
        {"source": "historical"}
    )

    print(f"stream_events before      : {events_before}")
    print(f"stream_metrics before     : {metrics_before}")
    print(f"historical events before  : {historical_events_before}")
    print(f"historical metrics before : {historical_metrics_before}")

    test_event_ids = []

    try:
        # -----------------------------------------------------
        # Step 3: Create live-style events from the fixture
        # -----------------------------------------------------

        match = create_live_fixture()

        events = create_live_events(match)

        print(f"\nLive-style events created : {len(events)}")

        assert len(events) == 2

        expected_ids = [
            f"live_{TEST_MATCH_ID}_1_p1",
            f"live_{TEST_MATCH_ID}_1_p2",
        ]

        # Existing event ID format is preserved.
        assert [
            event["event_id"] for event in events
        ] == expected_ids

        test_event_ids = list(expected_ids)

        # Live events use tournament / tournament_id.
        assert events[0]["tournament"] == match["tournament"]
        assert events[0]["tournament_id"] == match["tournament_id"]
        assert events[0]["opponent"] == "Test Player Two"
        assert events[1]["opponent"] == "Test Player One"

        print("Live event creation: PASS")


        # -----------------------------------------------------
        # Step 4: Process the events
        # -----------------------------------------------------

        processor = StreamProcessor(window_size=10)

        for event in events:

            result = processor.process_event(event)

            assert result["processed"] is True
            assert result["event_type"] == "LIVE_MATCH_UPDATE"
            assert result["player"] == event["player"]
            assert result["probably_duplicate"] is False

        print("Stream processing: PASS")

        # -----------------------------------------------------
        # Step 5: Store the processed events
        # -----------------------------------------------------

        saved = store.save_events(events, source="live")

        assert saved == 2

        stored_count = store.events.count_documents(
            {"event_id": {"$in": expected_ids}}
        )

        assert stored_count == 2

        print("Live event storage: PASS")

        # -----------------------------------------------------
        # Step 6: Verify the normalized live document
        # -----------------------------------------------------

        document = store.events.find_one(
            {"event_id": expected_ids[0]}
        )

        assert document is not None
        assert document["source"] == "live"
        assert document["match_id"] == TEST_MATCH_ID
        assert document["event_type"] == "LIVE_MATCH_UPDATE"

        # Live field names are kept as-is.
        assert document["tournament"] == match["tournament"]
        assert document["tournament_id"] == match["tournament_id"]

        # Live-specific fields are preserved.
        assert document["player_1"] == "Test Player One"
        assert document["player_2"] == "Test Player Two"
        assert document["sequence"] == 1
        assert document["sets"] == match["score"]["sets"]
        assert document["games"] == match["score"]["games"]
        assert document["points"] == match["score"]["points"]

        # event_data still holds the original event.
        assert "event_data" in document
        assert document["event_data"]["event_id"] == expected_ids[0]

        print("Live document normalization: PASS")



        # -----------------------------------------------------
        # Step 7: Verify historical backward compatibility
        # -----------------------------------------------------

        historical_event = create_historical_fixture()

        store.save_event(
            historical_event,
            source="historical_test"
        )

        historical_document = store.events.find_one(
            {"event_id": TEST_HISTORICAL_EVENT_ID}
        )

        assert historical_document is not None

        # tourney_name -> tournament fallback
        assert (
            historical_document["tournament"]
            == historical_event["tourney_name"]
        )

        # tourney_id -> tournament_id fallback
        assert (
            historical_document["tournament_id"]
            == historical_event["tourney_id"]
        )

        # Historical fields are preserved.
        assert historical_document["year"] == 2024
        assert historical_document["score"] == "6-4 6-3"
        assert historical_document["minutes"] == 95
        assert historical_document["ranking"] == 5
        assert historical_document["ranking_points"] == 4321
        assert historical_document["aces"] == 7
        assert historical_document["double_faults"] == 2

        assert (
            historical_document["event_data"]["tourney_name"]
            == "Historical Test Tournament"
        )

        print("Historical normalization fallback: PASS")

        # -----------------------------------------------------
        # Step 8: Reprocessing the same event must not create
        #         a second stream_events document
        # -----------------------------------------------------

        duplicate_result = processor.process_event(events[0])

        assert duplicate_result["processed"] is True
        assert duplicate_result["probably_duplicate"] is True

        store.save_event(events[0], source="live")

        duplicates_stored = store.events.count_documents(
            {"event_id": expected_ids[0]}
        )

        assert duplicates_stored == 1

        print("Duplicate protection (upsert by event_id): PASS")

        # -----------------------------------------------------
        # Step 9: Store live metrics (upserted, single document)
        # -----------------------------------------------------

        metrics = processor.get_metrics()

        store.save_metrics(
            metrics,
            source="live",
            upsert=True,
            metric_key=TEST_METRICS_KEY
        )

        # Calling it twice must NOT create a second document.
        store.save_metrics(
            metrics,
            source="live",
            upsert=True,
            metric_key=TEST_METRICS_KEY
        )

        metrics_documents = store.metrics.count_documents(
            {"source": "live", "metric_key": TEST_METRICS_KEY}
        )

        assert metrics_documents == 1

        stored_metrics = store.metrics.find_one(
            {"source": "live", "metric_key": TEST_METRICS_KEY}
        )

        assert stored_metrics["total_events"] == 3
        assert stored_metrics["duplicate_events"] == 1
        assert stored_metrics["exact_distinct_players"] == 2
        assert stored_metrics["exact_distinct_matches"] == 1

        print("Live metrics upsert: PASS")

    finally:
        # -----------------------------------------------------
        # Cleanup: remove ONLY the documents created by this test
        # -----------------------------------------------------

        if test_event_ids:
            store.events.delete_many(
                {"event_id": {"$in": test_event_ids}}
            )

        store.events.delete_many({"match_id": TEST_MATCH_ID})

        store.events.delete_many(
            {"event_id": TEST_HISTORICAL_EVENT_ID}
        )

        store.metrics.delete_many(
            {"metric_key": TEST_METRICS_KEY}
        )

        print("\nTest documents removed.")

        # -----------------------------------------------------
        # Verify that nothing else was touched
        # -----------------------------------------------------

        events_after = store.get_event_count()
        metrics_after = store.get_metrics_count()

        historical_events_after = store.events.count_documents(
            {"source": "historical"}
        )

        historical_metrics_after = store.metrics.count_documents(
            {"source": "historical"}
        )

        print(f"stream_events after       : {events_after}")
        print(f"stream_metrics after      : {metrics_after}")
        print(f"historical events after   : {historical_events_after}")
        print(f"historical metrics after  : {historical_metrics_after}")

        assert events_after == events_before, (
            "Test cleanup did not restore stream_events."
        )

        assert metrics_after == metrics_before, (
            "Test cleanup did not restore stream_metrics."
        )

        assert historical_events_after == historical_events_before, (
            "Historical stream_events documents were modified!"
        )

        assert historical_metrics_after == historical_metrics_before, (
            "Historical stream_metrics documents were modified!"
        )

        store.close()

        print("MongoDB connection closed.")

    # ---------------------------------------------------------
    # Final result
    # ---------------------------------------------------------

    print("\n" + "=" * 60)
    print("LIVE STREAM MONGODB INTEGRATION TEST PASSED!")
    print("=" * 60)
    print("Data source: CONTROLLED TEST FIXTURE")
    print(
        f"Historical stream_events preserved: {events_after}"
    )


if __name__ == "__main__":
    main()

