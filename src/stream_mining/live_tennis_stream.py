import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

from src.stream_mining.stream_processor import StreamProcessor
from src.stream_mining.live_event_adapter import create_live_events
from src.stream_mining.stream_mongodb import MongoStreamStore


API_URL = "https://api.livetennisapi.com/api/public/v1/matches"

# Polling interval (kept at the existing value).
POLL_INTERVAL = 15

# Same sliding-window size as the historical pipeline.
WINDOW_SIZE = 100

# MongoDB source label used for live stream documents.
MONGODB_SOURCE = "live"

# Live metrics are continuously updated (upserted) instead of
# inserting a new document every polling cycle.
LIVE_METRICS_KEY = "live_stream_current"


def fetch_live_atp_matches():
    """
    Fetch currently live matches and filter the ATP ones.

    Returns a tuple:
        (all_matches, atp_matches)

    The public API can return matches from several tours,
    so the ATP filtering is always done locally.
    """

    load_dotenv()

    api_key = os.getenv("LIVE_TENNIS_API_KEY")

    if not api_key:
        raise RuntimeError(
            "LIVE_TENNIS_API_KEY not found in environment."
        )

    response = requests.get(
        API_URL,
        params={"status": "live"},
        headers={"X-API-Key": api_key},
        timeout=15,
    )

    response.raise_for_status()

    data = response.json()

    # The API normally wraps the list in a "data" key,
    # but a bare list payload is tolerated as well.
    if isinstance(data, dict):
        all_matches = data.get("data") or []
    elif isinstance(data, list):
        all_matches = data
    else:
        all_matches = []

    # Ignore malformed entries so one bad record cannot
    # break the whole polling cycle.
    all_matches = [
        match
        for match in all_matches
        if isinstance(match, dict)
    ]

    # Keep only ATP matches.
    atp_matches = [
        match
        for match in all_matches
        if str(match.get("tour") or "").lower() == "atp"
    ]

    return all_matches, atp_matches


def print_event(event):
    """Display one normalized live tennis event."""

    print("\n" + "-" * 60)

    print(
        f"{event['player_1']} "
        f"vs "
        f"{event['player_2']}"
    )

    print(f"Processing player : {event['player']}")
    print(f"Match ID          : {event['match_id']}")
    print(f"Tournament        : {event['tournament']}")
    print(f"Surface           : {event['surface']}")
    print(f"Round             : {event['round']}")
    print(f"Sets              : {event['sets']}")
    print(f"Games             : {event['games']}")
    print(f"Points            : {event['points']}")
    print(f"Server            : {event['server']}")
    print(f"Sequence          : {event['sequence']}")
    print(f"Event ID          : {event['event_id']}")


def print_metrics(metrics, title="STREAM METRICS"):
    """Display stream-processing metrics."""

    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

    print(
        f"Total events                  : "
        f"{metrics['total_events']}"
    )

    print(
        f"Duplicate events              : "
        f"{metrics['duplicate_events']}"
    )

    print(
        f"Duplicate rate                : "
        f"{metrics['duplicate_rate']:.2f}%"
    )

    print(
        f"Invalid events                : "
        f"{metrics['invalid_events']}"
    )

    print(
        f"Exact distinct players        : "
        f"{metrics['exact_distinct_players']}"
    )

    print(
        f"Estimated distinct players    : "
        f"{metrics['estimated_distinct_players']}"
    )

    print(
        f"Exact distinct matches        : "
        f"{metrics['exact_distinct_matches']}"
    )

    print(
        f"Active window size            : "
        f"{metrics['active_window_size']}"
    )


def process_atp_match(processor, store, match):
    """
    Convert, process and store the events of one ATP match.

    Returns a tuple:
        (events_created, events_processed, events_saved)
    """

    match_id = match.get("id")

    try:
        # Convert the API match into normalized
        # player-level events.
        events = create_live_events(match)

    except Exception as error:

        print(
            f"  Skipping malformed match "
            f"{match_id}: {error}"
        )

        return 0, 0, 0

    processed_events = []

    for event in events:

        print_event(event)

        # Send each normalized event through the
        # existing stream processor.
        result = processor.process_event(event)

        print(
            f"Processed             : "
            f"{result.get('processed')}"
        )

        print(
            f"Probably duplicate    : "
            f"{result.get('probably_duplicate')}"
        )

        if result.get("processed"):
            processed_events.append(event)

    saved = 0

    if processed_events:

        # Upsert by event_id, so receiving the same API
        # snapshot again does not create duplicate documents.
        saved = store.save_events(
            processed_events,
            source=MONGODB_SOURCE
        )

        print(
            f"Events saved to MongoDB: {saved}"
        )

    return len(events), len(processed_events), saved


def main():
    print("=" * 60)
    print("LIVE ATP TENNIS STREAM")
    print("=" * 60)

    processor = StreamProcessor(window_size=WINDOW_SIZE)

    # ---------------------------------------------------------
    # Connect MongoDB
    # ---------------------------------------------------------

    print("\nConnecting to MongoDB...")

    try:
        store = MongoStreamStore()

    except Exception as error:

        print(f"MongoDB connection failed: {error}")

        return

    print("MongoDB connection: PASS")

    try:
        while True:

            print(
                f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                "Fetching live matches..."
            )

            try:
                all_matches, atp_matches = (
                    fetch_live_atp_matches()
                )

                print(
                    f"API matches received : "
                    f"{len(all_matches)}"
                )

                print(
                    f"ATP matches          : "
                    f"{len(atp_matches)}"
                )

                print(
                    f"Non-ATP filtered     : "
                    f"{len(all_matches) - len(atp_matches)}"
                )

                if not atp_matches:

                    print(
                        "No ATP matches live right now. "
                        "Nothing to store."
                    )

                else:

                    for match in atp_matches:

                        process_atp_match(
                            processor,
                            store,
                            match
                        )

            except requests.RequestException as e:

                print(
                    f"API request failed: {e}"
                )

            except Exception as e:

                print(
                    f"Processing error: {e}"
                )

            # -------------------------------------------------
            # Persist live metrics
            #
            # The live metrics document is upserted, so the
            # collection keeps ONE document that always holds
            # the latest live stream state.
            # -------------------------------------------------

            try:
                metrics = processor.get_metrics()

                store.save_metrics(
                    metrics,
                    source=MONGODB_SOURCE,
                    upsert=True,
                    metric_key=LIVE_METRICS_KEY
                )

                print(
                    f"Live metrics updated : "
                    f"total_events={metrics['total_events']} | "
                    f"duplicates={metrics['duplicate_events']} | "
                    f"players={metrics['exact_distinct_players']} | "
                    f"matches={metrics['exact_distinct_matches']}"
                )

            except Exception as e:

                print(
                    f"Metrics storage failed: {e}"
                )

            print(
                f"\nWaiting {POLL_INTERVAL} seconds..."
            )

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:

        print("\n\nStopping live stream...")

        metrics = processor.get_metrics()

        print_metrics(metrics, "FINAL LIVE STREAM METRICS")

        try:
            store.save_metrics(
                metrics,
                source=MONGODB_SOURCE,
                upsert=True,
                metric_key=LIVE_METRICS_KEY
            )

            print("\nFinal live metrics saved to MongoDB.")

        except Exception as e:

            print(f"Final metrics storage failed: {e}")

    finally:
        store.close()

        print("MongoDB connection closed.")

    print("\nStream stopped.")


if __name__ == "__main__":
    main()