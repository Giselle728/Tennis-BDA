import json
from pathlib import Path

import pandas as pd

from src.stream_mining.stream_processor import StreamProcessor
from src.stream_mining.stream_mongodb import MongoStreamStore


# =========================================================
# Paths
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    BASE_DIR
    / "data"
    / "processed"
    / "tennis_matches_clean.csv"
)

OUTPUT_FILE = (
    BASE_DIR
    / "data"
    / "processed"
    / "real_atp_stream_results.json"
)


# =========================================================
# Configuration
# =========================================================

WINDOW_SIZE = 100

MONGODB_SOURCE = "historical"


# =========================================================
# Helper Functions
# =========================================================

def safe_value(value):
    """
    Convert pandas NaN values to None so that the
    generated events can be stored cleanly in JSON
    and MongoDB.
    """

    if pd.isna(value):
        return None

    return value


def create_match_events(row, event_number):
    """
    Create two player-level stream events from one
    historical ATP match.

    One event represents the winner.
    One event represents the loser.
    """

    match_id = str(
        safe_value(row.get("match_id"))
        or f"{row.get('tourney_id')}_{row.get('year')}_{event_number}"
    )

    timestamp = safe_value(
        row.get("match_date")
    )

    if timestamp is None:
        timestamp = safe_value(
            row.get("tourney_date")
        )

    # -----------------------------------------------------
    # Common match information
    # -----------------------------------------------------

    common_data = {
        "match_id": match_id,
        "timestamp": timestamp,

        "tourney_id": safe_value(
            row.get("tourney_id")
        ),

        "tourney_name": safe_value(
            row.get("tourney_name")
        ),

        "surface": safe_value(
            row.get("surface")
        ),

        "year": safe_value(
            row.get("year")
        ),

        "round": safe_value(
            row.get("round")
        ),

        "score": safe_value(
            row.get("score")
        ),

        "minutes": safe_value(
            row.get("minutes")
        ),
    }

    # -----------------------------------------------------
    # Winner event
    # -----------------------------------------------------

    winner = safe_value(
        row.get("winner_name")
    )

    winner_id = safe_value(
        row.get("winner_id")
    )

    winner_ranking = safe_value(
        row.get("winner_rank")
    )

    winner_ranking_points = safe_value(
        row.get("winner_rank_points")
    )

    winner_event = {
        **common_data,

        "event_id": (
            f"{match_id}_winner"
        ),

        "player": winner,
        "player_id": winner_id,

        "opponent": safe_value(
            row.get("loser_name")
        ),

        "role": "winner",

        "event_type": "MATCH_WIN",

        "ranking": winner_ranking,

        "ranking_points": winner_ranking_points,

        "aces": safe_value(
            row.get("w_ace")
        ),

        "double_faults": safe_value(
            row.get("w_df")
        ),

        "first_serve_pct": safe_value(
            row.get("w_svpt")
        ),
    }

    # -----------------------------------------------------
    # Loser event
    # -----------------------------------------------------

    loser = safe_value(
        row.get("loser_name")
    )

    loser_id = safe_value(
        row.get("loser_id")
    )

    loser_ranking = safe_value(
        row.get("loser_rank")
    )

    loser_ranking_points = safe_value(
        row.get("loser_rank_points")
    )

    loser_event = {
        **common_data,

        "event_id": (
            f"{match_id}_loser"
        ),

        "player": loser,
        "player_id": loser_id,

        "opponent": winner,

        "role": "loser",

        "event_type": "MATCH_LOSS",

        "ranking": loser_ranking,

        "ranking_points": loser_ranking_points,

        "aces": safe_value(
            row.get("l_ace")
        ),

        "double_faults": safe_value(
            row.get("l_df")
        ),

        "first_serve_pct": safe_value(
            row.get("l_svpt")
        ),
    }

    return [
        winner_event,
        loser_event
    ]


# =========================================================
# Main Historical Stream
# =========================================================

def main():

    print("=" * 60)
    print("HISTORICAL ATP TENNIS STREAM")
    print("=" * 60)

    # -----------------------------------------------------
    # Check input file
    # -----------------------------------------------------

    if not INPUT_FILE.exists():

        print(
            f"\nERROR: Input file not found:\n"
            f"{INPUT_FILE}"
        )

        return

    print(
        f"\nInput file:\n{INPUT_FILE}"
    )

    # -----------------------------------------------------
    # Load historical data
    # -----------------------------------------------------

    df = pd.read_csv(INPUT_FILE)

    print(
        f"\nHistorical matches loaded: "
        f"{len(df)}"
    )

    # -----------------------------------------------------
    # Sort chronologically
    # -----------------------------------------------------

    date_column = None

    for candidate in [
        "match_date",
        "tourney_date",
        "date"
    ]:

        if candidate in df.columns:
            date_column = candidate
            break

    if date_column:

        df[date_column] = pd.to_datetime(
            df[date_column],
            errors="coerce"
        )

        df = df.sort_values(
            by=date_column
        ).reset_index(drop=True)

        print(
            f"Chronological sorting: PASS "
            f"({date_column})"
        )

    else:

        print(
            "Chronological sorting: "
            "date column not found"
        )

    # -----------------------------------------------------
    # Initialize stream processor
    # -----------------------------------------------------

    processor = StreamProcessor(
        window_size=WINDOW_SIZE
    )

    # -----------------------------------------------------
    # Connect MongoDB
    # -----------------------------------------------------

    print("\nConnecting to MongoDB...")

    store = MongoStreamStore()

    print("MongoDB connection: PASS")

    # -----------------------------------------------------
    # Process historical matches
    # -----------------------------------------------------

    all_events = []

    total_matches = 0
    total_events = 0

    print("\nProcessing historical matches...")

    for index, row in df.iterrows():

        total_matches += 1

        events = create_match_events(
            row,
            index
        )

        for event in events:

            # ---------------------------------------------
            # Stream processing
            # ---------------------------------------------

            result = processor.process_event(
                event
            )

            if not result.get("processed"):

                continue

            # ---------------------------------------------
            # Store normalized event in output list
            # ---------------------------------------------

            all_events.append(event)

            total_events += 1

    print(
        f"\nMatches processed : "
        f"{total_matches}"
    )

    print(
        f"Events generated  : "
        f"{total_events}"
    )

    # -----------------------------------------------------
    # Save events to JSON
    # -----------------------------------------------------

    print("\nSaving JSON stream results...")

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            all_events,
            file,
            indent=2,
            default=str
        )

    print(
        f"JSON output saved:\n"
        f"{OUTPUT_FILE}"
    )

    # -----------------------------------------------------
    # Save events to MongoDB
    # -----------------------------------------------------

    print("\nSaving stream events to MongoDB...")

    saved_events = store.save_events(
        all_events,
        source=MONGODB_SOURCE
    )

    print(
        f"MongoDB stream events saved: "
        f"{saved_events}"
    )

    # -----------------------------------------------------
    # Get final stream metrics
    # -----------------------------------------------------

    metrics = processor.get_metrics()

    # -----------------------------------------------------
    # Save metrics to MongoDB
    # -----------------------------------------------------

    print(
        "\nSaving stream metrics to MongoDB..."
    )

    metrics_id = store.save_metrics(
        metrics,
        source=MONGODB_SOURCE
    )

    print(
        f"MongoDB metrics document ID: "
        f"{metrics_id}"
    )

    # -----------------------------------------------------
    # Display final metrics
    # -----------------------------------------------------

    print("\n" + "=" * 60)
    print("FINAL STREAM METRICS")
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
        f"{metrics['duplicate_rate']}"
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

    print(
        f"Configured window size        : "
        f"{metrics['configured_window_size']}"
    )

    print(
        f"Event type counts             : "
        f"{metrics['event_type_counts']}"
    )

    print(
        "\nTop players by events:"
    )

    for player in metrics[
        "top_players_by_events"
    ]:

        print(
            f"  {player['player']}: "
            f"{player['events']}"
        )

    print(
        "\nTop matches by events:"
    )

    for match in metrics[
        "top_matches_by_events"
    ]:

        print(
            f"  {match['match_id']}: "
            f"{match['events']}"
        )

    # -----------------------------------------------------
    # MongoDB verification counts
    # -----------------------------------------------------

    print("\n" + "=" * 60)
    print("MONGODB STREAM STORAGE")
    print("=" * 60)

    print(
        f"stream_events documents : "
        f"{store.get_event_count()}"
    )

    print(
        f"stream_metrics documents: "
        f"{store.get_metrics_count()}"
    )

    # -----------------------------------------------------
    # Close MongoDB
    # -----------------------------------------------------

    store.close()

    print(
        "\nMongoDB connection closed."
    )

    print("\n" + "=" * 60)
    print("HISTORICAL STREAM PROCESSING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()