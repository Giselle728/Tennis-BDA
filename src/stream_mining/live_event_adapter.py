"""
Convert Live Tennis API match data into the common
stream event format used by StreamProcessor.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def create_live_events(match: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Convert one live tennis match into player-level stream events.

    One match produces up to two events:
    - one event for player 1
    - one event for player 2

    Both events contain the same match/score information,
    but each has its own 'player' field so that the existing
    StreamProcessor can track players correctly.
    """

    players = match.get("players") or {}

    player_1 = players.get("p1") or {}
    player_2 = players.get("p2") or {}

    score = match.get("score") or {}

    match_id = str(match.get("id"))

    sequence = score.get("sequence", 0)

    event_timestamp = (
        score.get("timestamp")
        or match.get("live_at")
        or datetime.now(timezone.utc).isoformat()
    )

    common_data = {
        "timestamp": event_timestamp,

        "match_id": match_id,

        "event_type": "LIVE_MATCH_UPDATE",

        "status": match.get("status"),
        "tour": match.get("tour"),

        "tournament": match.get("tournament"),
        "tournament_id": match.get("tournament_id"),

        "surface": match.get("surface"),

        "round": match.get("round"),
        "round_code": match.get("round_code"),

        "format": match.get("format"),

        # Player 1 information
        "player_1": player_1.get("name"),
        "player_1_id": player_1.get("id"),
        "player_1_ranking": player_1.get("ranking"),

        # Player 2 information
        "player_2": player_2.get("name"),
        "player_2_id": player_2.get("id"),
        "player_2_ranking": player_2.get("ranking"),

        # Live score information
        "sets": score.get("sets"),
        "games": score.get("games"),
        "points": score.get("points"),

        "server": score.get("server"),
        "is_tiebreak": score.get("is_tiebreak"),

        "sequence": sequence,

        "score_timestamp": score.get("timestamp"),

        "stale": score.get("stale"),
        "observed_age_seconds": score.get(
            "observed_age_seconds"
        ),
    }

    events = []

    # ---------------------------------------------------------
    # Player 1 event
    # ---------------------------------------------------------

    if player_1.get("name"):

        events.append(
            {
                **common_data,

                "event_id": (
                    f"live_{match_id}_{sequence}_p1"
                ),

                "player": player_1.get("name"),
                "player_id": player_1.get("id"),
                "opponent": player_2.get("name"),
            }
        )

    # ---------------------------------------------------------
    # Player 2 event
    # ---------------------------------------------------------

    if player_2.get("name"):

        events.append(
            {
                **common_data,

                "event_id": (
                    f"live_{match_id}_{sequence}_p2"
                ),

                "player": player_2.get("name"),
                "player_id": player_2.get("id"),
                "opponent": player_1.get("name"),
            }
        )

    return events