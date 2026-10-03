import pandas as pd
from pathlib import Path

# The season a match belongs to is defined once, by the players
# analytics module, and reused here so head-to-head records agree with
# the Players page and the Matches page.
try:
    from src.analytics.player_analysis import season_years
except ImportError:  # direct execution: python src/analytics/head_to_head.py
    from player_analysis import season_years


# ============================================================
# CONFIGURATION
# ============================================================

DATA_PATH = Path("data/processed/tennis_matches_clean.csv")


# ============================================================
# LOAD DATA
# ============================================================

def load_data():
    """Load the cleaned tennis dataset."""

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {DATA_PATH}"
        )

    return pd.read_csv(DATA_PATH)


# ============================================================
# HEAD-TO-HEAD ANALYSIS
# ============================================================

def get_head_to_head(player_a, player_b):

    df = load_data()

    # The dataset's 'year' column follows the tournament start date, so
    # the matches that open the 2019 season (played on 2018-12-31) are
    # recorded as 2018. season_years() maps them onto the 2019 season,
    # so a head-to-head is counted per real season (2019-2024).
    df["year"] = season_years(df)

    # --------------------------------------------------------
    # Find matches between the two players
    # --------------------------------------------------------

    h2h = df[
        (
            (df["winner_name"] == player_a) &
            (df["loser_name"] == player_b)
        )
        |
        (
            (df["winner_name"] == player_b) &
            (df["loser_name"] == player_a)
        )
    ].copy()

    # --------------------------------------------------------
    # Check whether players have played each other
    # --------------------------------------------------------

    if h2h.empty:
        return None

    # --------------------------------------------------------
    # Determine winner from Player A's perspective
    # --------------------------------------------------------

    h2h["player_a_won"] = (
        h2h["winner_name"] == player_a
    )

    # --------------------------------------------------------
    # Overall statistics
    # --------------------------------------------------------

    total_matches = len(h2h)

    player_a_wins = int(
        h2h["player_a_won"].sum()
    )

    player_b_wins = (
        total_matches - player_a_wins
    )

    player_a_win_rate = (
        player_a_wins / total_matches * 100
        if total_matches > 0
        else 0
    )

    player_b_win_rate = (
        player_b_wins / total_matches * 100
        if total_matches > 0
        else 0
    )

    # --------------------------------------------------------
    # Surface-wise H2H
    # --------------------------------------------------------

    surface_h2h = (
        h2h
        .dropna(subset=["surface"])
        .groupby("surface")
        .agg(
            matches=("match_id", "count"),
            player_a_wins=("player_a_won", "sum")
        )
        .reset_index()
    )

    surface_h2h["player_b_wins"] = (
        surface_h2h["matches"]
        -
        surface_h2h["player_a_wins"]
    )

    surface_h2h["player_a_win_rate"] = (
        surface_h2h["player_a_wins"]
        /
        surface_h2h["matches"]
        *
        100
    )

    surface_h2h["player_b_win_rate"] = (
        surface_h2h["player_b_wins"]
        /
        surface_h2h["matches"]
        *
        100
    )

    surface_h2h["player_a_win_rate"] = (
        surface_h2h["player_a_win_rate"].round(2)
    )

    surface_h2h["player_b_win_rate"] = (
        surface_h2h["player_b_win_rate"].round(2)
    )

    # --------------------------------------------------------
    # Year-wise H2H
    # --------------------------------------------------------

    yearly_h2h = (
        h2h
        .groupby("year")
        .agg(
            matches=("match_id", "count"),
            player_a_wins=("player_a_won", "sum")
        )
        .reset_index()
    )

    yearly_h2h["player_b_wins"] = (
        yearly_h2h["matches"]
        -
        yearly_h2h["player_a_wins"]
    )

    yearly_h2h["player_a_win_rate"] = (
        yearly_h2h["player_a_wins"]
        /
        yearly_h2h["matches"]
        *
        100
    )

    yearly_h2h["player_b_win_rate"] = (
        yearly_h2h["player_b_wins"]
        /
        yearly_h2h["matches"]
        *
        100
    )

    yearly_h2h["player_a_win_rate"] = (
        yearly_h2h["player_a_win_rate"].round(2)
    )

    yearly_h2h["player_b_win_rate"] = (
        yearly_h2h["player_b_win_rate"].round(2)
    )

    # --------------------------------------------------------
    # Match history
    # --------------------------------------------------------

    match_history = h2h[
        [
            "match_id",
            "tourney_date",
            "year",
            "tourney_name",
            "surface",
            "round",
            "winner_name",
            "loser_name",
            "score",
            "minutes"
        ]
    ].copy()

    # Sort newest first, so the most recent meeting leads the list
    match_history = match_history.sort_values(
        by=["year", "match_id"],
        ascending=False
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # Return results
    # --------------------------------------------------------

    return {
        "player_a": player_a,
        "player_b": player_b,

        "total_matches": total_matches,

        "player_a_wins": player_a_wins,
        "player_b_wins": player_b_wins,

        "player_a_win_rate":
            round(player_a_win_rate, 2),

        "player_b_win_rate":
            round(player_b_win_rate, 2),

        "surface_h2h": surface_h2h,

        "yearly_h2h": yearly_h2h,

        "match_history": match_history
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    # Change these two players for testing
    player_a = "Novak Djokovic"
    player_b = "Daniil Medvedev"

    result = get_head_to_head(
        player_a,
        player_b
    )

    print("=" * 70)
    print("🎾 HEAD-TO-HEAD ANALYSIS")
    print("=" * 70)

    if result is None:

        print(
            f"\nNo matches found between "
            f"{player_a} and {player_b}."
        )

    else:

        print(
            f"\n{result['player_a']} "
            f"vs "
            f"{result['player_b']}"
        )

        # ----------------------------------------------------
        # Overall
        # ----------------------------------------------------

        print("\n--- Overall Head-to-Head ---")

        print(
            f"Total Matches : "
            f"{result['total_matches']}"
        )

        print(
            f"{result['player_a']} Wins : "
            f"{result['player_a_wins']}"
        )

        print(
            f"{result['player_b']} Wins : "
            f"{result['player_b_wins']}"
        )

        print(
            f"{result['player_a']} Win Rate : "
            f"{result['player_a_win_rate']}%"
        )

        print(
            f"{result['player_b']} Win Rate : "
            f"{result['player_b_win_rate']}%"
        )

        # ----------------------------------------------------
        # Surface
        # ----------------------------------------------------

        print("\n--- Surface-wise Head-to-Head ---")

        print(
            result["surface_h2h"].to_string(
                index=False
            )
        )

        # ----------------------------------------------------
        # Year
        # ----------------------------------------------------

        print("\n--- Year-wise Head-to-Head ---")

        print(
            result["yearly_h2h"].to_string(
                index=False
            )
        )

        # ----------------------------------------------------
        # Match History
        # ----------------------------------------------------

        print("\n--- Match History ---")

        print(
            result["match_history"].to_string(
                index=False
            )
        )

    print("\n" + "=" * 70)
    print("✅ HEAD-TO-HEAD ANALYSIS COMPLETE")
    print("=" * 70)