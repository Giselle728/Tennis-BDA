import pandas as pd
from pathlib import Path

# The season a match belongs to is defined once, by the players
# analytics module, and reused here so the Matches page and the
# Players page can never disagree about it.
try:
    from src.analytics.player_analysis import season_years
except ImportError:  # direct execution: python src/analytics/match_analysis.py
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
# MATCH ANALYSIS
# ============================================================

def get_match_analysis(
    match_id=None,
    year=None,
    tournament=None,
    surface=None,
    player=None
):
    """
    Analyze and filter tennis matches.

    Parameters:
        match_id    : Specific match ID
        year        : Match season (2019-2024)
        tournament  : Tournament name
        surface     : Hard / Clay / Grass
        player      : Player name

    Returns:
        Filtered match data with useful statistics.
    """

    df = load_data()

    # The dataset's 'year' column follows the tournament start date,
    # so the 85 matches that open the 2019 season (Brisbane, Doha,
    # Pune, all played on 2018-12-31) are recorded as 2018. The
    # season is therefore taken from season_years(), the same rule
    # the Players page uses, before anything is filtered, sorted or
    # returned: the project covers the 2019-2024 seasons and the 2019
    # season includes those 85 rows. No row is dropped or rewritten
    # in the dataset.
    df["year"] = season_years(df)

    # --------------------------------------------------------
    # Apply filters
    # --------------------------------------------------------

    filtered = df.copy()

    if match_id is not None:
        filtered = filtered[
            filtered["match_id"] == match_id
        ]

    if year is not None:
        filtered = filtered[
            filtered["year"] == year
        ]

    if tournament is not None:
        filtered = filtered[
            filtered["tourney_name"].str.lower()
            ==
            tournament.lower()
        ]

    if surface is not None:
        filtered = filtered[
            filtered["surface"].str.lower()
            ==
            surface.lower()
        ]

    if player is not None:
        filtered = filtered[
            (filtered["winner_name"] == player)
            |
            (filtered["loser_name"] == player)
        ]

    # --------------------------------------------------------
    # Select useful columns
    # --------------------------------------------------------

    columns = [
        "match_id",
        "year",
        "tourney_name",
        "surface",
        "round",
        "winner_name",
        "loser_name",
        "score",
        "minutes",

        "w_ace",
        "l_ace",

        "w_df",
        "l_df",

        "w_1st_serve_pct",
        "l_1st_serve_pct",

        "w_1st_serve_won_pct",
        "l_1st_serve_won_pct",

        "w_bp_conversion_pct",
        "l_bp_conversion_pct"
    ]

    result = filtered[columns].copy()

    # --------------------------------------------------------
    # Sort results
    # --------------------------------------------------------

    result = result.sort_values(
        by=["year", "match_id"]
    ).reset_index(drop=True)

    return result


# ============================================================
# SINGLE MATCH DETAILS
# ============================================================

def get_match_details(match_id):
    """
    Return detailed statistics for one match.
    """

    df = load_data()

    match = df[
        df["match_id"] == match_id
    ].copy()

    if match.empty:
        return None

    row = match.iloc[0]

    # Same season rule as get_match_analysis(), so the detail view of a
    # match agrees with the season the table lists it under.
    season = season_years(match).iloc[0]

    return {
        "match_id": row["match_id"],
        "year": int(season) if pd.notna(season) else None,
        "tournament": row["tourney_name"],
        "surface": row["surface"],
        "round": row["round"],

        "winner": row["winner_name"],
        "loser": row["loser_name"],

        "score": row["score"],
        "duration": row["minutes"],

        "winner_aces": row["w_ace"],
        "loser_aces": row["l_ace"],

        "winner_double_faults": row["w_df"],
        "loser_double_faults": row["l_df"],

        "winner_first_serve_pct":
            row["w_1st_serve_pct"],

        "loser_first_serve_pct":
            row["l_1st_serve_pct"],

        "winner_first_serve_won_pct":
            row["w_1st_serve_won_pct"],

        "loser_first_serve_won_pct":
            row["l_1st_serve_won_pct"],

        "winner_bp_conversion":
            row["w_bp_conversion_pct"],

        "loser_bp_conversion":
            row["l_bp_conversion_pct"]
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    print("=" * 70)
    print("🎾 TENNIS MATCH ANALYSIS")
    print("=" * 70)

    # --------------------------------------------------------
    # Test 1: Find matches involving a player
    # --------------------------------------------------------

    player = "Novak Djokovic"

    print(
        f"\n--- Matches involving {player} ---"
    )

    player_matches = get_match_analysis(
        player=player
    )

    print(
        player_matches.head(10).to_string(
            index=False
        )
    )

    print(
        f"\nTotal matches found: "
        f"{len(player_matches)}"
    )

    # --------------------------------------------------------
    # Test 2: Filter by surface
    # --------------------------------------------------------

    print("\n--- Djokovic Hard Court Matches ---")

    hard_matches = get_match_analysis(
        player=player,
        surface="Hard"
    )

    print(
        hard_matches.head(10).to_string(
            index=False
        )
    )

    print(
        f"\nHard court matches: "
        f"{len(hard_matches)}"
    )

    # --------------------------------------------------------
    # Test 3: Filter by year
    # --------------------------------------------------------

    print("\n--- Djokovic 2023 Matches ---")

    matches_2023 = get_match_analysis(
        player=player,
        year=2023
    )

    print(
        matches_2023.head(10).to_string(
            index=False
        )
    )

    print(
        f"\n2023 matches: "
        f"{len(matches_2023)}"
    )

    # --------------------------------------------------------
    # Test 4: Detailed match
    # --------------------------------------------------------

    test_match_id = "2023-560_226"

    print(
        f"\n--- Detailed Match: "
        f"{test_match_id} ---"
    )

    details = get_match_details(
        test_match_id
    )

    if details is None:

        print("Match not found.")

    else:

        for key, value in details.items():

            print(
                f"{key:35}: {value}"
            )

    print("\n" + "=" * 70)
    print("✅ MATCH ANALYSIS COMPLETE")
    print("=" * 70)