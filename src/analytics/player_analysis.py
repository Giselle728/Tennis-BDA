import pandas as pd
from pathlib import Path


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

    df = pd.read_csv(DATA_PATH)

    return df


# ============================================================
# SEASON YEAR
# ============================================================

def season_years(frame):
    """
    Return the season year of every match in 'frame'.

    The 'year' column of the cleaned dataset is derived from the
    tournament start date, so the season-opening events that begin
    on 31 December (Brisbane, Doha, Pune) are labelled 2018 even
    though they belong to the 2019 season. The season is read from
    the tournament id, and then the match id, instead, for example
    "2019-M020_271" -> 2019. The dataset's own 'year' column is
    only used for rows whose ids cannot be read: no year is
    hardcoded and no row is invented.
    """

    def leading_year(column):

        return pd.to_numeric(
            frame[column]
            .astype(str)
            .str.extract(r"^\s*(\d{4})", expand=False),
            errors="coerce"
        )

    season = leading_year("tourney_id").fillna(
        leading_year("match_id")
    )

    season = season.fillna(
        pd.to_numeric(frame["year"], errors="coerce")
    )

    # Nullable integers, so a row without any usable year stays
    # missing (and is skipped by the aggregations) instead of
    # silently turning into year 0.
    return season.astype("Int64")


# ============================================================
# PLAYER ANALYSIS
# ============================================================

def get_player_analysis(player_name):
    """
    Generate performance statistics for a selected player.
    """

    df = load_data()

    # --------------------------------------------------------
    # Find matches involving the player
    # --------------------------------------------------------

    player_matches = df[
        (df["winner_name"] == player_name) |
        (df["loser_name"] == player_name)
    ].copy()

    if player_matches.empty:
        return None

    # --------------------------------------------------------
    # Basic match statistics
    # --------------------------------------------------------

    wins = (
        player_matches["winner_name"] == player_name
    ).sum()

    losses = (
        player_matches["loser_name"] == player_name
    ).sum()

    total_matches = wins + losses

    win_rate = (
        wins / total_matches * 100
        if total_matches > 0
        else 0
    )

    # --------------------------------------------------------
    # Player-specific statistics
    # --------------------------------------------------------

    # Values when player is winner
    winner_matches = player_matches[
        player_matches["winner_name"] == player_name
    ]

    # Values when player is loser
    loser_matches = player_matches[
        player_matches["loser_name"] == player_name
    ]

    # --------------------------------------------------------
    # Aces
    # --------------------------------------------------------

    total_aces = (
        winner_matches["w_ace"].sum(skipna=True)
        +
        loser_matches["l_ace"].sum(skipna=True)
    )

    # --------------------------------------------------------
    # Double faults
    # --------------------------------------------------------

    total_double_faults = (
        winner_matches["w_df"].sum(skipna=True)
        +
        loser_matches["l_df"].sum(skipna=True)
    )

    # --------------------------------------------------------
    # First serve percentage
    # --------------------------------------------------------

    serve_percentages = pd.concat([
        winner_matches["w_1st_serve_pct"],
        loser_matches["l_1st_serve_pct"]
    ])

    avg_first_serve_pct = serve_percentages.mean()

    # --------------------------------------------------------
    # First serve points won percentage
    # --------------------------------------------------------

    first_serve_won = pd.concat([
        winner_matches["w_1st_serve_won_pct"],
        loser_matches["l_1st_serve_won_pct"]
    ])

    avg_first_serve_won_pct = first_serve_won.mean()

    # --------------------------------------------------------
    # Break point conversion
    # --------------------------------------------------------

    bp_conversion = pd.concat([
        winner_matches["w_bp_conversion_pct"],
        loser_matches["l_bp_conversion_pct"]
    ])

    avg_bp_conversion = bp_conversion.mean()

    # --------------------------------------------------------
    # Average match duration
    # --------------------------------------------------------

    avg_duration = player_matches["minutes"].mean()

    # --------------------------------------------------------
    # Current / most recent ranking
    # --------------------------------------------------------

    ranking_values = []

    ranking_values.extend(
        winner_matches["winner_rank"].dropna().tolist()
    )

    ranking_values.extend(
        loser_matches["loser_rank"].dropna().tolist()
    )

    latest_rank = None

    if ranking_values:
        latest_rank = ranking_values[-1]

    # --------------------------------------------------------
    # Year-wise performance
    # --------------------------------------------------------

    yearly = (
        player_matches
        .assign(
            season=season_years(player_matches),
            player_won=
            player_matches["winner_name"] == player_name
        )
        .dropna(subset=["season"])
        .groupby("season")
        .agg(
            matches=("match_id", "count"),
            wins=("player_won", "sum")
        )
        .reset_index()
        .rename(columns={"season": "year"})
        .sort_values("year")
        .reset_index(drop=True)
    )

    # Plain ints so the JSON payload and the chart labels stay
    # numeric, oldest season first so the trajectory always reads
    # left to right.
    yearly["year"] = yearly["year"].astype(int)

    yearly["losses"] = (
        yearly["matches"] - yearly["wins"]
    )

    yearly["win_rate"] = (
        yearly["wins"] /
        yearly["matches"] *
        100
    )

    # --------------------------------------------------------
    # Surface-wise performance
    # --------------------------------------------------------

    surface_data = player_matches.dropna(
        subset=["surface"]
    ).copy()

    surface_data["player_won"] = (
        surface_data["winner_name"] == player_name
    )

    surface_stats = (
        surface_data
        .groupby("surface")
        .agg(
            matches=("match_id", "count"),
            wins=("player_won", "sum")
        )
        .reset_index()
    )

    surface_stats["losses"] = (
        surface_stats["matches"] -
        surface_stats["wins"]
    )

    surface_stats["win_rate"] = (
        surface_stats["wins"] /
        surface_stats["matches"] *
        100
    )

    # --------------------------------------------------------
    # Return complete result
    # --------------------------------------------------------

    return {
        "player": player_name,
        "matches": int(total_matches),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate": round(win_rate, 2),

        "total_aces": int(total_aces),
        "total_double_faults": int(total_double_faults),

        "avg_first_serve_pct":
            round(avg_first_serve_pct, 2),

        "avg_first_serve_won_pct":
            round(avg_first_serve_won_pct, 2),

        "avg_break_point_conversion":
            round(avg_bp_conversion, 2),

        "avg_match_duration":
            round(avg_duration, 2),

        "latest_rank": latest_rank,

        "yearly_performance": yearly,

        "surface_performance": surface_stats
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    player = "Novak Djokovic"

    result = get_player_analysis(player)

    if result is None:

        print(f"\nPlayer not found: {player}")

    else:

        print("=" * 70)
        print("🎾 PLAYER PERFORMANCE ANALYSIS")
        print("=" * 70)

        print(f"\nPlayer: {result['player']}")

        print("\n--- Overall Performance ---")

        print(
            f"Matches       : {result['matches']}"
        )

        print(
            f"Wins          : {result['wins']}"
        )

        print(
            f"Losses        : {result['losses']}"
        )

        print(
            f"Win Rate      : {result['win_rate']}%"
        )

        print("\n--- Serve & Performance Statistics ---")

        print(
            f"Total Aces    : {result['total_aces']}"
        )

        print(
            f"Double Faults : {result['total_double_faults']}"
        )

        print(
            f"1st Serve %   : {result['avg_first_serve_pct']}%"
        )

        print(
            f"1st Serve Won : {result['avg_first_serve_won_pct']}%"
        )

        print(
            f"BP Conversion : {result['avg_break_point_conversion']}%"
        )

        print(
            f"Avg Duration  : {result['avg_match_duration']} min"
        )

        print(
            f"Latest Rank   : {result['latest_rank']}"
        )

        print("\n--- Year-wise Performance ---")

        print(
            result["yearly_performance"].to_string(
                index=False
            )
        )

        print("\n--- Surface-wise Performance ---")

        print(
            result["surface_performance"].to_string(
                index=False
            )
        )

        print("\n" + "=" * 70)