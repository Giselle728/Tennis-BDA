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

    return pd.read_csv(DATA_PATH)


# ============================================================
# SURFACE ANALYSIS
# ============================================================

def get_surface_analysis():

    df = load_data()

    # Ignore matches where surface is missing
    surface_df = df.dropna(
        subset=["surface"]
    ).copy()

    # ========================================================
    # BASIC SURFACE STATISTICS
    # ========================================================

    surface_summary = (
        surface_df
        .groupby("surface")
        .agg(
            matches=("match_id", "count"),

            avg_aces_winner=("w_ace", "mean"),
            avg_aces_loser=("l_ace", "mean"),

            avg_double_faults_winner=("w_df", "mean"),
            avg_double_faults_loser=("l_df", "mean"),

            avg_first_serve_winner=(
                "w_1st_serve_pct",
                "mean"
            ),

            avg_first_serve_loser=(
                "l_1st_serve_pct",
                "mean"
            ),

            avg_bp_conversion_winner=(
                "w_bp_conversion_pct",
                "mean"
            ),

            avg_bp_conversion_loser=(
                "l_bp_conversion_pct",
                "mean"
            ),

            avg_duration=("minutes", "mean")
        )
        .reset_index()
    )

    # ========================================================
    # COMBINED AVERAGES
    # ========================================================

    surface_summary["avg_aces"] = (
        surface_summary["avg_aces_winner"]
        +
        surface_summary["avg_aces_loser"]
    ) / 2

    surface_summary["avg_double_faults"] = (
        surface_summary["avg_double_faults_winner"]
        +
        surface_summary["avg_double_faults_loser"]
    ) / 2

    surface_summary["avg_first_serve_pct"] = (
        surface_summary["avg_first_serve_winner"]
        +
        surface_summary["avg_first_serve_loser"]
    ) / 2

    surface_summary["avg_bp_conversion"] = (
        surface_summary["avg_bp_conversion_winner"]
        +
        surface_summary["avg_bp_conversion_loser"]
    ) / 2

    # ========================================================
    # ROUND VALUES
    # ========================================================

    numeric_columns = [
        "avg_aces",
        "avg_double_faults",
        "avg_first_serve_pct",
        "avg_bp_conversion",
        "avg_duration"
    ]

    for column in numeric_columns:
        surface_summary[column] = (
            surface_summary[column].round(2)
        )

    # ========================================================
    # YEAR-WISE SURFACE ANALYSIS
    # ========================================================

    # Total aces in each match
    surface_df["total_aces"] = (
        surface_df["w_ace"] +
        surface_df["l_ace"]
    )

    yearly_surface = (
        surface_df
        .groupby(["year", "surface"])
        .agg(
            matches=("match_id", "count"),
            avg_duration=("minutes", "mean"),
            avg_aces=("total_aces", "mean")
        )
        .reset_index()
    )

    yearly_surface["avg_duration"] = (
        yearly_surface["avg_duration"].round(2)
    )

    yearly_surface["avg_aces"] = (
        yearly_surface["avg_aces"].round(2)
    )

    # ========================================================
    # RETURN RESULTS
    # ========================================================

    return {
        "surface_summary": surface_summary,
        "yearly_surface": yearly_surface
    }


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":

    result = get_surface_analysis()

    print("=" * 70)
    print("🎾 TENNIS SURFACE ANALYSIS")
    print("=" * 70)

    print("\n--- Overall Surface Statistics ---")

    print(
        result["surface_summary"].to_string(
            index=False
        )
    )

    print("\n--- Year-wise Surface Statistics ---")

    print(
        result["yearly_surface"].to_string(
            index=False
        )
    )

    print("\n" + "=" * 70)
    print("✅ SURFACE ANALYSIS COMPLETE")
    print("=" * 70)