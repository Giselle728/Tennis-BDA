import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# File path
# ---------------------------------------------------------
INPUT_FILE = Path("data/processed/tennis_matches_clean.csv")


# ---------------------------------------------------------
# Check file exists
# ---------------------------------------------------------
if not INPUT_FILE.exists():
    print(f"❌ File not found: {INPUT_FILE}")
    print("Run clean_data.py first.")
    exit()


# ---------------------------------------------------------
# Load cleaned dataset
# ---------------------------------------------------------
df = pd.read_csv(INPUT_FILE)


print("=" * 70)
print("🎾 CLEANED TENNIS DATASET PROFILE")
print("=" * 70)


# ---------------------------------------------------------
# 1. Dataset size
# ---------------------------------------------------------
print("\n1. DATASET SIZE")
print("-" * 70)

print(f"Rows    : {len(df):,}")
print(f"Columns : {len(df.columns):,}")


# ---------------------------------------------------------
# 2. Columns
# ---------------------------------------------------------
print("\n2. COLUMNS")
print("-" * 70)

for i, column in enumerate(df.columns, start=1):
    print(f"{i:2}. {column}")


# ---------------------------------------------------------
# 3. Years
# ---------------------------------------------------------
print("\n3. MATCHES BY YEAR")
print("-" * 70)

print(
    df["year"]
    .value_counts()
    .sort_index()
)


# ---------------------------------------------------------
# 4. Surfaces
# ---------------------------------------------------------
print("\n4. SURFACE DISTRIBUTION")
print("-" * 70)

print(
    df["surface"]
    .value_counts(dropna=False)
)


# ---------------------------------------------------------
# 5. Tournament levels
# ---------------------------------------------------------
print("\n5. TOURNAMENT LEVEL")
print("-" * 70)

print(
    df["tourney_level"]
    .value_counts(dropna=False)
)


# ---------------------------------------------------------
# 6. Match rounds
# ---------------------------------------------------------
print("\n6. MATCH ROUND")
print("-" * 70)

print(
    df["round"]
    .value_counts(dropna=False)
)


# ---------------------------------------------------------
# 7. Missing values
# ---------------------------------------------------------
print("\n7. TOP MISSING VALUES")
print("-" * 70)

missing = (
    df.isna()
    .sum()
    .sort_values(ascending=False)
)

missing = missing[missing > 0]

if len(missing) == 0:
    print("No missing values.")
else:
    print(missing.head(15))


# ---------------------------------------------------------
# 8. Important statistics
# ---------------------------------------------------------
print("\n8. IMPORTANT STATISTICS")
print("-" * 70)

important_columns = [
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

print(
    df[important_columns]
    .describe()
    .round(2)
)


# ---------------------------------------------------------
# 9. Unique players
# ---------------------------------------------------------
print("\n9. PLAYER COUNT")
print("-" * 70)

winner_players = set(df["winner_name"].dropna())
loser_players = set(df["loser_name"].dropna())

all_players = winner_players | loser_players

print(f"Unique players: {len(all_players):,}")


# ---------------------------------------------------------
# 10. Most active players
# ---------------------------------------------------------
print("\n10. MOST ACTIVE PLAYERS")
print("-" * 70)

winner_counts = df["winner_name"].value_counts()
loser_counts = df["loser_name"].value_counts()

match_counts = (
    winner_counts
    .add(loser_counts, fill_value=0)
    .sort_values(ascending=False)
)

print(match_counts.head(10))


# ---------------------------------------------------------
# 11. Most match wins
# ---------------------------------------------------------
print("\n11. MOST MATCH WINS")
print("-" * 70)

print(
    df["winner_name"]
    .value_counts()
    .head(10)
)


# ---------------------------------------------------------
# 12. Match ID validation
# ---------------------------------------------------------
print("\n12. MATCH ID VALIDATION")
print("-" * 70)

duplicate_match_ids = df["match_id"].duplicated().sum()

print(f"Duplicate match IDs: {duplicate_match_ids:,}")


# ---------------------------------------------------------
# 13. Cleaned dataset sample
# ---------------------------------------------------------
print("\n13. SAMPLE CLEANED RECORD")
print("-" * 70)

sample_columns = [
    "match_id",
    "tourney_name",
    "surface",
    "year",
    "winner_name",
    "loser_name",
    "w_ace",
    "l_ace",
    "w_1st_serve_pct",
    "l_1st_serve_pct",
    "w_bp_conversion_pct",
    "l_bp_conversion_pct"
]

print(
    df[sample_columns]
    .head()
    .to_string(index=False)
)


# ---------------------------------------------------------
# Final
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("✅ CLEANED DATASET PROFILE COMPLETE")
print("=" * 70)

print("\nDataset:")
print(f"  Matches       : {len(df):,}")
print(f"  Columns       : {len(df.columns):,}")
print(f"  Players       : {len(all_players):,}")
print(f"  Tournaments   : {df['tourney_name'].nunique():,}")

print("\nSource:")
print(f"  {INPUT_FILE}")

print("\nNext step:")
print("  → Begin basic tennis analytics")
print("  → Then prepare the dataset for HDFS")