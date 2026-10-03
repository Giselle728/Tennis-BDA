import pandas as pd
from pathlib import Path


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Find ATP match files
# ---------------------------------------------------------
match_files = sorted(RAW_DIR.glob("atp_matches_*.csv"))

if not match_files:
    print("❌ No ATP match CSV files found in data/raw/")
    exit()

print("=" * 70)
print("🎾 TENNIS BDA DATA CLEANING")
print("=" * 70)

print("\nFiles found:")
for file in match_files:
    print(f"  ✓ {file.name}")


# ---------------------------------------------------------
# Load and combine datasets
# ---------------------------------------------------------
dataframes = []

for file in match_files:
    try:
        df_temp = pd.read_csv(file)
        dataframes.append(df_temp)
        print(f"\nLoaded {file.name}: {len(df_temp):,} rows")

    except Exception as e:
        print(f"❌ Error reading {file.name}: {e}")


if not dataframes:
    print("\n❌ No datasets could be loaded.")
    exit()


df = pd.concat(dataframes, ignore_index=True)

print("\n" + "=" * 70)
print("1. ORIGINAL DATA")
print("=" * 70)

print(f"Rows    : {len(df):,}")
print(f"Columns : {len(df.columns)}")


# ---------------------------------------------------------
# Remove exact duplicate rows
# ---------------------------------------------------------
before = len(df)

df = df.drop_duplicates()

duplicates_removed = before - len(df)

print("\n" + "=" * 70)
print("2. DUPLICATES")
print("=" * 70)

print(f"Duplicates removed: {duplicates_removed:,}")


# ---------------------------------------------------------
# Convert tournament date
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("3. DATE CLEANING")
print("=" * 70)

df["tourney_date"] = pd.to_datetime(
    df["tourney_date"].astype(str),
    format="%Y%m%d",
    errors="coerce"
)

invalid_dates = df["tourney_date"].isna().sum()

print(f"Invalid dates: {invalid_dates:,}")


# ---------------------------------------------------------
# Create year column
# ---------------------------------------------------------
df["year"] = df["tourney_date"].dt.year

print(
    f"Year range: "
    f"{df['year'].min():.0f} - {df['year'].max():.0f}"
)


# ---------------------------------------------------------
# Standardize text columns
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("4. TEXT STANDARDIZATION")
print("=" * 70)

text_columns = [
    "tourney_id",
    "tourney_name",
    "surface",
    "tourney_level",
    "winner_name",
    "winner_hand",
    "winner_ioc",
    "loser_name",
    "loser_hand",
    "loser_ioc",
    "score",
    "round"
]

for column in text_columns:
    if column in df.columns:
        df[column] = df[column].astype("string").str.strip()


print(f"Standardized {len(text_columns)} text columns.")


# ---------------------------------------------------------
# Standardize surface
# ---------------------------------------------------------
if "surface" in df.columns:
    df["surface"] = df["surface"].str.title()

    valid_surfaces = ["Hard", "Clay", "Grass"]

    invalid_surface_mask = (
        df["surface"].notna()
        & ~df["surface"].isin(valid_surfaces)
    )

    invalid_surfaces = invalid_surface_mask.sum()

    df.loc[invalid_surface_mask, "surface"] = pd.NA

    print(f"Invalid surface values replaced: {invalid_surfaces:,}")


# ---------------------------------------------------------
# Numeric columns
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("5. NUMERIC CONVERSION")
print("=" * 70)

numeric_columns = [
    "draw_size",
    "match_num",
    "winner_id",
    "winner_seed",
    "winner_ht",
    "winner_age",
    "loser_id",
    "loser_seed",
    "loser_ht",
    "loser_age",
    "best_of",
    "minutes",
    "w_ace",
    "w_df",
    "w_svpt",
    "w_1stIn",
    "w_1stWon",
    "w_2ndWon",
    "w_SvGms",
    "w_bpSaved",
    "w_bpFaced",
    "l_ace",
    "l_df",
    "l_svpt",
    "l_1stIn",
    "l_1stWon",
    "l_2ndWon",
    "l_SvGms",
    "l_bpSaved",
    "l_bpFaced",
    "winner_rank",
    "winner_rank_points",
    "loser_rank",
    "loser_rank_points"
]

for column in numeric_columns:
    if column in df.columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

print(f"Processed {len(numeric_columns)} numeric columns.")


# ---------------------------------------------------------
# Create unique match ID
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("6. MATCH ID")
print("=" * 70)

df["match_id"] = (
    df["tourney_id"].astype("string")
    + "_"
    + df["match_num"].astype("Int64").astype("string")
)

print("Created unique match_id column.")


# ---------------------------------------------------------
# Create useful derived statistics
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("7. DERIVED STATISTICS")
print("=" * 70)


# Winner first-serve percentage
df["w_1st_serve_pct"] = (
    df["w_1stIn"] / df["w_svpt"] * 100
)

# Loser first-serve percentage
df["l_1st_serve_pct"] = (
    df["l_1stIn"] / df["l_svpt"] * 100
)

# Winner first-serve points won percentage
df["w_1st_serve_won_pct"] = (
    df["w_1stWon"] / df["w_1stIn"] * 100
)

# Loser first-serve points won percentage
df["l_1st_serve_won_pct"] = (
    df["l_1stWon"] / df["l_1stIn"] * 100
)

# Winner break-point conversion
df["w_bp_conversion_pct"] = (
    (df["w_bpFaced"] - df["w_bpSaved"])
    / df["w_bpFaced"]
    * 100
)

# Loser break-point conversion
df["l_bp_conversion_pct"] = (
    (df["l_bpFaced"] - df["l_bpSaved"])
    / df["l_bpFaced"]
    * 100
)

print("Created:")
print("  ✓ First serve percentage")
print("  ✓ First serve points won percentage")
print("  ✓ Break-point conversion percentage")


# ---------------------------------------------------------
# Remove impossible derived values
# ---------------------------------------------------------
percentage_columns = [
    "w_1st_serve_pct",
    "l_1st_serve_pct",
    "w_1st_serve_won_pct",
    "l_1st_serve_won_pct",
    "w_bp_conversion_pct",
    "l_bp_conversion_pct"
]

for column in percentage_columns:
    df.loc[
        (df[column] < 0) | (df[column] > 100),
        column
    ] = pd.NA


# ---------------------------------------------------------
# Handle missing surface
# ---------------------------------------------------------
# We keep these matches in the main dataset.
# Surface-specific analysis will ignore missing surfaces.
surface_missing = df["surface"].isna().sum()

print(f"\nMatches with missing surface: {surface_missing:,}")
print("These matches are retained.")


# ---------------------------------------------------------
# Sort data
# ---------------------------------------------------------
df = df.sort_values(
    ["tourney_date", "tourney_name", "match_num"]
).reset_index(drop=True)


# ---------------------------------------------------------
# Save processed dataset
# ---------------------------------------------------------
output_file = PROCESSED_DIR / "tennis_matches_clean.csv"

df.to_csv(
    output_file,
    index=False
)


# ---------------------------------------------------------
# Final summary
# ---------------------------------------------------------
print("\n" + "=" * 70)
print("✅ CLEANING COMPLETE")
print("=" * 70)

print(f"""
Processed dataset:

  Rows       : {len(df):,}
  Columns    : {len(df.columns)}
  Players    : {len(set(df["winner_name"].dropna()) | set(df["loser_name"].dropna())):,}
  Tournaments: {df["tourney_name"].nunique():,}

Output:
  {output_file}

Next step:
  → Inspect the processed dataset
  → Begin basic tennis analytics
  → Prepare data for HDFS
""")