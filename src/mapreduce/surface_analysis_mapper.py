#!/usr/bin/env python3

import sys
import csv

HEADER = """tourney_id,tourney_name,surface,draw_size,tourney_level,tourney_date,match_num,winner_id,winner_seed,winner_entry,winner_name,winner_hand,winner_ht,winner_ioc,winner_age,loser_id,loser_seed,loser_entry,loser_name,loser_hand,loser_ht,loser_ioc,loser_age,score,best_of,round,minutes,w_ace,w_df,w_svpt,w_1stIn,w_1stWon,w_2ndWon,w_SvGms,w_bpSaved,w_bpFaced,l_ace,l_df,l_svpt,l_1stIn,l_1stWon,l_2ndWon,l_SvGms,l_bpSaved,l_bpFaced,winner_rank,winner_rank_points,loser_rank,loser_rank_points,year,match_id,w_1st_serve_pct,l_1st_serve_pct,w_1st_serve_won_pct,l_1st_serve_won_pct,w_bp_conversion_pct,l_bp_conversion_pct""".split(",")

for line in sys.stdin:
    line = line.rstrip("\n")

    if not line:
        continue

    try:
        row = next(csv.reader([line]))
    except Exception:
        continue

    # Skip the actual CSV header
    if row and row[0] == "tourney_id":
        continue

    # Ignore malformed rows
    if len(row) != len(HEADER):
        continue

    data = dict(zip(HEADER, row))

    surface = data.get("surface", "").strip()

    if not surface:
        continue

    def get_float(column):
        value = data.get(column, "").strip()

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    w_ace = get_float("w_ace")
    l_ace = get_float("l_ace")
    w_df = get_float("w_df")
    l_df = get_float("l_df")

    w_first_serve = get_float("w_1st_serve_pct")
    l_first_serve = get_float("l_1st_serve_pct")

    w_bp = get_float("w_bp_conversion_pct")
    l_bp = get_float("l_bp_conversion_pct")

    minutes = get_float("minutes")

    def avg_pair(a, b):
        values = [x for x in (a, b) if x is not None]
        return sum(values) / len(values) if values else None

    avg_aces = avg_pair(w_ace, l_ace)
    avg_df = avg_pair(w_df, l_df)
    avg_first_serve = avg_pair(w_first_serve, l_first_serve)
    avg_bp_conversion = avg_pair(w_bp, l_bp)

    values = [
        1,
        avg_aces if avg_aces is not None else -1,
        avg_df if avg_df is not None else -1,
        avg_first_serve if avg_first_serve is not None else -1,
        avg_bp_conversion if avg_bp_conversion is not None else -1,
        minutes if minutes is not None else -1
    ]

    print(f"{surface}\t" + "\t".join(map(str, values)))
