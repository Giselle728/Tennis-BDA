#!/usr/bin/env python3

import sys

current_surface = None

match_count = 0
aces_sum = 0.0
aces_count = 0

df_sum = 0.0
df_count = 0

first_serve_sum = 0.0
first_serve_count = 0

bp_conversion_sum = 0.0
bp_conversion_count = 0

duration_sum = 0.0
duration_count = 0


def reset():
    global match_count
    global aces_sum, aces_count
    global df_sum, df_count
    global first_serve_sum, first_serve_count
    global bp_conversion_sum, bp_conversion_count
    global duration_sum, duration_count

    match_count = 0

    aces_sum = 0.0
    aces_count = 0

    df_sum = 0.0
    df_count = 0

    first_serve_sum = 0.0
    first_serve_count = 0

    bp_conversion_sum = 0.0
    bp_conversion_count = 0

    duration_sum = 0.0
    duration_count = 0


def emit(surface):
    if surface is None:
        return

    avg_aces = aces_sum / aces_count if aces_count else 0
    avg_df = df_sum / df_count if df_count else 0
    avg_first_serve = (
        first_serve_sum / first_serve_count
        if first_serve_count else 0
    )
    avg_bp_conversion = (
        bp_conversion_sum / bp_conversion_count
        if bp_conversion_count else 0
    )
    avg_duration = (
        duration_sum / duration_count
        if duration_count else 0
    )

    print(
        f"{surface}\t"
        f"{match_count}\t"
        f"{avg_aces:.2f}\t"
        f"{avg_df:.2f}\t"
        f"{avg_first_serve:.2f}\t"
        f"{avg_bp_conversion:.2f}\t"
        f"{avg_duration:.2f}"
    )


reset()

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) != 7:
        continue

    surface = parts[0]

    try:
        count = int(parts[1])
        avg_aces = float(parts[2])
        avg_df = float(parts[3])
        avg_first_serve = float(parts[4])
        avg_bp_conversion = float(parts[5])
        duration = float(parts[6])
    except ValueError:
        continue

    if current_surface != surface:
        if current_surface is not None:
            emit(current_surface)

        current_surface = surface
        reset()

    match_count += count

    if avg_aces >= 0:
        aces_sum += avg_aces
        aces_count += 1

    if avg_df >= 0:
        df_sum += avg_df
        df_count += 1

    if avg_first_serve >= 0:
        first_serve_sum += avg_first_serve
        first_serve_count += 1

    if avg_bp_conversion >= 0:
        bp_conversion_sum += avg_bp_conversion
        bp_conversion_count += 1

    if duration >= 0:
        duration_sum += duration
        duration_count += 1

if current_surface is not None:
    emit(current_surface)
