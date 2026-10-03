#!/usr/bin/env python3

import sys

current_player = None
current_wins = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    player, count = line.split("\t", 1)
    count = int(count)

    if current_player == player:
        current_wins += count
    else:
        if current_player is not None:
            print(f"{current_player}\t{current_wins}")

        current_player = player
        current_wins = count

if current_player is not None:
    print(f"{current_player}\t{current_wins}")
