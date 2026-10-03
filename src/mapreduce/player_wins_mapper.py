#!/usr/bin/env python3

import sys
import csv

reader = csv.DictReader(sys.stdin)

for row in reader:
    winner = row.get("winner_name", "").strip()

    if winner:
        print(f"{winner}\t1")
