import csv
import sys


def parse_duration(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return -1


def main():
    reader = csv.reader(sys.stdin)

    for row in reader:
        # Skip empty or malformed rows
        if len(row) <= 26:
            continue

        # Skip the CSV header
        if row[0].strip() == "tourney_id":
            continue

        # Fixed column positions from the CSV header
        tournament = row[1].strip()
        duration = parse_duration(row[26].strip())

        # Skip rows without a tournament name
        if not tournament:
            continue

        print(f"{tournament}\t1\t{duration}")


if __name__ == "__main__":
    main()