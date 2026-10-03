import sys


current_tournament = None

match_count = 0
duration_sum = 0.0
duration_count = 0


def emit_result(tournament):
    """Print aggregated statistics for a tournament."""

    global match_count
    global duration_sum
    global duration_count

    if tournament is None:
        return

    if duration_count > 0:
        avg_duration = duration_sum / duration_count
    else:
        avg_duration = 0.0

    print(
        f"{tournament}\t"
        f"{match_count}\t"
        f"{avg_duration:.2f}"
    )


for line in sys.stdin:

    line = line.strip()

    if not line:
        continue

    fields = line.split("\t")

    if len(fields) != 3:
        continue

    tournament = fields[0]

    try:

        count = int(fields[1])
        duration = float(fields[2])

    except ValueError:

        continue

    # When the tournament changes, emit the previous result.
    if (
        current_tournament is not None
        and tournament != current_tournament
    ):

        emit_result(current_tournament)

        match_count = 0
        duration_sum = 0.0
        duration_count = 0

    current_tournament = tournament

    match_count += count

    # Ignore missing duration values represented by -1.
    if duration >= 0:

        duration_sum += duration
        duration_count += 1


# Emit the final tournament.
if current_tournament is not None:

    emit_result(current_tournament)