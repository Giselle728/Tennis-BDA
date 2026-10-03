
import os
import subprocess

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


# =========================================================
# Load Environment Variables
# =========================================================

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")

if not MONGO_URI:
    print("MONGODB_URI not found in .env file.")
    raise SystemExit(1)


# =========================================================
# MongoDB Atlas Connection
# =========================================================

client = MongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=10000
)

try:
    client.admin.command("ping")
    print("MongoDB Atlas connected!")

except Exception as e:
    print("MongoDB connection failed:", e)
    raise SystemExit(1)


# =========================================================
# Database and Collection
# =========================================================

db = client["tennis_analytics"]
collection = db["tournament_analysis"]


# =========================================================
# Read Tournament Analysis from HDFS
# =========================================================

hdfs_path = (
    "/user/giselledmello/tennis/output/"
    "tournament_analysis/part-00000"
)

try:
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", hdfs_path],
        capture_output=True,
        text=True,
        check=True
    )

    output = result.stdout

except subprocess.CalledProcessError as e:
    print("Failed to read HDFS output:")
    print(e.stderr)
    client.close()
    raise SystemExit(1)


# =========================================================
# Parse Hadoop Output
#
# Format:
# tournament    match_count    avg_duration
# =========================================================

operations = []
valid_records = 0
skipped_records = 0

for line in output.strip().splitlines():

    fields = line.strip().split("\t")

    if len(fields) != 3:
        skipped_records += 1
        continue

    tournament = fields[0].strip()

    try:
        match_count = int(fields[1])
        avg_duration = float(fields[2])

    except ValueError:
        skipped_records += 1
        continue

    if not tournament:
        skipped_records += 1
        continue

    document = {
        "tournament": tournament,
        "match_count": match_count,
        "avg_duration_minutes": avg_duration
    }

    operation = UpdateOne(
        {"tournament": tournament},
        {"$set": document},
        upsert=True
    )

    operations.append(operation)
    valid_records += 1


# =========================================================
# Store Data in MongoDB Atlas
# =========================================================

if operations:

    result = collection.bulk_write(
        operations,
        ordered=False
    )

    print("\nTournament analysis loaded successfully!")

    print("Valid records:", valid_records)
    print("Skipped records:", skipped_records)
    print("Records inserted:", result.upserted_count)
    print("Records matched:", result.matched_count)
    print("Records modified:", result.modified_count)

else:

    print("No valid records found.")
    print("Skipped records:", skipped_records)


# =========================================================
# Display Sample Documents
# =========================================================

print("\nSample stored documents:")

for document in collection.find(
    {},
    {"_id": 0}
).limit(5):

    print(document)


# =========================================================
# Close MongoDB Connection
# =========================================================

client.close()