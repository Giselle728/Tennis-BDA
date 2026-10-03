import os
import subprocess

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


# =========================================================
# Load environment variables
# =========================================================

load_dotenv()

uri = os.getenv("MONGODB_URI")

if not uri:
    raise ValueError("MONGODB_URI not found in .env")


# =========================================================
# HDFS output path
# =========================================================

HDFS_PATH = (
    "/user/giselledmello/tennis/"
    "output/surface_analysis/part-00000"
)


# =========================================================
# Read MapReduce output from HDFS
# =========================================================

def read_hdfs_output():
    """Read MapReduce output from HDFS."""

    result = subprocess.run(
        ["hdfs", "dfs", "-cat", HDFS_PATH],
        capture_output=True,
        text=True,
        check=True
    )

    return result.stdout.strip().splitlines()


# =========================================================
# Parse MapReduce output
# =========================================================

def parse_surface_data(lines):
    """Convert tab-separated MapReduce rows into MongoDB documents."""

    documents = []

    for line in lines:

        if not line.strip():
            continue

        fields = line.split("\t")

        # Expected format:
        # surface
        # match_count
        # avg_aces
        # avg_double_faults
        # avg_first_serve_pct
        # avg_break_point_conversion_pct
        # avg_duration_minutes

        if len(fields) != 7:
            print(f"Skipping unexpected row: {line}")
            continue

        try:

            surface = fields[0]

            document = {
                "surface": surface,
                "match_count": int(fields[1]),
                "avg_aces": float(fields[2]),
                "avg_double_faults": float(fields[3]),
                "avg_first_serve_pct": float(fields[4]),
                "avg_break_point_conversion_pct": float(fields[5]),
                "avg_duration_minutes": float(fields[6]),
            }

            documents.append(document)

        except ValueError as error:

            print(f"Skipping invalid row: {line}")
            print(f"Parsing error: {error}")

    return documents


# =========================================================
# Main function
# =========================================================

def main():

    client = MongoClient(
        uri,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=10000
    )

    try:

        # -------------------------------------------------
        # Confirm MongoDB Atlas connection
        # -------------------------------------------------

        client.admin.command("ping")

        print("MongoDB Atlas connected!")

        # -------------------------------------------------
        # Select database and collection
        # -------------------------------------------------

        db = client["tennis_analytics"]

        collection = db["surface_analysis"]

        # -------------------------------------------------
        # Read and parse HDFS output
        # -------------------------------------------------

        lines = read_hdfs_output()

        documents = parse_surface_data(lines)

        if not documents:

            print("No valid records found.")

            return

        # -------------------------------------------------
        # Upsert surface analysis records
        # -------------------------------------------------

        operations = [

            UpdateOne(
                {"surface": document["surface"]},

                {
                    "$set": document,

                    # Remove the old generic metric fields
                    "$unset": {
                        "metric_1": "",
                        "metric_2": "",
                        "metric_3": "",
                        "metric_4": "",
                        "metric_5": ""
                    }
                },

                upsert=True
            )

            for document in documents

        ]

        result = collection.bulk_write(operations)

        # -------------------------------------------------
        # Display loading results
        # -------------------------------------------------

        print("\nSurface analysis loaded successfully!")

        print(f"Records inserted: {result.upserted_count}")

        print(f"Records matched: {result.matched_count}")

        print(f"Records updated: {result.modified_count}")

        # -------------------------------------------------
        # Display stored documents
        # -------------------------------------------------

        print("\nStored documents:")

        for document in collection.find(
            {},
            {"_id": 0}
        ).sort("surface", 1):

            print(document)

    # -----------------------------------------------------
    # Handle HDFS errors
    # -----------------------------------------------------

    except subprocess.CalledProcessError as error:

        print("HDFS command failed:")

        print(error.stderr)

    # -----------------------------------------------------
    # Handle other errors
    # -----------------------------------------------------

    except Exception as error:

        print("Loading failed:")

        print(error)

    # -----------------------------------------------------
    # Close MongoDB connection
    # -----------------------------------------------------

    finally:

        client.close()


# =========================================================
# Program entry point
# =========================================================

if __name__ == "__main__":

    main()