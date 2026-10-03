
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
    "output/player_wins/part-00000"
)


# =========================================================
# Read MapReduce output from HDFS
# =========================================================

def read_hdfs_output():
    """Read Player Wins MapReduce output from HDFS."""

    result = subprocess.run(
        ["hdfs", "dfs", "-cat", HDFS_PATH],
        capture_output=True,
        text=True,
        check=True
    )

    return result.stdout.strip().splitlines()


# =========================================================
# Parse Player Wins output
# =========================================================

def parse_player_wins(lines):
    """Convert tab-separated rows into MongoDB documents."""

    documents = []

    for line in lines:

        if not line.strip():
            continue

        # Split only at the tab separator.
        # This preserves spaces inside player names.
        fields = line.split("\t")

        if len(fields) != 2:
            print(f"Skipping unexpected row: {line}")
            continue

        try:

            player_name = fields[0].strip()
            win_count = int(fields[1].strip())

            if not player_name:
                print(f"Skipping row with empty player name: {line}")
                continue

            document = {
                "player_name": player_name,
                "win_count": win_count
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

        collection = db["player_wins"]

        # -------------------------------------------------
        # Read and parse HDFS output
        # -------------------------------------------------

        lines = read_hdfs_output()

        documents = parse_player_wins(lines)

        if not documents:

            print("No valid records found.")

            return

        # -------------------------------------------------
        # Upsert Player Wins records
        # -------------------------------------------------

        operations = [

            UpdateOne(
                {
                    "player_name": document["player_name"]
                },

                {
                    "$set": document
                },

                upsert=True
            )

            for document in documents

        ]

        result = collection.bulk_write(
            operations,
            ordered=False
        )

        # -------------------------------------------------
        # Display loading results
        # -------------------------------------------------

        print("\nPlayer wins loaded successfully!")

        print(f"Records inserted: {result.upserted_count}")

        print(f"Records matched: {result.matched_count}")

        print(f"Records updated: {result.modified_count}")

        # -------------------------------------------------
        # Display sample stored documents
        # -------------------------------------------------

        print("\nSample stored documents:")

        for document in collection.find(
            {},
            {
                "_id": 0
            }
        ).sort(
            "win_count",
            -1
        ).limit(20):

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