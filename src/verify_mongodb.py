
import os

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient


# =========================================================
# Load Environment Variables
# =========================================================

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")

if not MONGO_URI:
    print("MONGODB_URI not found in .env file.")
    raise SystemExit(1)


# =========================================================
# Connect to MongoDB Atlas
# =========================================================

client = MongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=10000
)

try:
    client.admin.command("ping")
    print("MongoDB Atlas connected successfully!\n")

except Exception as e:
    print("MongoDB connection failed:", e)
    raise SystemExit(1)


# =========================================================
# Select Database
# =========================================================

db = client["tennis_analytics"]


# =========================================================
# Collections to Verify
# =========================================================

collections_to_check = {
    "player_wins": ["player_name", "win_count"],
    "surface_analysis": [
        "surface",
        "match_count",
        "avg_aces",
        "avg_double_faults",
        "avg_first_serve_pct",
        "avg_break_point_conversion_pct",
        "avg_duration_minutes"
    ],
    "tournament_analysis": [
        "tournament",
        "match_count",
        "avg_duration_minutes"
    ]
}


# =========================================================
# Verify Collections
# =========================================================

print("=" * 60)
print("MONGODB COLLECTION VERIFICATION")
print("=" * 60)

available_collections = db.list_collection_names()

print("\nAvailable collections:")
print(available_collections)

all_checks_passed = True

for collection_name, required_fields in collections_to_check.items():

    print("\n" + "-" * 60)
    print(f"Collection: {collection_name}")
    print("-" * 60)

    if collection_name not in available_collections:
        print("ERROR: Collection does not exist.")
        all_checks_passed = False
        continue

    collection = db[collection_name]

    # Count documents
    document_count = collection.count_documents({})

    print(f"Document count: {document_count}")

    if document_count == 0:
        print("WARNING: Collection is empty.")
        all_checks_passed = False
        continue

    # Get one sample document
    sample = collection.find_one({}, {"_id": 0})

    print("\nSample document:")
    print(sample)

    # Check required fields
    missing_fields = [
        field for field in required_fields
        if field not in sample
    ]

    if missing_fields:
        print("\nMissing fields:")
        print(missing_fields)
        all_checks_passed = False

    else:
        print("\nRequired fields: PASS")

    # Check for documents missing required fields
    missing_field_filter = {
        "$or": [
            {field: {"$exists": False}}
            for field in required_fields
        ]
    }

    missing_field_count = collection.count_documents(
        missing_field_filter
    )

    if missing_field_count > 0:
        print(
            f"Documents missing required fields: "
            f"{missing_field_count}"
        )
        all_checks_passed = False

    else:
        print("Field completeness: PASS")


# =========================================================
# Final Verification Result
# =========================================================

print("\n" + "=" * 60)
print("FINAL VERIFICATION RESULT")
print("=" * 60)

if all_checks_passed:
    print("ALL COLLECTION CHECKS PASSED!")

else:
    print("Some checks need attention.")

client.close()
print("\nMongoDB connection closed.")
