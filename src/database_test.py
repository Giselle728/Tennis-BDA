
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

load_dotenv()

uri = os.getenv("MONGODB_URI")

if not uri:
    raise ValueError("MONGODB_URI not found in .env")

client = MongoClient(
    uri,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=10000
)

try:
    client.admin.command("ping")
    print("✅ MongoDB Atlas connected successfully!")

    print("Databases:")
    print(client.list_database_names())

except Exception as e:
    print("❌ MongoDB connection failed:")
    print(e)

finally:
    client.close()