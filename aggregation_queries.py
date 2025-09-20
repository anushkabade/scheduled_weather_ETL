from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client.weather_db
collection = db.weather_data

# Aggregation: Daily min, max, avg temperature per city
today = datetime.utcnow().date()
pipeline = [
    {"$match": {"fetched_at": {"$gte": datetime(today.year, today.month, today.day)}}},
    {"$group": {
        "_id": "$city",
        "min_temp": {"$min": "$temperature"},
        "max_temp": {"$max": "$temperature"},
        "avg_temp": {"$avg": "$temperature"}
    }}
]

results = list(collection.aggregate(pipeline))
print("Daily Weather Summary:")
for r in results:
    print(f"City: {r['_id']}, Min: {r['min_temp']}°C, Max: {r['max_temp']}°C, Avg: {r['avg_temp']:.2f}°C")

