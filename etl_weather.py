import requests
from pymongo import MongoClient, ASCENDING
from datetime import datetime
from config import API_KEY, CITIES, MONGO_URI, DB_NAME, COLLECTION_NAME

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Ensure index for deduplication and fast queries
collection.create_index([("city", ASCENDING), ("fetched_at", ASCENDING)], unique=True)

def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        data = response.json()
        if response.status_code != 200:
            print(f"Error fetching data for {city}: {data.get('message')}")
            return None
        return {
            "city": city,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather_description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "fetched_at": datetime.utcnow()
        }
    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
        return None

def main():
    for city in CITIES:
        weather_data = fetch_weather(city)
        if weather_data:
            try:
                collection.insert_one(weather_data)
                print(f"Inserted weather data for {city}")
            except Exception as e:
                print(f"Skipped inserting {city}: {e}")

if __name__ == "__main__":
    main()

