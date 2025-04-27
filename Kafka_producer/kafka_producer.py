import requests
import json
import time
from confluent_kafka import Producer


kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'weather-producer'
}

api_url = "https://api.weatherapi.com/v1/current.json"
params = {
    "key": "be31cf794df34c0b848162013242211",
    "q": "tunis",
    "dt": "2023-01-01",
    "time_epoch": 1671517600
}
kafka_topic = 'weatherkafkatopic'

# Function to fetch data from the API, filter, and publish to Kafka
def fetch_filter_and_publish():
    try:
        response = requests.get(api_url, params=params)
        data = response.json()
        #print("Raw Data:")
        #print(json.dumps(data, indent=4))
        filtered_data = {
            'datetime': data['location']['localtime'],
            'name': data['location']['name'],
            'country': data['location']['country'],
            'latitude': data['location']['lat'],
            'longitude': data['location']['lon'],
            'timezone': data['location']['tz_id'],
            'temp_c': data['current']['temp_c'],  
            'wind_mph': data['current']['wind_mph'],
            'humidity': data['current']['humidity'],
            'precip_mm': data['current']['precip_mm']
        }
        producer.produce(kafka_topic, json.dumps(filtered_data))

        print("Filtered data published to Kafka topic:", kafka_topic)

    except Exception as e:
        print(f"Error: {e}")

producer = Producer(kafka_config)

while True:
    fetch_filter_and_publish()
    time.sleep(9)  # 900 seconds = 15 minutes
