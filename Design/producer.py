import csv
import json
import glob
import os
from google.cloud import pubsub_v1
import random

# authentication
files = glob.glob("*.json")
if not files:
    print("No service account key found!")
    exit()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# configuration --> topic = designTopic
project_id = "project-2193e630-2541-4006-90b" 
topic_name = "designTopic" #! Topic created: designTopic

# initialize publisher (we are publishing / sending messages to the topic)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Starting to publish records from Labels.csv to {topic_path}...")

# read each record from the CSV
try:
    with open('Labels.csv', mode='r') as csv_file:
        # dictReader automatically uses the first row as keys for the dictionary
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            # Skip empty rows or rows missing key data
            if not row.get('time') or not row.get('temperature'):
                continue
                
            try:
                # Convert string values from CSV to appropriate types
                formatted_row = {
                    "ID": random.randint(0, 10000000),
                    "time": int(float(row['time'])),
                    "profile_name": row['profileName'],
                    "temperature": float(row['temperature']),
                    "humidity": float(row['humidity']),
                    "pressure": float(row['pressure'])
                }
                
                message_json = json.dumps(formatted_row)
                message_bytes = message_json.encode('utf-8')
                
                print(f"Publishing record: {message_json}")
                future = publisher.publish(topic_path, message_bytes)
                future.result()
            except ValueError as ve:
                print(f"Skipping malformed row: {row}. Error: {ve}")

    print("All records published successfully.")

except FileNotFoundError:
    print("Error: Labels.csv not found in the current directory.")
except Exception as e:
    print(f"An error occurred: {e}")