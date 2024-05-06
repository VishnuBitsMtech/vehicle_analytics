from confluent_kafka import Consumer
import json

# Constants
SPEED_THRESHOLD = 80  # Example threshold for over speeding

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "mqtt_data_topic"

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'mqtt-data-consumer',
    'auto.offset.reset': 'earliest'
}

# Statistics dictionary to keep track of over speeding cases
statistics = {}

# Function for preprocessing data and identifying over speeding cases
def preprocess_and_identify_over_speeding(data):
    # Convert data from JSON string to Python dictionary
    data_dict = json.loads(data)

    # Perform preprocessing and check for over speeding
    if data_dict['speed'] > SPEED_THRESHOLD:
        # Update statistics
        update_statistics(data_dict)
        return data_dict
    else:
        return None

# Function to update statistics based on over speeding cases
def update_statistics(data):
    route = data.get('route', 'Unknown')  # Use 'Unknown' if route is not provided
    truck_id = data['vehicle_id']
    driver_name = data['driver_name'] if 'driver_name' in data else 'Unknown'
    # Update statistics dictionary
    if route not in statistics:
        statistics[route] = {}
    if truck_id not in statistics[route]:
        statistics[route][truck_id] = {'count': 1, 'driver_names': [driver_name]}
    else:
        statistics[route][truck_id]['count'] += 1
        statistics[route][truck_id]['driver_names'].append(driver_name)

# Function to display over speeding statistics
def show_statistics():
    # Display over speeding statistics to end consumers
    for route, trucks in statistics.items():
        print(f"Route: {route}")
        for truck_id, data in trucks.items():
            count = data['count']
            driver_names = ', '.join(data['driver_names'])
            print(f"Truck ID: {truck_id}, Over Speeding Cases: {count}, Driver Names: {driver_names}")

# Function to save over speeding statistics to a text file
def save_statistics_to_file():
    with open('over_speeding_statistics.txt', 'a') as file:
        for route, trucks in statistics.items():
            for truck_id, data in trucks.items():
                count = data['count']
                driver_names = ', '.join(data['driver_names'])
                file.write(f"Route: {route}, Truck ID: {truck_id}, Over Speeding Cases: {count}, Driver Names: {driver_names}\n")

# Create Kafka Consumer
consumer = Consumer(consumer_conf)

# Subscribe to the Kafka topic
consumer.subscribe([KAFKA_TOPIC])

# Consume messages from the Kafka topic
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the received message
        message = msg.value().decode('utf-8')
        print(f"Received message: {message}")

        # Preprocess the data and identify over speeding cases
        preprocessed_data = preprocess_and_identify_over_speeding(message)
        if preprocessed_data:
            print("Over speeding case detected:", preprocessed_data)

except KeyboardInterrupt:
    pass

finally:
    # Leave the consumer group and commit final offsets
    consumer.close()

    # Save over speeding statistics
    save_statistics_to_file()
