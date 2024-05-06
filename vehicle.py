import random
import time
import MySQLdb
import paho.mqtt.client as mqtt
import json
from confluent_kafka import Consumer, Producer
import sys

# Mosquitto MQTT Broker Configuration
MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 1883

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "mqtt_data_topic"

# Persistent Storage Configuration
STORAGE_FILE = "raw_data.txt"

class Vehicle:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        self.latitude = 0.0
        self.longitude = 0.0
        self.speed = 0
        self.fuel_level = 100
        self.tire_pressure = 32
        self.driver_condition = "Normal"

        # Connect to MySQL database
        self.conn = MySQLdb.connect(
            host="localhost", user="admin", password="admin", database="truck_driver_management"
        )
        self.cursor = self.conn.cursor()

    def get_driver_details(self):
        # Fetch driver details from MySQL database
        sql = "SELECT d.first_name, d.last_name FROM drivers d JOIN driver_truck_assignments a ON d.driver_id = a.driver_id WHERE a.truck_id = %s"
        self.cursor.execute(sql, (self.vehicle_id,))
        driver = self.cursor.fetchone()
        if driver:
            return f"{driver[0]} {driver[1]}"
        else:
            return "Unknown Driver"

    def get_truck_details(self):
        # Fetch truck details from MySQL database
        sql = "SELECT truck_registration_number, truck_model, truck_make FROM trucks WHERE truck_id = %s"
        self.cursor.execute(sql, (self.vehicle_id,))
        truck = self.cursor.fetchone()
        if truck:
            return f"{truck[0]} - {truck[1]} ({truck[2]})"
        else:
            return "Unknown Truck"

    def update_location(self):
        # Simulate vehicle movement by updating latitude and longitude
        self.latitude += random.uniform(-0.01, 0.01)
        self.longitude += random.uniform(-0.01, 0.01)

    def update_speed(self):
        # Simulate random speed changes
        self.speed = random.randint(0, 120)

    def update_fuel_level(self):
        # Simulate fuel consumption
        self.fuel_level -= random.uniform(0.01, 0.1)

    def update_tire_pressure(self):
        # Simulate tire pressure changes
        self.tire_pressure -= random.uniform(0.1, 0.5)

    def update_driver_condition(self):
        # Simulate changes in driver condition
        conditions = ["Normal", "Drowsy", "Fatigued", "Alert"]
        self.driver_condition = random.choice(conditions)

    def generate_vehicle_data(self):
        # Generate and return vehicle data
        driver_name = self.get_driver_details()
        truck_details = self.get_truck_details()
        data = {
            "vehicle_id": self.vehicle_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "speed": self.speed,
            "fuel_level": self.fuel_level,
            "tire_pressure": self.tire_pressure,
            "driver_condition": self.driver_condition,
            "driver_name": driver_name,
            "truck_details": truck_details
        }
        return data

# MQTT Callback Functions
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))

def on_message(client, userdata, msg):
    pass  # No action needed here

# Task 4: Data Transfer program moving the data from the truck to central server like Mosquito broker through MQTT protocol
def send_data_to_mqtt_broker(vehicle_data):
    client = mqtt.Client()
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.publish("truck/data", json.dumps(vehicle_data))
    client.disconnect()

# Task 5: Data transfer program from Mosquito broker to Kafka Topic and a raw data storage
def on_mqtt_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    data = json.loads(payload)

    # Publish data to Kafka topic
    produce_kafka_data(data)

    # Store raw data in persistent storage
    store_raw_data(data)

# Kafka Producer
def produce_kafka_data(data):
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
    producer.flush()

# Store Raw Data in Persistent Storage
def store_raw_data(data):
    with open(STORAGE_FILE, "a") as file:
        file.write(json.dumps(data) + "\n")

def main(vehicle_id):
    vehicle = Vehicle(vehicle_id)
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_mqtt_message

    # Connect to MQTT Broker
    mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    mqtt_client.loop_start()  # Start the MQTT client loop
    mqtt_client.subscribe("truck/data")  # Subscribe to the "truck/data" topic

    while True:
        vehicle.update_location()
        vehicle.update_speed()
        vehicle.update_fuel_level()
        vehicle.update_tire_pressure()
        vehicle.update_driver_condition()
        vehicle_data = vehicle.generate_vehicle_data()
        send_data_to_mqtt_broker(vehicle_data)  # Send data to MQTT broker
        print("Data sent to MQTT broker:", vehicle_data)
        time.sleep(1)  # Sleep for some time to simulate real-time data generation

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <vehicle_id>")
        sys.exit(1)
    vehicle_id = sys.argv[1]
    main(vehicle_id)