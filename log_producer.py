# log_producer.py
# This script generates simulated log messages and sends them to a Kafka topic.

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# --- Configuration ---

# Kafka broker address
KAFKA_BROKER = 'localhost:9092'
# Kafka topic to which logs will be sent
KAFKA_TOPIC = 'logs'
# Time to wait between sending messages (in seconds)
MESSAGE_DELAY = 1

# --- Log Message Templates ---

# A list of possible log levels
LOG_LEVELS = ['INFO', 'WARNING', 'ERROR', 'DEBUG', 'CRITICAL']

# A list of sample log messages
LOG_MESSAGES = [
    'User logged in successfully',
    'Failed to connect to database',
    'Payment processed for order #',
    'New user registered',
    'API endpoint returned status 500',
    'Disk space is critically low',
    'Cache cleared successfully',
    'Invalid credentials provided for user',
    'Request timed out',
    'Data successfully exported to CSV'
]

# A list of possible sources for the logs
LOG_SOURCES = ['api-gateway', 'database-server', 'webapp-1', 'webapp-2', 'payment-service']


def create_producer():
    """
    Creates and returns a KafkaProducer instance.
    Handles connection errors and retries.
    """
    print("Connecting to Kafka broker...")
    producer = None
    while producer is None:
        try:
            # The value_serializer helps to encode our dictionary to JSON bytes
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka.")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    return producer


def generate_log_message():
    """
    Generates a single, structured log message as a Python dictionary.
    """
    # Choose random elements for the log message
    level = random.choice(LOG_LEVELS)
    message_template = random.choice(LOG_MESSAGES)
    source = random.choice(LOG_SOURCES)
    
    # Add a random ID to some messages for variety
    if '#' in message_template:
        message = f"{message_template}{random.randint(1000, 9999)}"
    elif 'user' in message_template:
        message = f"{message_template} '{'user' + str(random.randint(1, 100))}'"
    else:
        message = message_template

    # Create the final log dictionary
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'level': level,
        'source': source,
        'message': message,
        'request_time_ms': random.randint(50, 500) if level != 'ERROR' else random.randint(500, 5000)
    }
    return log_entry


def main():
    """
    Main function to run the log producer.
    """
    kafka_producer = create_producer()
    
    print(f"Starting to send logs to topic '{KAFKA_TOPIC}' every {MESSAGE_DELAY} second(s). Press Ctrl+C to stop.")

    try:
        while True:
            # Generate a new log message
            log_data = generate_log_message()
            
            # Send the log message to the Kafka topic
            kafka_producer.send(KAFKA_TOPIC, value=log_data)
            
            # Print to console to show what's being sent
            print(f"Sent: {log_data}")
            
            # Wait for the specified delay
            time.sleep(MESSAGE_DELAY)
            
    except KeyboardInterrupt:
        print("\nStopping the log producer.")
    finally:
        # Ensure all buffered messages are sent before exiting
        kafka_producer.flush()
        kafka_producer.close()
        print("Kafka producer closed.")


if __name__ == '__main__':
    main()
