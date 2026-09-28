# log_producer.py
# Generates simulated application logs (with labelled, injected anomalies) and
# sends them to a Kafka topic.
#
# The generator functions are also imported by train_model.py so the model is
# trained on exactly the same distribution the stream produces.

import argparse
import json
import os
import random
import string
import time
from datetime import datetime, timezone

# --- Configuration (overridable via environment variables) ---

# Kafka broker address (inside docker-compose use kafka:29092)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
# Kafka topic to which logs will be sent
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'logs')
# Time to wait between sending messages (in seconds); 0 enables burst mode
MESSAGE_DELAY = float(os.getenv('MESSAGE_DELAY', '1'))
# Fraction of logs that are injected anomalies (ground truth for evaluation)
ANOMALY_RATE = float(os.getenv('ANOMALY_RATE', '0.05'))

# --- Normal log templates ---
# (message template, allowed levels, request_time_ms range, relative weight)
# Levels are tied to the message so the data is realistic: errors are ERROR/CRITICAL.
NORMAL_TEMPLATES = [
    ('User logged in successfully', ['INFO'], (40, 250), 20),
    ('New user registered', ['INFO'], (80, 300), 8),
    ('Payment processed for order #', ['INFO'], (120, 450), 12),
    ('Cache cleared successfully', ['INFO', 'DEBUG'], (10, 80), 8),
    ('Data successfully exported to CSV', ['INFO'], (200, 600), 6),
    ('Health check passed', ['DEBUG'], (5, 40), 14),
    ('Invalid credentials provided for user', ['WARNING'], (60, 250), 6),
    ('Disk space is running low', ['WARNING'], (20, 120), 3),
    ('Request timed out', ['ERROR'], (1000, 3000), 3),
    ('Failed to connect to database', ['ERROR', 'CRITICAL'], (500, 2000), 2),
    ('API endpoint returned status 500', ['ERROR'], (300, 1500), 3),
]

# A list of possible sources for the logs
LOG_SOURCES = ['api-gateway', 'database-server', 'webapp-1', 'webapp-2', 'payment-service']

# --- Anomaly templates (rare in the stream) ---
RARE_ERROR_MESSAGES = [
    'Kernel panic: fatal exception in interrupt handler',
    'OutOfMemoryError: Java heap space exhausted in worker pool',
    'Segmentation fault in native module libcrypto',
    'Unauthorized root access attempt detected from foreign host',
    'SSL certificate verification failed: certificate revoked',
    'Replication lag exceeded threshold, split brain suspected',
]

STACK_FRAMES = [
    'at com.example.payment.Gateway.charge(Gateway.java:{n})',
    'at com.example.db.ConnectionPool.acquire(ConnectionPool.java:{n})',
    'at com.example.api.Router.dispatch(Router.java:{n})',
    'at org.apache.http.impl.client.InternalHttpClient.execute(InternalHttpClient.java:{n})',
    'at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:{n})',
]

ANOMALY_TYPES = ['latency_spike', 'rare_error', 'unusual_length']

_WEIGHTS = [t[3] for t in NORMAL_TEMPLATES]


def _render_message(template, rng):
    """Adds a random ID to some messages for variety."""
    if '#' in template:
        return f"{template}{rng.randint(1000, 9999)}"
    if template.endswith('user'):
        return f"{template} 'user{rng.randint(1, 100)}'"
    return template


def _normal_log(rng):
    template, levels, (lo, hi), _ = rng.choices(NORMAL_TEMPLATES, weights=_WEIGHTS, k=1)[0]
    return {
        'level': rng.choice(levels),
        'message': _render_message(template, rng),
        'request_time_ms': rng.randint(lo, hi),
    }


def _anomalous_log(rng, anomaly_type):
    if anomaly_type == 'latency_spike':
        # An otherwise normal-looking log with an extreme response time
        log = _normal_log(rng)
        log['request_time_ms'] = rng.randint(8000, 30000)
        if log['level'] in ('DEBUG', 'INFO'):
            log['level'] = 'WARNING'
        return log

    if anomaly_type == 'rare_error':
        return {
            'level': rng.choice(['ERROR', 'CRITICAL']),
            'message': rng.choice(RARE_ERROR_MESSAGES),
            'request_time_ms': rng.randint(50, 3000),
        }

    # unusual_length: either a huge stack trace or a tiny garbled message
    if rng.random() < 0.7:
        frames = [rng.choice(STACK_FRAMES).format(n=rng.randint(10, 999))
                  for _ in range(rng.randint(6, 20))]
        message = 'Unhandled exception in request pipeline ' + ' '.join(frames)
        level = 'CRITICAL'
    else:
        message = ''.join(rng.choice(string.ascii_letters + string.digits) for _ in range(rng.randint(1, 4)))
        level = 'ERROR'
    return {'level': level, 'message': message, 'request_time_ms': rng.randint(50, 3000)}


def generate_log_with_type(rng=random, anomaly_rate=ANOMALY_RATE):
    """
    Generates one structured log and returns (log_entry, anomaly_type),
    where anomaly_type is None for normal logs.
    """
    if rng.random() < anomaly_rate:
        anomaly_type = rng.choice(ANOMALY_TYPES)
        log = _anomalous_log(rng, anomaly_type)
    else:
        anomaly_type = None
        log = _normal_log(rng)

    log_entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'level': log['level'],
        'source': rng.choice(LOG_SOURCES),
        'message': log['message'],
        'request_time_ms': log['request_time_ms'],
        'injected_anomaly': anomaly_type is not None,
    }
    return log_entry, anomaly_type


def generate_log_message(rng=random, anomaly_rate=ANOMALY_RATE):
    """Generates a single, structured log message as a Python dictionary."""
    return generate_log_with_type(rng, anomaly_rate)[0]


def create_producer(burst):
    """
    Creates and returns a KafkaProducer instance.
    Handles connection errors and retries.
    """
    from kafka import KafkaProducer

    # Batch aggressively in burst mode for throughput; send promptly otherwise
    batching = {'linger_ms': 20, 'batch_size': 256 * 1024} if burst else {'linger_ms': 0}

    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    producer = None
    while producer is None:
        try:
            # The value_serializer helps to encode our dictionary to JSON bytes
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **batching
            )
            print("Successfully connected to Kafka.")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    return producer


def parse_args():
    parser = argparse.ArgumentParser(description='Simulated log producer for Kafka.')
    parser.add_argument('--burst', action='store_true',
                        help='Send as fast as possible with Kafka batching (same as MESSAGE_DELAY=0).')
    parser.add_argument('--count', type=int, default=int(os.getenv('MESSAGE_COUNT', '0')),
                        help='Stop after sending this many messages (0 = run forever).')
    return parser.parse_args()


def main():
    """
    Main function to run the log producer.
    """
    args = parse_args()
    burst = args.burst or MESSAGE_DELAY <= 0
    kafka_producer = create_producer(burst)

    mode = 'burst mode' if burst else f'every {MESSAGE_DELAY} second(s)'
    print(f"Sending logs to topic '{KAFKA_TOPIC}' in {mode} with anomaly rate {ANOMALY_RATE}. "
          f"Press Ctrl+C to stop.", flush=True)

    sent = 0
    start = window_start = time.perf_counter()
    window_sent = 0
    try:
        while not args.count or sent < args.count:
            log_data = generate_log_message()
            kafka_producer.send(KAFKA_TOPIC, value=log_data)
            sent += 1

            if burst:
                # Report throughput every 5 seconds instead of printing each message
                window_sent += 1
                now = time.perf_counter()
                if now - window_start >= 5:
                    print(f"Sent {sent} logs, {window_sent / (now - window_start):.0f} events/sec", flush=True)
                    window_start, window_sent = now, 0
            else:
                print(f"Sent: {log_data}", flush=True)
                time.sleep(MESSAGE_DELAY)

    except KeyboardInterrupt:
        print("\nStopping the log producer.")
    finally:
        # Ensure all buffered messages are sent before exiting
        kafka_producer.flush()
        elapsed = time.perf_counter() - start
        print(f"Total: {sent} logs in {elapsed:.2f}s = {sent / elapsed:.0f} events/sec (including final flush)",
              flush=True)
        kafka_producer.close()
        print("Kafka producer closed.")


if __name__ == '__main__':
    main()
