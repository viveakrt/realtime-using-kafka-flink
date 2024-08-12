import json
import random
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

def generate_random_issue():
    issue_types = ["Data Reliability", "Table Health", "Schema Change", "Performance Degradation"]
    severity_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    columns = [f"column{i}" for i in range(1, 6)]

    issue_type = random.choice(issue_types)
    severity = random.choice(severity_levels)
    affected_columns = random.sample(columns, random.randint(1, len(columns)))
    
    issue_id = f"issue{random.randint(1000, 9999)}"
    table_id = f"db{random.randint(1, 10)}.schema{random.randint(1, 5)}.table{random.randint(1, 20)}"
    
    event = {
        "eventType": "TABLE_HEALTH_ISSUE",
        "source": "MonteCarlo",
        "tableId": table_id,
        "tableId": table_id,
        "issueId": issue_id,
        "issueType": issue_type,
        "severity": severity,
    }
    return event

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    # Kafka configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'client.id': 'python-producer'
    }

    # Create Producer instance
    producer = Producer(conf)

    # Number of messages to generate
    num_messages = 100

    for _ in range(num_messages):
        message = generate_random_issue()
        producer.produce('monte-carlo-observation', key=message["issueId"], value=json.dumps(message), callback=delivery_report)
        producer.poll(1)  # Serve delivery reports (async)
        time.sleep(5)  # Wait for 5 seconds before sending the next message

    # Wait up to 1 second for events
    producer.flush()

if __name__ == "__main__":
    main()
