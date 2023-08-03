from kafka import KafkaProducer
import json
import random
import time

def message_sender(topic, msg):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,11,5))
    print(f"Topic: {topic} Message: {msg}")
    producer.send(topic, value=msg.encode())
    producer.flush()
    return True

def detection_gen():
    # Generate random values
    location = random.randint(1, 100)
    event_type = random.choice(["vehicle", "person", "animal", "fire"])
    severity = random.choice(["red", "yellow", "green"])

    # Create message dictionary
    msg = {"msg_type": "detection", "location": location, "event_type": event_type, "severity": severity}
    msg_str = json.dumps(msg)
    print(msg_str)
    return msg_str

def access_gen():
    # Generate random values
    location = random.randint(1, 100)
    event_type = random.choice(["inner door", "exterior door", "garage", "gate"])
    severity = random.choice(["red", "yellow", "green"])

    # Create message dictionary
    msg = {"msg_type": "access", "location": location, "event_type": event_type, "severity": severity}
    msg_str = json.dumps(msg)
    print(msg_str)
    return msg_str

def theft_gen():
    # Generate random values
    location = random.randint(1, 100)
    event_type = random.choice(["money", "merchandize", "fuel"])
    severity = random.choice(["red", "yellow", "green"])

    # Create message dictionary
    msg = {"msg_type": "theft", "location": location, "event_type": event_type, "severity": severity}
    msg_str = json.dumps(msg)
    print(msg_str)
    return msg_str

def fire_gen():
    # Generate random values
    location = random.randint(1, 100)
    event_type = random.choice(["brush", "building", "vehicle", "person"])
    severity = random.choice(["red", "yellow", "green"])

    # Create message dictionary
    msg = {"msg_type": "fire", "location": location, "event_type": event_type, "severity": severity}
    msg_str = json.dumps(msg)
    print(msg_str)
    return msg_str



while True:
    acton_type = random.choice(["detection", "access", "theft", "fire"])

    if acton_type == "detection":
        msg_str = detection_gen()
    elif acton_type == "access":
        msg_str = access_gen()
    elif acton_type == "theft":
        msg_str = theft_gen()
    elif acton_type == "fire":
        msg_str = fire_gen()



    # Send message to topic
    message_sender('message_stream', msg_str)

    time.sleep(5)
