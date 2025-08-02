from kafka import KafkaProducer
import json, random, time
from datetime import datetime,timedelta
import uuid

producer = KafkaProducer(
    bootstrap_servers='balusudha-kafka-namespace.servicebus.windows.net:9093',
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password='',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None
)


def random_date():
    start_year = 2015
    end_year = 2024
    start_date =  datetime(start_year,1,1)
    end_date = datetime(end_year,12,31)
    random_date = (start_date + timedelta(days=random.randint(0, (end_date - start_date).days))).isoformat()
    return random_date


def random_time():
    hour = random.randint(0,24)
    minutes = random.randint(0,59)
    sec = random.randint(0,59)
    return f"{hour:02d}:{minutes:02d}:{sec:02d}"


def random_location():
    locations = ["Mumbai","Delhi","Bengaluru","Hyderabad","Chennai","Kolkata","Pune","Ahmedabad","Jaipur","Lucknow"]
    return random.choice(locations)

def random_card_type():
    return random.choice(["VISA", "MasterCard", "AMEX", "RUPAY"])

def random_card_number():
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def random_status():
    return random.choice(["SUCCESS", "FAILED"])

def random_flag():
    return random.choice([True, False])

def generate_transaction():
    txn = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"{random.randint(1000,9999)}",
        "amount": round(random.uniform(10000.0, 50000.0), 2),
        "date": random_date(),
        "time": random_time(),
        "location": random_location(),
        "cardtype": random_card_type(),
        "cardnumber": random_card_number(),
        "status": random_status(),
        "flag": random_flag()
    }
    return txn



while True:
    row = generate_transaction()
    print("Sending data row:", row)
    producer.send("transactions", key=row["transactionId"], value=row)
    time.sleep(0.05)