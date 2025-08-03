import json, random, time 
from datetime import datetime 
from kafka import KafkaProducer 


EVENT_HUB_NAMESPACE = "kafeelkafka.servicebus.windows.net"
TOPIC = "transactions"
CONNECTION_STRING = "Endpoint=sb://kafeelkafka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=/4sdIFVP6Iw0qGhRCgBpiBcm1Pa0I6QKM+AEhDydN3Q="

producer = KafkaProducer( 
    bootstrap_servers=f'{EVENT_HUB_NAMESPACE}:9093', 
    security_protocol='SASL_SSL', 
    sasl_mechanism='PLAIN', 
    sasl_plain_username='$ConnectionString', 
    sasl_plain_password=f"{CONNECTION_STRING}", 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
    key_serializer=lambda k: k.encode('utf-8') 

) 

users = ["U100", "U101", "U102"] 
locations = ["Mumbai", "Delhi", "Bangalore", "NYC", "London"] 

def generate_txn(): 
    return { 
        "transactionId": f"TX{random.randint(1000,9999)}", 
        "cardNumber": f"9876-XXXX-XXXX-{random.randint(1000,9999)}", 
        "amount": round(random.uniform(100, 100000), 2), 
        "location": random.choice(locations), 
        "timestamp": datetime.utcnow().isoformat(), 
        "userId": random.choice(users) 

    } 

while True: 
    txn = generate_txn() 
    print("Sending:", txn) 
    producer.send("transactions", key=txn["transactionId"], value=txn) 
    time.sleep(1) 