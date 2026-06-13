import json
import time
import random
from kafka import KafkaProducer
import sys

print("🚀 Producer Kafka démarré...")

# Attendre que Kafka soit prêt
time.sleep(15)

try:
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version_auto_timeout_ms=30000,
        request_timeout_ms=30000
    )
    print("✅ Connecté à Kafka avec succès!")
except Exception as e:
    print(f"❌ Erreur de connexion à Kafka: {e}")
    sys.exit(1)

users = ["alice", "bob", "charlie", "diana", "eve", "franck", "george", "helen"]
pages = ["home", "login", "profile", "dashboard", "about", "contact", "products", "checkout"]
statuses = [200, 200, 200, 200, 404, 500, 302, 200]

count = 0
while True:
    log = {
        "user": random.choice(users),
        "page": random.choice(pages),
        "status": random.choice(statuses),
        "timestamp": time.time()
    }
    try:
        future = producer.send('web_logs', log)
        record_metadata = future.get(timeout=10)
        count += 1
        print(f"[{count}] ✅ Produit: {log}")
    except Exception as e:
        print(f"❌ Erreur d'envoi: {e}")
    
    time.sleep(random.uniform(0.5, 2))
