import json
import time
import random
from kafka import KafkaProducer

# Attendre que Kafka soit prêt
time.sleep(10)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version_auto_timeout_ms=30000
)

users = ["alice", "bob", "charlie", "diana", "eve", "franck"]
pages = ["home", "login", "profile", "dashboard", "about", "contact"]
statuses = [200, 200, 200, 404, 500, 302]  # Plus de 200 pour simuler un vrai trafic

print("🚀 Producer démarré - Envoi de logs vers Kafka...")

while True:
    log = {
        "user": random.choice(users),
        "page": random.choice(pages),
        "status": random.choice(statuses),
        "timestamp": time.time()
    }
    try:
        producer.send("web_logs", log)
        print(f"✅ Produit : {log}")
    except Exception as e:
        print(f"❌ Erreur d'envoi : {e}")
    
    time.sleep(random.uniform(0.5, 1.5))  # Temps variable entre 0.5 et 1.5 secondes
    