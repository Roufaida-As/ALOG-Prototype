import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import time
import threading
import json
import os

# --- Configuration ---
# Utilise les noms des services Docker définis dans docker-compose.yaml
BROKER       = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT    = 1883
TOPIC_MQTT   = "sgff/capteurs"

# Topics Kafka (Tactique : Prioritize Events)
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"

# Seuils de détection
SEUIL_TEMP_CRITIQUE = 70.0
SEUIL_TEMP_NORMAL   = 50.0
SEUIL_FUMEE         = 60.0

# Sécurité (Tactique : Authenticate Actors)
TOKENS_VALIDES = {"tok-abc123", "tok-def456", "tok-ghi789"}

# Suivi Heartbeat
derniere_activite = {}
HEARTBEAT_TIMEOUT = 15  # secondes
CONFIRMATION_DELAY = 2.0  # secondes pour la phase d'analyse approfondie

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "localhost:9092")

# --- Fonctions de Connexion Résilientes ---

def get_kafka_producer(config):
    """Attend que Kafka soit prêt avant de continuer."""
    while True:
        try:
            p = Producer(config)
            # On vérifie si le broker répond réellement
            p.list_topics(timeout=2.0)
            print("[KAFKA] Connecte au bus d'evenements")
            return p
        except Exception as e:
            print(f"[KAFKA] Attente de Kafka sur {KAFKA_BOOTSTRAP}... ({e})")
            time.sleep(5)

def connect_mqtt(client):
    """Attend que le broker MQTT soit prêt."""
    while True:
        try:
            client.connect(BROKER, MQTT_PORT)
            print(f"[MQTT] Connecte au broker {BROKER}")
            break
        except Exception as e:
            print(f"[MQTT] Attente de Mosquitto sur {BROKER}... ({e})")
            time.sleep(2)

# Initialisation du Producer Kafka
producer = get_kafka_producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# --- Logique métier ---

def classifier_message(mesure):
    """Tactique : Prioritize Events."""
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)
    if temp >= SEUIL_TEMP_CRITIQUE or fumee >= SEUIL_FUMEE:
        return "CRITIQUE"
    elif temp >= SEUIL_TEMP_NORMAL:
        return "NORMAL"
    return "ROUTINE"


def envoyer_warning_et_analyse(mesure):
    """Envoie une alerte préliminaire puis lance une validation approfondie."""
    warning = {**mesure,
               "phase": "PRELIMINAIRE",
               "alerte_status": "WARNING",
               "warning_sent": True}
    producer.produce(TOPIC_CRITIQUE, json.dumps(warning).encode("utf-8"))
    producer.poll(0)
    print(f"[EDGE] Alerte préliminaire envoyée pour {mesure.get('capteur_id')}")

    def analyse_approfondie():
        time.sleep(CONFIRMATION_DELAY)
        temp = mesure.get("temperature", 0)
        fumee = mesure.get("fumee", 0)
        if temp >= SEUIL_TEMP_CRITIQUE or fumee >= SEUIL_FUMEE:
            phase = "CONFIRMEE"
            alerte_status = "ALERTE CONFIRMEE"
            topic = TOPIC_CRITIQUE
        elif temp >= SEUIL_TEMP_NORMAL:
            phase = "CONFIRMEE"
            alerte_status = "ALERTE VALIDEE"
            topic = TOPIC_NORMAL
        else:
            phase = "FAUX_POSITIF"
            alerte_status = "FAUX POSITIF"
            topic = TOPIC_NORMAL

        confirmation = {**mesure,
                        "phase": phase,
                        "alerte_status": alerte_status}
        producer.produce(topic, json.dumps(confirmation).encode("utf-8"))
        producer.poll(0)
        print(f"[EDGE] Analyse approfondie -> {alerte_status} envoyé vers {topic}")

    threading.Thread(target=analyse_approfondie, daemon=True).start()


def on_message(client, userdata, msg):
    """Traitement des messages MQTT reçus."""
    try:
        mesure = json.loads(msg.payload.decode("utf-8"))
        capteur_id = mesure.get("capteur_id", "?")
        token      = mesure.get("token", "")

        # 1. Authentification (Tactique : Authenticate Actors)
        if token not in TOKENS_VALIDES:
            print(f"[SÉCURITÉ] ⛔ Accès refusé pour {capteur_id} (Token invalide)")
            return

        # 2. Heartbeat
        derniere_activite[capteur_id] = time.time()

        # 3. Classification (Tactique : Prioritize Events / Control Data Rate)
        niveau = classifier_message(mesure)
        if niveau == "ROUTINE":
            return # On ignore les données routinières (économie bande passante)

        # 4. Transmission Kafka
        mesure["niveau"] = niveau
        if niveau == "CRITIQUE":
            envoyer_warning_et_analyse(mesure)
            return

        topic = TOPIC_CRITIQUE if niveau == "CRITIQUE" else TOPIC_NORMAL
        producer.produce(topic, json.dumps(mesure).encode("utf-8"))
        producer.poll(0) # Déclenche les callbacks internes

        print(f"[EDGE] Envoyé vers {topic} | {capteur_id} ({niveau})")

    except Exception as e:
        print(f"[EDGE] Erreur : {e}")

def surveiller_heartbeat():
    """Tactique : Heartbeat."""
    while True:
        time.sleep(5)
        maintenant = time.time()
        for cid, last in list(derniere_activite.items()):
            if maintenant - last > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT] Capteur {cid} deconnecte")

# --- Lancement ---

# Thread pour le monitoring des pannes
threading.Thread(target=surveiller_heartbeat, daemon=True).start()

# Setup Client MQTT
client = mqtt.Client(client_id="edge_node")
client.on_message = on_message

connect_mqtt(client)
client.subscribe(TOPIC_MQTT)

print("=== Edge Node SGFF Opérationnel ===")
client.loop_forever()