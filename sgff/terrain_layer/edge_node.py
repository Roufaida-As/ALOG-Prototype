import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import time
import threading
import json
import os


# Configuration
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

# Suivi Heartbeat, dernière activité des capteurs pour détecter les pannes (Tactique : Heartbeat)
derniere_activite = {}
HEARTBEAT_TIMEOUT = 15  # secondes
CONFIRMATION_DELAY = 2.0  # secondes pour la phase d'analyse approfondie

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "localhost:29092")


def iter_kafka_bootstraps():
    """Retourne les brokers Kafka a essayer, dans l'ordre de preference."""
    candidates = []
    for broker in str(KAFKA_BOOTSTRAP).split(","):
        broker = broker.strip()
        if broker:
            candidates.append(broker)

    for fallback in ("localhost:29092", "127.0.0.1:29092", "localhost:9092", "127.0.0.1:9092"):
        if fallback not in candidates:
            candidates.append(fallback)

    return candidates


def get_kafka_producer(config):
    """Attend que Kafka soit prêt avant de continuer."""
    while True:
        for broker in iter_kafka_bootstraps():
            try:
                candidate_config = dict(config)
                candidate_config["bootstrap.servers"] = broker
                p = Producer(candidate_config)
                # On verifie que Kafka est joignable avant de poursuivre.
                p.list_topics(timeout=2.0)
                print(f"[KAFKA] Connecte au bus d'evenements via {broker}")
                return p
            except Exception as e:
                print(f"[KAFKA] Attente de Kafka sur {broker}... ({e})")

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

# Logique métier 

def publier_sur_kafka(topic, message):
    """Envoie un message Kafka et traite tout de suite les callbacks internes."""
    producer.produce(topic, json.dumps(message).encode("utf-8"))
    producer.poll(0)

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
    """Envoie d'abord un warning, puis une confirmation différée."""
    # Phase 1 : on publie immédiatement un avertissement pour traiter le critique en premier.
    warning = {**mesure,
               "phase": "PRELIMINAIRE",
               "alerte_status": "WARNING",
               "warning_sent": True}
    publier_sur_kafka(TOPIC_CRITIQUE, warning)
    print(f"[EDGE] Alerte préliminaire envoyée pour {mesure.get('capteur_id')}")

    def analyse_approfondie():
        # Phase 2 : on attend un court délai puis on publie la confirmation finale.
        time.sleep(CONFIRMATION_DELAY)
        phase = "CONFIRMEE"
        alerte_status = "ALERTE CONFIRMEE"
        topic = TOPIC_CRITIQUE

        confirmation = {**mesure,
                         "phase": phase,
                         "alerte_status": alerte_status}
        publier_sur_kafka(topic, confirmation)
        print(f"[EDGE] Analyse approfondie -> {alerte_status} envoyé vers {topic}")

    threading.Thread(target=analyse_approfondie, daemon=True).start()


def on_connect(client, userdata, flags, rc):
    """Callback when MQTT connection is established."""
    if rc == 0:
        print(f"[MQTT] Connecté au broker, inscription au topic {TOPIC_MQTT}")
        client.subscribe(TOPIC_MQTT)
    else:
        print(f"[MQTT] Erreur de connexion, code {rc}")

def on_disconnect(client, userdata, rc):
    """Callback when MQTT disconnects."""
    if rc != 0:
        print(f"[MQTT] Déconnecté du broker, code {rc}")

def on_message(client, userdata, msg):
    """Traitement des messages MQTT reçus."""
    try:
        print(f"[MQTT] Message reçu sur {msg.topic}: {msg.payload[:100]}...")  # DEBUG
        mesure = json.loads(msg.payload.decode("utf-8"))
        capteur_id = mesure.get("capteur_id", "?")
        token      = mesure.get("token", "")

        # 1 Authentification (Tactique : Authenticate and revoke Actors)
        if token not in TOKENS_VALIDES:
            print(f"[SÉCURITÉ] Accès refusé pour {capteur_id} (Token invalide)")
            return

        # 2 Heartbeat
        derniere_activite[capteur_id] = time.time()

        # 3 Classification (Tactique : Prioritize Events / Control Data Rate)
        niveau = classifier_message(mesure)
        if niveau == "ROUTINE":
            return # On ignore les données routinières (économie bande passante)

        # 4 Transmission Kafka
        mesure["niveau"] = niveau
        if niveau == "CRITIQUE":
            envoyer_warning_et_analyse(mesure)
            return

    # Si on arrive ici, c'est que c'est une alerte normale, on la publie directement
        publier_sur_kafka(TOPIC_NORMAL, mesure)
        print(f"[EDGE] Envoyé vers {TOPIC_NORMAL} | {capteur_id} ({niveau})")

    except Exception as e:
        print(f"[EDGE] Erreur : {e}")

def surveiller_heartbeat():
    """Tactique : Heartbeat."""
    while True:
        time.sleep(5)
        maintenant = time.time()
        disconnected = []
        for cid, last in list(derniere_activite.items()):
            if maintenant - last > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT] Capteur {cid} deconnecte")
                disconnected.append(cid)
        
        # Remove disconnected sensors from tracking to avoid repeated messages
        for cid in disconnected:
            del derniere_activite[cid]

# Lancement

# Thread pour le monitoring des pannes
threading.Thread(target=surveiller_heartbeat, daemon=True).start()

# Setup Client MQTT
client = mqtt.Client(client_id="edge_node")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

connect_mqtt(client)

print("Edge Node SGFF Opérationnel")
#client.loop_forever() , forever is because we want the edge node to run indefinitely, listening for MQTT messages and processing them.
client.loop_forever()