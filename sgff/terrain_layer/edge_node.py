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

# config node
node_mode = "CENTRAL"
# --- Fonctions de Connexion Résilientes ---

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
    try:
        producer.produce(topic, json.dumps(message).encode("utf-8"), callback=delivery_callback)
        producer.poll(0)
        return True
    except BufferError:
        # File locale pleine: on traite les callbacks puis on retente une fois.
        producer.poll(1.0)
        try:
            producer.produce(topic, json.dumps(message).encode("utf-8"), callback=delivery_callback)
            producer.poll(0)
            return True
        except Exception as e:
            print(f"[KAFKA] Echec de publication (file pleine): {e}")
            return False
    except Exception as e:
        print(f"[KAFKA] Echec de publication: {e}")
        return False

def classifier_message(mesure):
    """Tactique : Prioritize Events."""
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)
    if temp >= SEUIL_TEMP_CRITIQUE or fumee >= SEUIL_FUMEE:
        return "CRITIQUE"
    elif temp >= SEUIL_TEMP_NORMAL:
        return "NORMAL"
    return "ROUTINE"

def delivery_callback(err, msg):
    if err is None:
        return
    else:
        print(f"[KAFKA] Erreur de livraison : {err.str()}")


def kafka_disponible(timeout=2.0):
    """Vérifie si Kafka est joignable depuis l'edge."""
    try:
        producer.list_topics(timeout=timeout)
        return True
    except Exception:
        return False

# Tactique : passive redundancy (P2P fallback)
def activer_mode_P2P():
    global node_mode
    node_mode = "P2P"
    print("[P2P] Centrale perdue → Mode P2P activé")
    
def desactiver_mode_P2P():
    global node_mode
    node_mode = "CENTRAL"
    print("[P2P] Centrale retrouvée → Mode CENTRAL réactivé")

def envoyer_warning_et_analyse(mesure):
    """Envoie d'abord un warning, puis une confirmation différée."""
    if node_mode != "CENTRAL":
        print(f"[P2P] Warning local | {mesure.get('capteur_id')} (CRITIQUE)")
        return

    # Phase 1 : on publie immédiatement un avertissement pour traiter le critique en premier.
    warning = {**mesure,
               "phase": "PRELIMINAIRE",
               "alerte_status": "WARNING",
               "warning_sent": True}
    if not publier_sur_kafka(TOPIC_CRITIQUE, warning):
        activer_mode_P2P()
        print(f"[P2P] Warning local | {mesure.get('capteur_id')} (CRITIQUE)")
        return

    print(f"[EDGE] Alerte préliminaire envoyée pour {mesure.get('capteur_id')}")

    def analyse_approfondie():
        # Phase 2 : on attend un court délai puis on publie la confirmation finale.
        time.sleep(CONFIRMATION_DELAY)
        if node_mode != "CENTRAL":
            print(f"[P2P] Confirmation locale | {mesure.get('capteur_id')} (CRITIQUE)")
            return

        phase = "CONFIRMEE"
        alerte_status = "ALERTE CONFIRMEE"
        topic = TOPIC_CRITIQUE

        confirmation = {**mesure,
                         "phase": phase,
                         "alerte_status": alerte_status}
        if publier_sur_kafka(topic, confirmation):
            print(f"[EDGE] Analyse approfondie -> {alerte_status} envoyé vers {topic}")
        else:
            activer_mode_P2P()
            print(f"[P2P] Confirmation locale | {mesure.get('capteur_id')} (CRITIQUE)")

    threading.Thread(target=analyse_approfondie, daemon=True).start()


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

        topic = TOPIC_NORMAL
        if node_mode == "CENTRAL":
            if publier_sur_kafka(topic, mesure):
                print(f"[EDGE] Envoyé vers {topic} | {capteur_id} ({niveau})")
            else:
                activer_mode_P2P()
                print(f"[P2P] traitement local | {capteur_id} ({niveau})")
        else:
            print(f"[P2P] traitement local | {capteur_id} ({niveau})")   

    except Exception as e:
        print(f"[EDGE] Erreur : {e}")


def surveiller_heartbeat():
    """Tactique : Heartbeat."""
    global node_mode
    while True:
        time.sleep(5)
        maintenant = time.time()
        disconnected = []
        for cid, last in list(derniere_activite.items()):
            if maintenant - last > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT] Capteur {cid} déconnecté !")
                disconnected.append(cid)

        for cid in disconnected:
            del derniere_activite[cid]

        # Bascule explicite sur disponibilité Kafka (évite les faux positifs en absence de trafic).
        if node_mode == "CENTRAL" and not kafka_disponible(timeout=2.0):
            activer_mode_P2P()
        elif node_mode == "P2P" and kafka_disponible(timeout=2.0):
            desactiver_mode_P2P()

# Lancement

# Thread pour le monitoring des pannes
threading.Thread(target=surveiller_heartbeat, daemon=True).start()

# Setup Client MQTT
client = mqtt.Client(client_id="edge_node")
client.on_message = on_message

connect_mqtt(client)
client.subscribe(TOPIC_MQTT)
print(f"[MQTT] Inscription au topic {TOPIC_MQTT}")

print("Edge Node SGFF Opérationnel")
#client.loop_forever() , forever is because we want the edge node to run indefinitely, listening for MQTT messages and processing them.
client.loop_forever()