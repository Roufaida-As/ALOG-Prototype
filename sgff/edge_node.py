"""
edge_node.py
------------
Le nœud terrain (Edge Node) — première ligne du système.

Rôle : recevoir les mesures MQTT, les filtrer, et transmettre
       uniquement les événements qualifiés à Kafka.

Tactiques démontrées :
- Authenticate Actors : rejette tout message sans token valide.
- Prioritize Events : classe les messages en CRITIQUE / NORMAL / ROUTINE.
- Heartbeat : chaque capteur connu doit donner signe de vie
                                    toutes les 10s, sinon → alerte de panne.
- Control data rate : messages ROUTINE non transmis à Kafka — réduit la charge du pipeline central
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import time
import threading

# --- Configuration ---
BROKER       = "localhost"
MQTT_PORT    = 1883
TOPIC_MQTT   = "sgff/capteurs"

# Topics Kafka (un par niveau de priorité — tactique Prioritize Events)
TOPIC_CRITIQUE = "sensor-critical"   # alertes immédiates
TOPIC_NORMAL   = "sensor-qualified"  # données normales filtrées

# Seuils de détection (configurables — tactique Binding Time)
SEUIL_TEMP_CRITIQUE = 70.0   # °C → alerte niveau 2/3
SEUIL_TEMP_NORMAL   = 50.0   # °C → surveillance renforcée
SEUIL_FUMEE         = 60.0   # ppm

# Tokens valides (en prod : vérification certificat X.509)
TOKENS_VALIDES = {"tok-abc123", "tok-def456", "tok-ghi789"}

# Suivi heartbeat : {capteur_id → dernier timestamp reçu}
derniere_activite = {}
HEARTBEAT_TIMEOUT = 10  # secondes sans message = capteur considéré en panne

# --- Connexion Kafka ---
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def classifier_message(mesure):
    """
    Pré-qualification légère : détermine le niveau de priorité.
    Tactique : Prioritize Events — les critiques passent en file dédiée.
    """
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)

    if temp >= SEUIL_TEMP_CRITIQUE or fumee >= SEUIL_FUMEE:
        return "CRITIQUE"
    elif temp >= SEUIL_TEMP_NORMAL:
        return "NORMAL"
    else:
        return "ROUTINE"

def on_message(client, userdata, msg):
    """Callback déclenché à chaque message MQTT reçu."""
    try:
        mesure = json.loads(msg.payload.decode("utf-8"))
        capteur_id = mesure.get("capteur_id", "?")
        token      = mesure.get("token", "")

        # ── AUTHENTIFICATION (Tactique : Authenticate Actors) ──────────
        if token not in TOKENS_VALIDES:
            print(f"[SÉCURITÉ] ⛔ Message rejeté — token invalide | "
                  f"source: {capteur_id} | token: '{token}'")
            return  # message forgé : on arrête ici, il n'entre pas dans Kafka

        # ── HEARTBEAT : mise à jour de l'activité du capteur ───────────
        derniere_activite[capteur_id] = time.time()

        # ── CLASSIFICATION (Tactique : Prioritize Events) ───────────────
        niveau = classifier_message(mesure)
        mesure["niveau"] = niveau

        if niveau == "ROUTINE":
            # On ne transmet pas les données routinières pour économiser
            # la bande passante (Tactique : Manage Sampling Rate)
            return

        # ── TRANSMISSION VERS KAFKA ─────────────────────────────────────
        topic = TOPIC_CRITIQUE if niveau == "CRITIQUE" else TOPIC_NORMAL
        producer.send(topic, mesure)

        symbole = "🔴" if niveau == "CRITIQUE" else "🟡"
        print(f"[EDGE] {symbole} {niveau} | {capteur_id} | "
              f"Temp: {mesure['temperature']}°C | "
              f"Fumée: {mesure['fumee']} → Kafka:{topic}")

    except Exception as e:
        print(f"[EDGE] Erreur traitement message : {e}")

def surveiller_heartbeat():
    """
    Vérifie périodiquement que tous les capteurs connus donnent signe de vie.
    Tactique : Heartbeat (Bass ch.5)
    Si un capteur dépasse HEARTBEAT_TIMEOUT secondes sans message → panne détectée.
    """
    while True:
        time.sleep(5)
        maintenant = time.time()
        for capteur_id, derniere in list(derniere_activite.items()):
            if maintenant - derniere > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT] ⚠️  Capteur {capteur_id} silencieux "
                      f"depuis {int(maintenant - derniere)}s → panne détectée !")

# --- Lancement du thread heartbeat ---
thread_hb = threading.Thread(target=surveiller_heartbeat, daemon=True)
thread_hb.start()

# --- Connexion MQTT ---
client = mqtt.Client(client_id="edge_node")
client.on_message = on_message
client.connect(BROKER, MQTT_PORT)
client.subscribe(TOPIC_MQTT)

print("=== Edge Node SGFF démarré ===")
print(f"Seuil critique : {SEUIL_TEMP_CRITIQUE}°C | Seuil normal : {SEUIL_TEMP_NORMAL}°C")
print("En attente de messages capteurs...\n")

client.loop_forever()