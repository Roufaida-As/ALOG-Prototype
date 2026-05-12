"""
capteur_simulateur.py
---------------------
Simule 3 capteurs légitimes terrain + 1 attaquant.

Tactiques démontrées :
- Authenticate Actors : chaque capteur légitime a un token d'identité.
  L'attaquant envoie sans token → sera rejeté par l'Edge Node.
- Prioritize Events : les capteurs en zone critique
  transmettent plus fréquemment que les zones normales.
"""

import paho.mqtt.client as mqtt
import json
import time
import random

# --- Configuration ---
BROKER = "localhost"
PORT   = 1883
TOPIC  = "sgff/capteurs"   # topic MQTT où tous les capteurs publient

# Identifiants légitimes (en prod : certificats X.509)
CAPTEURS = [
    {"id": "CAP-TZ-01", "zone": "Tizi Ouzou", "token": "tok-abc123", "base_temp": 38},
    {"id": "CAP-BJ-01", "zone": "Béjaïa",     "token": "tok-def456", "base_temp": 35},
    {"id": "CAP-BL-01", "zone": "Blida",       "token": "tok-ghi789", "base_temp": 32},
]

def simuler_mesure(capteur):
    """Génère une mesure réaliste avec variation aléatoire."""
    temp = capteur["base_temp"] + random.uniform(-3, 25)
    fumee = random.uniform(0, 100)
    return {
        "capteur_id": capteur["id"],
        "zone":       capteur["zone"],
        "token":      capteur["token"],   # identifiant d'authentification
        "temperature": round(temp, 1),
        "fumee":       round(fumee, 1),
        "timestamp":   time.time()
    }

def simuler_attaquant(client):
    """
    Simule un attaquant qui injecte de fausses données.
    Pas de token valide → doit être rejeté par l'Edge Node.
    (Tactique : Authenticate Actors)
    """
    message_forge = {
        "capteur_id": "ATTAQUANT",
        "zone":       "Tizi Ouzou",
        "token":      "FAUX_TOKEN",     # token invalide
        "temperature": 95.0,            # valeur extrême pour déclencher une fausse alerte
        "fumee":       100.0,
        "timestamp":   time.time()
    }
    client.publish(TOPIC, json.dumps(message_forge))
    print("[ATTAQUANT] Message forgé envoyé avec faux token !")

# --- Connexion au broker MQTT ---
client = mqtt.Client(client_id="simulateur")
client.connect(BROKER, PORT)
client.loop_start()

print("=== Simulateur de capteurs SGFF démarré ===")
print("Capteurs actifs :", [c["id"] for c in CAPTEURS])
print("Ctrl+C pour arrêter\n")

cycle = 0
try:
    while True:
        cycle += 1

        # Chaque capteur envoie sa mesure
        for capteur in CAPTEURS:
            mesure = simuler_mesure(capteur)
            client.publish(TOPIC, json.dumps(mesure))
            print(f"[{capteur['id']}] Temp: {mesure['temperature']}°C | "
                  f"Fumée: {mesure['fumee']} | Zone: {capteur['zone']}")

        # Toutes les 10 secondes, l'attaquant tente une injection
        if cycle % 5 == 0:
            print()
            simuler_attaquant(client)
            print()

        time.sleep(2)

except KeyboardInterrupt:
    print("\nSimulateur arrêté.")
    client.loop_stop()