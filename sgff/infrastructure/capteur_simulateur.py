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
    {"id": "CAP-BJ-01", "zone": "Bejaia",     "token": "tok-def456", "base_temp": 35},
    {"id": "CAP-BL-01", "zone": "Blida",       "token": "tok-ghi789", "base_temp": 32},
]

def simuler_mesure(capteur):
    """Génère une mesure réaliste avec variation aléatoire."""
    # Température de base + fluctuation aléatoire pour simuler des conditions changeantes 
    temp = capteur["base_temp"] + random.uniform(-3, 25)
    # Fumée : fluctuation aléatoire, avec une probabilité plus élevée de valeurs élevées en zone critique
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
    (Tactique : Revoke actor)
    """
    message_forge = {
        "capteur_id": "ATTAQUANT",
        "zone":       "Tizi Ouzou",
        "token":      "FAUX_TOKEN",     # token invalide
        "temperature": 95.0,            # valeur extrême pour déclencher une fausse alerte
        "fumee":       100.0,
        "timestamp":   time.time()
    }
    # json.dumps pour simuler un message MQTT typique, dumps encode en bytes pour MQTT
    client.publish(TOPIC, json.dumps(message_forge))
    print("[ATTAQUANT] Message forgé envoyé avec faux token !")


def simuler_alerte_critique(client, capteur):
    """Envoie un événement critique légitime pour tester la priorité."""
    message_critique = {
        "capteur_id": capteur["id"],
        "zone":       capteur["zone"],
        "token":      capteur["token"],
        "temperature": 90.0,
        "fumee":       90.0,
        "timestamp":   time.time()
    }
    client.publish(TOPIC, json.dumps(message_critique))
    print(f"[{capteur['id']}] ALERTE CRITIQUE TEST envoyée !")

# --- Connexion au broker MQTT ---
client = mqtt.Client(client_id="simulateur")
client.connect(BROKER, PORT)
# Démarrage de la boucle MQTT dans un thread séparé pour ne pas bloquer le simulateur
client.loop_start()

print("Simulateur de capteurs SGFF est démarré...")
print("Capteurs actifs :", [c["id"] for c in CAPTEURS])
print("Ctrl+C pour arrêter\n")

# cycle sert de compteur d'itération de la boucle(tour de boucle),
# elle compte combien de fois 
# le programme a exécuté le tour complet de la boucle while true
# cela nous permet de faire des actions à des intervalles réguliers (ex: toutes les 5 itérations)
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

        # Toutes les 8 secondes, on injecte une alerte critique légitime pour tester
        if cycle % 4 == 0:
            simuler_alerte_critique(client, CAPTEURS[0])

        time.sleep(2)

except KeyboardInterrupt:
    print("\nSimulateur arrêté.")
    client.loop_stop()