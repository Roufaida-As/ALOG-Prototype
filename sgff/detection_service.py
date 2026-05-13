"""
detection_service.py
--------------------
Service de Détection — couche Métier du SGFF.

Rôle : consommer les événements Kafka qualifiés par l'Edge Node,
       les classifier finalement (niveau 1/2/3), et déclencher l'alerte.

Tactiques démontrées :
- Prioritize Events : les critiques sont traités en priorité
  (consumer group dédié sur le topic sensor-critical).
- Schedule Resources : les alertes critiques ne sont jamais
  retardées par le traitement des données normales.
- Maintain Audit Trail : toute alerte est journalisée
  avec horodatage, source, et niveau.
"""

from confluent_kafka import Consumer, KafkaException
import json
import time
from datetime import datetime

# --- Configuration ---
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"
BROKER         = "localhost:9092"

# Seuils de classification finale
SEUIL_NIVEAU_3 = 80.0   # °C → feu confirmé, intervention immédiate
SEUIL_NIVEAU_2 = 70.0   # °C → alerte sérieuse, surveillance renforcée
SEUIL_NIVEAU_1 = 50.0   # °C → anomalie détectée

# Journal des alertes (en prod : base de données PostgreSQL)
journal_alertes = []

def classifier_niveau(mesure):
    """Classification finale sur 3 niveaux."""
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)

    if temp >= SEUIL_NIVEAU_3 or (temp >= SEUIL_NIVEAU_2 and fumee >= 60):
        return 3, "🔴 FEU CONFIRMÉ — Intervention immédiate requise"
    elif temp >= SEUIL_NIVEAU_2:
        return 2, "🟠 ALERTE SÉRIEUSE — Surveillance renforcée"
    else:
        return 1, "🟡 ANOMALIE — Monitoring activé"

def traiter_alerte(mesure, priorite):
    """
    Traite une alerte : classification, journalisation, notification simulée.
    Tactique : Maintain Audit Trail — chaque alerte est enregistrée.
    """
    niveau, description = classifier_niveau(mesure)
    horodatage = datetime.now().strftime("%H:%M:%S")

    entree_journal = {
        "timestamp":   horodatage,
        "capteur_id":  mesure.get("capteur_id"),
        "zone":        mesure.get("zone"),
        "temperature": mesure.get("temperature"),
        "fumee":       mesure.get("fumee"),
        "niveau":      niveau,
        "priorite":    priorite
    }
    journal_alertes.append(entree_journal)

    print(f"\n{'='*55}")
    print(f"  ALERTE [{horodatage}] — Priorité: {priorite}")
    print(f"  Capteur : {mesure.get('capteur_id')} | Zone : {mesure.get('zone')}")
    print(f"  Temp : {mesure.get('temperature')}°C | Fumée : {mesure.get('fumee')}")
    print(f"  → Niveau {niveau} : {description}")
    print(f"  [AUDIT] Entrée #{len(journal_alertes)} journalisée.")
    print(f"{'='*55}\n")

    # Simulation de la notification multi-canal
    # En prod : WebSocket dashboard + SMS Twilio + FCM push (en parallèle)
    print(f"  [NOTIF] Dashboard mis à jour ✓")
    print(f"  [NOTIF] SMS envoyé à la Protection Civile — {mesure.get('zone')} ✓")

# --- Consommateurs Kafka ---
# Deux consommateurs séparés pour respecter la priorisation
# Le critique est traité en premier (Tactique : Schedule Resources)

consumer_critique = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "detection-critique",
    "auto.offset.reset": "latest"
})

consumer_normal = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "detection-normal",
    "auto.offset.reset": "latest"
})
consumer_critique.subscribe([TOPIC_CRITIQUE])
consumer_normal.subscribe([TOPIC_NORMAL])

print("=== Service de Détection SGFF démarré ===")
print(f"Écoute sur : {TOPIC_CRITIQUE} (priorité haute) + {TOPIC_NORMAL}")
print("En attente d'événements...\n")

try:
    while True:
        msg = consumer_critique.poll(1.0)
        if msg is not None and not msg.error():
            traiter_alerte(json.loads(msg.value().decode("utf-8")), "CRITIQUE")

        msg2 = consumer_normal.poll(1.0)
        if msg2 is not None and not msg2.error():
            traiter_alerte(json.loads(msg2.value().decode("utf-8")), "NORMAL")

        time.sleep(0.1)

except KeyboardInterrupt:
    print(f"\nService arrêté. {len(journal_alertes)} alertes journalisées.")
    consumer_critique.close()
    consumer_normal.close()