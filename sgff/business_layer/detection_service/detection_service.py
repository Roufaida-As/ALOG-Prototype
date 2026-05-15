from confluent_kafka import Consumer, KafkaException
import json
import time
from datetime import datetime
import os

# --- Configuration ---
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"
BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")

# Seuils de classification finale
SEUIL_NIVEAU_3 = 80.0   # °C → feu confirmé
SEUIL_NIVEAU_2 = 70.0   # °C → alerte sérieuse
SEUIL_NIVEAU_1 = 50.0   # °C → anomalie détectée

# Journal des alertes (Audit Trail)
journal_alertes = []

# --- Fonctions de Connexion Résilientes ---

def get_kafka_consumers(broker):
    """
    Initialise les consommateurs avec une boucle de résilience.
    Tactique : Availability (Wait for dependencies)
    """
    conf = {
        "bootstrap.servers": broker,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    }
    
    while True:
        try:
            # On crée un consommateur test pour vérifier si Kafka répond
            test_c = Consumer({**conf, "group.id": "test-connection"})
            test_c.list_topics(timeout=2.0)
            
            # Si on arrive ici, Kafka est prêt
            print("[DETECTION] ✅ Connecté au bus Kafka")
            
            # Création des deux consommateurs pour la priorisation
            c_crit = Consumer({**conf, "group.id": "detection-critique"})
            c_norm = Consumer({**conf, "group.id": "detection-normal"})
            return c_crit, c_norm
        except Exception as e:
            print(f"[DETECTION] ⏳ En attente de Kafka sur {broker}... ({e})")
            time.sleep(5)

# Initialisation
consumer_critique, consumer_normal = get_kafka_consumers(BROKER)
consumer_critique.subscribe([TOPIC_CRITIQUE])
consumer_normal.subscribe([TOPIC_NORMAL])

# --- Logique Métier ---

def classifier_niveau(mesure):
    """Classification finale sur 3 niveaux."""
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)

    if temp >= SEUIL_NIVEAU_3 or (temp >= SEUIL_NIVEAU_2 and fumee >= 60):
        return 3, "🔴 FEU CONFIRMÉ — Intervention immédiate"
    elif temp >= SEUIL_NIVEAU_2:
        return 2, "🟠 ALERTE SÉRIEUSE — Surveillance renforcée"
    else:
        return 1, "🟡 ANOMALIE — Monitoring activé"

def traiter_alerte(mesure, priorite):
    """
    Tactique : Maintain Audit Trail.
    Journalise l'alerte avec tous les détails de provenance.
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
    print(f"  Source : {mesure.get('capteur_id')} | Zone : {mesure.get('zone')}")
    print(f"  Status : Niveau {niveau} -> {description}")
    print(f"  [AUDIT] Log #{len(journal_alertes)} enregistré.")
    print(f"{'='*55}")

# --- Boucle de traitement ---

print("=== Service de Détection SGFF Opérationnel ===")
print(f"Priorisation activée : {TOPIC_CRITIQUE} traité en premier.")

try:
    while True:
        # 1. Traitement Prioritaire (Critique)
        # On poll le topic critique avec un timeout court
        msg = consumer_critique.poll(0.5)
        if msg is not None and not msg.error():
            traiter_alerte(json.loads(msg.value().decode("utf-8")), "CRITIQUE")

        # 2. Traitement Normal
        msg2 = consumer_normal.poll(0.5)
        if msg2 is not None and not msg2.error():
            traiter_alerte(json.loads(msg2.value().decode("utf-8")), "NORMAL")

        # Petite pause pour ne pas saturer le CPU si aucun message
        time.sleep(0.01)

except KeyboardInterrupt:
    print(f"\nArrêt du service. {len(journal_alertes)} alertes journalisées.")
finally:
    consumer_critique.close()
    consumer_normal.close()