from confluent_kafka import Consumer
import json
import time
from datetime import datetime
import os

# Configuration
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"
BROKER         = os.getenv("KAFKA_BROKER", "localhost:29092")

# Seuils de classification finale
SEUIL_NIVEAU_3 = 80.0   # °C → feu confirmé
SEUIL_NIVEAU_2 = 70.0   # °C → alerte sérieuse
SEUIL_NIVEAU_1 = 50.0   # °C → anomalie détectée

# Journal des alertes (Audit Trail)
journal_alertes = []


def iter_kafka_bootstraps():
    """Retourne les brokers Kafka a essayer, dans l'ordre de preference."""
    candidates = []
    for broker in str(BROKER).split(","):
        broker = broker.strip()
        if broker:
            candidates.append(broker)

    for fallback in ("localhost:29092", "127.0.0.1:29092", "localhost:9092", "127.0.0.1:9092"):
        if fallback not in candidates:
            candidates.append(fallback)

    return candidates


def get_kafka_consumers(broker):
    """
    Initialise les consommateurs avec une boucle de résilience.
    Tactique : Availability (Wait for dependencies)
    """
    conf = {
        "bootstrap.servers": broker, # Adresse du broker Kafka
        "auto.offset.reset": "latest", # comportement de consommation quand makch un offset commité : on prend les nouveaux messages dès le démarrage
        "enable.auto.commit": True # commit automatique des offsets pour ne pas se soucier de la
        # gestion manuelle des offsets because our prototype is simple
    }
    
    while True:
        for candidate in iter_kafka_bootstraps():
            try:
                candidate_conf = {**conf, "bootstrap.servers": candidate}
                # On crée un consommateur test pour vérifier si Kafka répond.
                test_c = Consumer({**candidate_conf, "group.id": "test-connection"})
                test_c.list_topics(timeout=2.0)
                test_c.close()

                # Si on arrive ici, Kafka est prêt.
                print(f"[DETECTION] Connecte au bus Kafka via {candidate}")

                # Création des deux consommateurs pour la priorisation.
                c_crit = Consumer({**candidate_conf, "group.id": "detection-critique"})
                c_norm = Consumer({**candidate_conf, "group.id": "detection-normal"})
                return c_crit, c_norm
            except Exception as e:
                print(f"[DETECTION] En attente de Kafka sur {candidate}... ({e})")

        time.sleep(5)

# Initialisation
consumer_critique, consumer_normal = get_kafka_consumers(BROKER)
consumer_critique.subscribe([TOPIC_CRITIQUE])
consumer_normal.subscribe([TOPIC_NORMAL])

# Logique Métier

def classifier_niveau(mesure):
    """Classification finale sur 3 niveaux."""
    temp  = mesure.get("temperature", 0)
    fumee = mesure.get("fumee", 0)

    if temp >= SEUIL_NIVEAU_3 or (temp >= SEUIL_NIVEAU_2 and fumee >= 60):
        return 3, "FEU CONFIRME — Intervention immediate"
    elif temp >= SEUIL_NIVEAU_2:
        return 2, "ALERTE SERIEUSE — Surveillance renforcee"
    else:
        return 1, "ANOMALIE — Monitoring active"

def traiter_alerte(mesure, priorite):
    """
    Tactique : Maintain Audit Trail.
    Journalise l'alerte avec tous les détails de provenance.
    """
    status = mesure.get("alerte_status", "")
    phase = mesure.get("phase", "")
    niveau, description = classifier_niveau(mesure)

    # Le flux actuel de l'Edge publie uniquement des alertes préliminaires et confirmées
    # On garde `status` et `phase` pour l'audit, mais on ne dépend plus d'un cas faux positif

    horodatage = datetime.now().strftime("%H:%M:%S")

    entree_journal = {
        "timestamp":   horodatage,
        "capteur_id":  mesure.get("capteur_id"),
        "zone":        mesure.get("zone"),
        "temperature": mesure.get("temperature"),
        "fumee":       mesure.get("fumee"),
        "niveau":      niveau,
        "priorite":    priorite,
        "phase":       phase,
        "status":      status
    }
    journal_alertes.append(entree_journal)

    print(f"\n{'='*55}")
    print(f"  ALERTE [{horodatage}] — Priorite: {priorite}")
    print(f"  Source : {mesure.get('capteur_id')} | Zone : {mesure.get('zone')}")
    print(f"  Phase : {phase or 'FINAL'} | Statut : {status or 'FINAL'}")
    print(f"  Status : Niveau {niveau} -> {description}")
    print(f"  [AUDIT] Log #{len(journal_alertes)} enregistre.")
    print()

# Boucle de traitement

print("Service de Détection SGFF Opérationnel ...")
print(f"Priorisation activée : {TOPIC_CRITIQUE} traité en premier.")

try:
    while True:
        # 1 Traitement Prioritaire (Critique)
        # On poll le topic critique avec un timeout court, poll veut dire "interroger" 
        # le broker pour voir s'il y a des messages disponibles, si oui, il les retourne,
        # sinon il attend jusqu'à ce que le timeout soit atteint et retourne None
        msg = consumer_critique.poll(0.5)
        if msg is not None and not msg.error():
            traiter_alerte(json.loads(msg.value().decode("utf-8")), "CRITIQUE")

        # 2 Traitement Normal
        msg2 = consumer_normal.poll(0.5)
        if msg2 is not None and not msg2.error():
            traiter_alerte(json.loads(msg2.value().decode("utf-8")), "NORMAL")

        # Petite pause pour ne pas saturer le CPU si aucun message
        time.sleep(0.01)

except KeyboardInterrupt:
    print(f"\nArrêt du service. {len(journal_alertes)} alertes journalisées!")
finally:
    consumer_critique.close()
    consumer_normal.close()