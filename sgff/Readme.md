# Prototype SGFF — Système de Gestion des Feux de Forêt

## Tactiques démontrées
- **Authenticate Actors** (Sécurité) : messages sans token valide rejetés par l'Edge Node
- **Prioritize Events** (Performance) : alertes critiques dans un topic Kafka dédié
- **Heartbeat** (Disponibilité) : détection automatique de panne capteur
- **Maintain Audit Trail** (Sécurité) : journalisation de chaque alerte
- **Schedule Resources** (Performance) : critiques traités avant normaux
- **Control data rate** (Performance) : messages ROUTINE 
  non transmis à Kafka — réduit la charge du pipeline central

## Lancement (4 terminaux)

### Terminal 1 — Démarrer Kafka + MQTT
```
docker-compose up
```
Attendre ~30 secondes que Kafka démarre.

### Terminal 2 — Démarrer l'Edge Node
```
pip install -r requirements.txt
python edge_node.py
```

### Terminal 3 — Démarrer le Service de Détection
```
python detection_service.py
```

### Terminal 4 — Lancer le simulateur de capteurs
```
python capteur_simulateur.py
```
### Terminal 5 — Dashboard
```
python -m uvicorn dashboard:app --reload
```

## Ce qu'on observe

| Ce qui se passe | Tactique illustrée |
|---|---|
| "Message rejeté — token invalide" dans edge_node | Authenticate Actors |
| Messages CRITIQUE → topic sensor-critical en priorité | Prioritize Events |
| "Capteur silencieux depuis Xs" si on coupe le simulateur | Heartbeat |
| Alertes niveau 1/2/3 avec horodatage dans detection_service | Maintain Audit Trail |
| Critiques traités avant normaux dans detection_service | Schedule Resources |
| Messages ROUTINE filtrés, non transmis à Kafka | Control data rate |
| Résumé horodaté par zone toutes les 3s | Maintain Audit Trail |