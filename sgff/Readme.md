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

## Tester la Redondance Passive (Passive Redundancy)
(simulation du niveau 2: coupure réseau entre les nœuds terrain et le coordinateur central)

### Étape 1 — Vérifier que tout fonctionne normalement
Dans le Terminal 2 vous devez voir : (edge_node est execute au niveau de ce terminal)
```
[EDGE] 🔴 Envoyé vers sensor-critical | CAP-BJ-01 (CRITIQUE)
[EDGE] 🟡 Envoyé vers sensor-qualified | CAP-TZ-01 (NORMAL)
```

### Étape 2 — Simuler une panne du serveur central (arrêter Kafka)
Ouvrir un nouveau terminal et exécuter :
```
docker stop sgff-kafka-1
```

### Étape 3 — Attendre 30 secondes et observer
Dans le Terminal 2 vous devez voir :
```
[P2P] ⚠️  Centrale perdue → Mode P2P activé
[P2P] 🔴 traitement local | CAP-BJ-01 (CRITIQUE)
[P2P] 🟡 traitement local | CAP-TZ-01 (NORMAL)
```
Les alertes continuent d'être traitées localement — le système reste opérationnel malgré la panne.

### Étape 4 — Simuler le retour du serveur central (redémarrer Kafka)
```
docker start sgff-kafka-1
```

### Étape 5 — Observer le retour en mode normal
Dans le Terminal 2 vous devez voir :
```
[P2P] ✅ Centrale retrouvée → Mode CENTRAL réactivé
[EDGE] 🔴 Envoyé vers sensor-critical | CAP-BJ-01 (CRITIQUE)
```
Le système reprend automatiquement la transmission vers Kafka sans intervention humaine.