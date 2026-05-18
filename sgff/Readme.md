# Prototype SGFF — Système de Gestion des Feux de Forêt

Ce prototype montre comment appliquer plusieurs tactiques d'architecture (sécurité, performance, disponibilité) sur une chaîne complète : capteurs simulés -> Edge Node -> Kafka -> service de détection -> dashboard.

## Tactiques implémentées
- **Authenticate Actors** (Sécurité) : messages avec token invalide rejetés par l'Edge Node.
- **Prioritize Events** (Performance) : messages critiques envoyés vers un topic Kafka dédié.
- **Prioritize Events (2 phases)** : l'Edge publie d'abord un warning préliminaire, puis une confirmation finale après un court délai.
- **Control Data Rate** (Performance) : messages `ROUTINE` filtrés et non transmis à Kafka.
- **Schedule Resources** (Performance) : le service de détection interroge le topic critique avant le topic normal.
- **Heartbeat** (Disponibilité) : surveillance de l'activité capteurs et détection de capteur silencieux.
- **Maintain Audit Trail** (Sécurité) : journalisation continue des alertes (source, zone, niveau, statut, phase).
- **RBAC / Limit Access** (Sécurité) : dashboard protégé avec rôles `OPERATEUR` et `ADMIN`.

## Pré-requis
- Python 3.10+
- Docker Desktop
- Accès au terminal (PowerShell, CMD ou Bash)

## Activer l'environnement virtuel (venv)
Depuis la racine `sgff` :

```powershell
python -m venv venv
```

PowerShell :

```powershell
.\venv\Scripts\Activate.ps1
```

CMD :

```cmd
venv\Scripts\activate.bat
```

Puis installer les dépendances :

```powershell
pip install -r requirements.txt
```

## Scénario 1 — Process Critical Data First (Services en Docker)
Objectif : démontrer la priorisation des événements critiques avec tous les services en conteneurs.

Terminal 1 (Kafka + MQTT) :

```powershell
cd sgff
docker-compose up
```

Terminal 2 (Dashboard) :

```powershell
python -m uvicorn presentation_layer.main:app --reload
```

Les autres services tournent dans des conteneurs Docker.

## Scénario 1 Alternative — Host-based (Recommandé pour voir les logs)
Objectif : même démonstration, mais les services Python tournent sur la machine hôte pour voir tous les logs clairement.

Terminal 1 (Seulement infrastructure Docker) :

```powershell
cd sgff
docker-compose up mosquitto zookeeper kafka
```

Terminal 2 (Edge Node) :

```powershell
cd sgff
python terrain_layer/edge_node.py
```

Terminal 3 (Service Détection) :

```powershell
python business_layer/detection_service/detection_service.py
```

Terminal 4 (Simulateur capteurs) :

```powershell
python infrastructure/capteur_simulateur.py
```

Terminal 5 (Dashboard avec RBAC) :

```powershell
python -m uvicorn presentation_layer.main:app --reload
```

**Important :** Quand tu exécutes les scripts Python sur l'hôte, la variable `KAFKA_BROKER` défaut à `localhost:29092` automatiquement. La valeur `kafka:9092` est réservée aux conteneurs lancés dans le réseau Docker.

Ce que on dois observer :
- Rejet des messages attaquants (token invalide) → `[SÉCURITÉ] Accès refusé`
- Flux critique traité avant le flux normal → messages sur `sensor-critical` d'abord
- Warning préliminaire puis confirmation finale pour les événements critiques
- Messages MQTT reçus → `[MQTT] Message reçu sur sgff/capteurs: {...}`
- Messages Kafka publiés → `[EDGE] Envoyé vers sensor-critical | CAP-TZ-01`

## Scénario 2 — Login + RBAC 
Objectif : démontrer la tactique `Limit Access`.

Quand les services backend tournent déjà (scénario 1), lancer l'interface RBAC :

```powershell
python -m uvicorn presentation_layer.main:app --reload
```

Puis tester :
- `operateur / foret123` -> accès dashboard uniquement.
- `admin / sgff2026` -> accès dashboard + panneau admin (`/admin`).

## Scénario 3 — Test bascule central <-> P2P
Objectif : vérifier la résilience si la partie centrale devient indisponible.

1. Lancer d'abord le scénario 1.
2. Stopper le conteneur Kafka.
3. Observer les logs Edge Node.
4. Redémarrer Kafka.
5. Vérifier le retour au mode central.


## Combiner les tests
Tu peux enchaîner les 3 scénarios sans tout redémarrer :
- Commencer par le scénario 1.
- Remplacer uniquement la commande du Terminal 5 par celle du scénario 2 pour le RBAC.
- Puis exécuter la manipulation du scénario 3 (arrêt/redémarrage Kafka).

## Résumé des observations attendues
| Observation | Tactique illustrée |
|---|---|
| Message rejeté (token invalide) | Authenticate Actors |
| Critiques sur `sensor-critical` en priorité | Prioritize Events |
| Warning préliminaire + confirmation finale | Prioritize Events |
| Filtrage des messages `ROUTINE` | Control Data Rate |
| Critique interrogé avant normal | Schedule Resources |
| Capteur silencieux détecté | Heartbeat |
| Journal d'alerte horodaté | Maintain Audit Trail |
| Connexion par rôle (`OPERATEUR` / `ADMIN`) | RBAC / Limit Access |