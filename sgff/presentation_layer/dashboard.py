from confluent_kafka import Consumer
import json
import time
from datetime import datetime
import asyncio
import threading
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# Configuration
BROKER         = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"

# Etat Global
clients_connectes: list[WebSocket] = []
alerte_queue: asyncio.Queue = None


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

# LOGIQUE KAFKA (Thread separe) 

def consommer_kafka(loop):
    """Consomme Kafka et injecte dans la queue asyncio avec resilience."""
    conf = {
        "bootstrap.servers": BROKER,
        "group.id": "dashboard-final-group",
        "auto.offset.reset": "latest"
    }

    selected_conf = None

    while True:
        for candidate in iter_kafka_bootstraps():
            test_consumer = None
            try:
                candidate_conf = {**conf, "bootstrap.servers": candidate}
                test_consumer = Consumer(candidate_conf)
                test_consumer.list_topics(timeout=2.0)
                test_consumer.close()
                print(f"[DASHBOARD] Connecte au bus Kafka via {candidate}")
                selected_conf = candidate_conf
                break
            except Exception as e:
                print(f"[DASHBOARD] Attente de Kafka sur {candidate}... ({e})")
                if test_consumer is not None:
                    try:
                        test_consumer.close()
                    except Exception:
                        pass

        if selected_conf is not None:
            break

        time.sleep(5)

    consumer = Consumer(selected_conf)
    consumer.subscribe([TOPIC_CRITIQUE, TOPIC_NORMAL])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            mesure = json.loads(msg.value().decode("utf-8"))
            type_alerte = "CRITIQUE" if msg.topic() == TOPIC_CRITIQUE else "NORMAL"
            status = mesure.get("alerte_status", "")
            phase = mesure.get("phase", "")

            # On n'affiche que les messages importants : warnings critiques, confirmations et faux positifs.
            if type_alerte == "NORMAL" and not status:
                continue

            payload = {
                "type": type_alerte,
                "phase": phase,
                "status": status,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "zone": mesure.get("zone", "?"),
                "capteur_id": mesure.get("capteur_id", "?"),
                "temperature": mesure.get("temperature"),
                "fumee": mesure.get("fumee"),
                "niveau": mesure.get("niveau", "?"),
                "message": f"{mesure.get('capteur_id')} — {mesure.get('temperature')}°C | Fumee: {mesure.get('fumee')}"
            }

            asyncio.run_coroutine_threadsafe(alerte_queue.put(payload), loop)
    finally:
        consumer.close()

# LIFESPAN

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alerte_queue
    alerte_queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=consommer_kafka, args=(loop,), daemon=True)
    thread.start()
    print("Dashboard SGFF - http://localhost:8000")
    yield

app = FastAPI(lifespan=lifespan)

# -- ROUTES & WEBSOCKET --
@app.get("/direct", response_class=HTMLResponse)
async def index():
    return HTML

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients_connectes.append(websocket)
    try:
        while True:
            alerte = await alerte_queue.get()

            # [REDUCE OVERHEAD] Diffusion parallele a tous les clients simultanement
            # au lieu d'envoyer un par un, on envoie a tous en meme temps
            print(f"[REDUCE OVERHEAD] Diffusion parallele vers {len(clients_connectes)} client(s) connecte(s)")
            async def envoyer(client):
                try:
                    await client.send_text(json.dumps(alerte))
                except:
                    clients_connectes.remove(client)
            await asyncio.gather(*[envoyer(c) for c in list(clients_connectes)])

    except WebSocketDisconnect:
        if websocket in clients_connectes:
            clients_connectes.remove(websocket)

# -- PAGE HTML ORIGINALE --
HTML = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>SGFF - Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; padding: 24px; }
        h1 { color: #ff6b35; font-size: 1.4rem; margin-bottom: 6px; }
        .subtitle { color: #666; font-size: 0.85rem; margin-bottom: 24px; }
        .zones { display: flex; gap: 16px; margin-bottom: 28px; flex-wrap: wrap; }
        .zone-card { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 10px; padding: 16px 20px; min-width: 200px; transition: border-color 0.3s; }
        .zone-card.critique { border-color: #e53935; }
        .zone-card.normal   { border-color: #fb8c00; }
        .zone-card.ok       { border-color: #43a047; }
        .zone-name { font-weight: bold; font-size: 1rem; margin-bottom: 8px; }
        .zone-temp { font-size: 1.6rem; font-weight: bold; }
        .zone-temp.critique { color: #e53935; }
        .zone-temp.normal   { color: #fb8c00; }
        .zone-temp.ok       { color: #43a047; }
        .zone-meta { font-size: 0.75rem; color: #888; margin-top: 4px; }
        h2 { font-size: 1rem; color: #aaa; margin-bottom: 12px; }
        #alertes { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 10px; padding: 16px; max-height: 420px; overflow-y: auto; }
        .alerte { display: flex; align-items: center; gap: 12px; padding: 10px 12px; border-radius: 6px; margin-bottom: 8px; animation: fadein 0.4s ease; }
        @keyframes fadein { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; transform: translateY(0); } }
        .alerte.CRITIQUE { background: rgba(229,57,53,0.15); border-left: 3px solid #e53935; }
        .alerte.NORMAL   { background: rgba(251,140,0,0.12); border-left: 3px solid #fb8c00; }
        .alerte.SECURITE { background: rgba(156,39,176,0.15); border-left: 3px solid #9c27b0; }
        .alerte-heure  { font-size: 0.75rem; color: #666; min-width: 64px; }
        .alerte-zone   { font-weight: bold; min-width: 130px; }
        .alerte-detail { font-size: 0.85rem; color: #bbb; flex: 1; }
        .alerte-niveau { font-size: 0.72rem; font-weight: bold; padding: 3px 8px; border-radius: 12px; }
        .alerte.CRITIQUE .alerte-niveau { background: #e53935; color: white; }
        .alerte.NORMAL   .alerte-niveau { background: #fb8c00; color: white; }
        .alerte.SECURITE .alerte-niveau { background: #9c27b0; color: white; }
        #statut { margin-top: 16px; font-size: 0.75rem; color: #444; text-align: right; }
    </style>
</head>
<body>
    <h1>SGFF - Systeme de Gestion des Feux de Foret</h1>
    <p class="subtitle">Dashboard temps reel - Tizi Ouzou · Bejaia · Blida</p>
    <div class="zones">
        <div class="zone-card ok" id="zone-Tizi Ouzou">
            <div class="zone-name">Tizi Ouzou</div>
            <div class="zone-temp ok" id="temp-Tizi Ouzou">--C</div>
            <div class="zone-meta" id="meta-Tizi Ouzou">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Bejaia">
            <div class="zone-name">Bejaia</div>
            <div class="zone-temp ok" id="temp-Bejaia">--C</div>
            <div class="zone-meta" id="meta-Bejaia">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Blida">
            <div class="zone-name">Blida</div>
            <div class="zone-temp ok" id="temp-Blida">--C</div>
            <div class="zone-meta" id="meta-Blida">En attente...</div>
        </div>
    </div>
    <h2>Flux d'alertes en temps reel</h2>
    <div id="alertes"></div>
    <div id="statut">Connexion WebSocket...</div>
    <script>
        const ws = new WebSocket("ws://" + window.location.host + "/ws");
        ws.onopen = () => { document.getElementById("statut").textContent = "Connecte — alertes en temps reel"; };
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            afficherAlerte(data);
            if (data.zone) mettreAJourZone(data);
        };
        ws.onclose = () => { document.getElementById("statut").textContent = "Connexion perdue"; };
        function afficherAlerte(data) {
            const div = document.createElement("div");
            div.className = `alerte ${data.type || "NORMAL"}`;
            div.innerHTML = `
                <span class="alerte-heure">${data.timestamp}</span>
                <span class="alerte-zone">${data.zone || "?"}</span>
                <span class="alerte-detail">${data.message || ""}</span>
                <span class="alerte-niveau">${data.status || data.niveau || data.type}</span>
            `;
            const container = document.getElementById("alertes");
            container.insertBefore(div, container.firstChild);
            while (container.children.length > 50) container.removeChild(container.lastChild);
        }
        function mettreAJourZone(data) {
            const zone = data.zone;
            const css  = data.type === "CRITIQUE" ? "critique" : data.type === "NORMAL" ? "normal" : "ok";
            const card = document.getElementById(`zone-${zone}`);
            if (!card) return;
            card.className = `zone-card ${css}`;
            const tempEl = document.getElementById(`temp-${zone}`);
            tempEl.className = `zone-temp ${css}`;
            tempEl.textContent = `${data.temperature}C`;
            document.getElementById(`meta-${zone}`).textContent = `Fumee: ${data.fumee} | ${data.timestamp}`;
        }
    </script>
</body>
</html>
"""

# ══════════════════════════════════════════════════════
# AJOUT : fonction pour main.py (Tactique RBAC)
# Cette fonction est utilisee par main.py uniquement.
# Elle genere le dashboard avec le login et role affiches.
# Le reste de ce fichier n'est pas modifie.
# ══════════════════════════════════════════════════════

def get_dashboard_html(login: str, role: str) -> str:
    """
    Genere le HTML du dashboard avec le nom et role de l'utilisateur.
    Appelee par main.py apres verification du login (RBAC).
    """
    admin_link = '<a href="/admin" style="color:#ff6b35;font-size:0.8rem;margin-left:16px">Panel Admin</a>' if role == "ADMIN" else ""
    return f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>SGFF - Dashboard</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; padding: 24px; }}
        .topbar {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }}
        h1 {{ color: #ff6b35; font-size: 1.4rem; }}
        .user-info {{ font-size:0.8rem; color:#666; }}
        .role-badge {{ display:inline-block; padding:2px 10px; border-radius:10px; font-size:0.75rem;
                       font-weight:bold; background:{"#e53935" if role == "ADMIN" else "#1565c0"}; color:white; margin-left:8px; }}
        .subtitle {{ color: #666; font-size: 0.85rem; margin-bottom: 24px; }}
        .zones {{ display: flex; gap: 16px; margin-bottom: 28px; flex-wrap: wrap; }}
        .zone-card {{ background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 10px; padding: 16px 20px; min-width: 200px; transition: border-color 0.3s; }}
        .zone-card.critique {{ border-color: #e53935; }}
        .zone-card.normal   {{ border-color: #fb8c00; }}
        .zone-card.ok       {{ border-color: #43a047; }}
        .zone-name {{ font-weight: bold; font-size: 1rem; margin-bottom: 8px; }}
        .zone-temp {{ font-size: 1.6rem; font-weight: bold; }}
        .zone-temp.critique {{ color: #e53935; }}
        .zone-temp.normal   {{ color: #fb8c00; }}
        .zone-temp.ok       {{ color: #43a047; }}
        .zone-meta {{ font-size: 0.75rem; color: #888; margin-top: 4px; }}
        h2 {{ font-size: 1rem; color: #aaa; margin-bottom: 12px; }}
        #alertes {{ background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 10px; padding: 16px; max-height: 420px; overflow-y: auto; }}
        .alerte {{ display: flex; align-items: center; gap: 12px; padding: 10px 12px; border-radius: 6px; margin-bottom: 8px; animation: fadein 0.4s ease; }}
        @keyframes fadein {{ from {{ opacity: 0; transform: translateY(-6px); }} to {{ opacity: 1; transform: translateY(0); }} }}
        .alerte.CRITIQUE {{ background: rgba(229,57,53,0.15); border-left: 3px solid #e53935; }}
        .alerte.NORMAL   {{ background: rgba(251,140,0,0.12); border-left: 3px solid #fb8c00; }}
        .alerte-heure  {{ font-size: 0.75rem; color: #666; min-width: 64px; }}
        .alerte-zone   {{ font-weight: bold; min-width: 130px; }}
        .alerte-detail {{ font-size: 0.85rem; color: #bbb; flex: 1; }}
        .alerte-niveau {{ font-size: 0.72rem; font-weight: bold; padding: 3px 8px; border-radius: 12px; }}
        .alerte.CRITIQUE .alerte-niveau {{ background: #e53935; color: white; }}
        .alerte.NORMAL   .alerte-niveau {{ background: #fb8c00; color: white; }}
        #statut {{ margin-top: 16px; font-size: 0.75rem; color: #444; text-align: right; }}
        a.logout {{ color: #666; font-size:0.8rem; text-decoration:none; }}
        a.logout:hover {{ color: #e53935; }}
    </style>
</head>
<body>
    <div class="topbar">
        <h1>SGFF - Systeme de Gestion des Feux de Foret</h1>
        <div class="user-info">
            {login} <span class="role-badge">{role}</span>
            {admin_link}
            <a href="/logout" class="logout" style="margin-left:16px">Deconnexion</a>
        </div>
    </div>
    <p class="subtitle">Dashboard temps reel - Tizi Ouzou · Bejaia · Blida</p>
    <div class="zones">
        <div class="zone-card ok" id="zone-Tizi Ouzou">
            <div class="zone-name">Tizi Ouzou</div>
            <div class="zone-temp ok" id="temp-Tizi Ouzou">--C</div>
            <div class="zone-meta" id="meta-Tizi Ouzou">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Bejaia">
            <div class="zone-name">Bejaia</div>
            <div class="zone-temp ok" id="temp-Bejaia">--C</div>
            <div class="zone-meta" id="meta-Bejaia">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Blida">
            <div class="zone-name">Blida</div>
            <div class="zone-temp ok" id="temp-Blida">--C</div>
            <div class="zone-meta" id="meta-Blida">En attente...</div>
        </div>
    </div>
    <h2>Flux d'alertes en temps reel</h2>
    <div id="alertes"></div>
    <div id="statut">Connexion WebSocket...</div>
    <script>
        const ws = new WebSocket("ws://" + window.location.host + "/ws");
        ws.onopen = () => {{ document.getElementById("statut").textContent = "Connecte - alertes en temps reel"; }};
        ws.onmessage = (event) => {{
            const data = JSON.parse(event.data);
            afficherAlerte(data);
            if (data.zone) mettreAJourZone(data);
        }};
        ws.onclose = () => {{ document.getElementById("statut").textContent = "Connexion perdue"; }};
        function afficherAlerte(data) {{
            const div = document.createElement("div");
            div.className = `alerte ${{data.type || "NORMAL"}}`;
            div.innerHTML = `
                <span class="alerte-heure">${{data.timestamp}}</span>
                <span class="alerte-zone">${{data.zone || "?"}}</span>
                <span class="alerte-detail">${{data.message || ""}}</span>
                <span class="alerte-niveau">${{data.niveau || data.type}}</span>
            `;
            const container = document.getElementById("alertes");
            container.insertBefore(div, container.firstChild);
            while (container.children.length > 50) container.removeChild(container.lastChild);
        }}
        function mettreAJourZone(data) {{
            const zone = data.zone;
            const css  = data.type === "CRITIQUE" ? "critique" : data.type === "NORMAL" ? "normal" : "ok";
            const card = document.getElementById(`zone-${{zone}}`);
            if (!card) return;
            card.className = `zone-card ${{css}}`;
            const tempEl = document.getElementById(`temp-${{zone}}`);
            tempEl.className = `zone-temp ${{css}}`;
            tempEl.textContent = `${{data.temperature}}C`;
            document.getElementById(`meta-${{zone}}`).textContent = `Fumee: ${{data.fumee}} | ${{data.timestamp}}`;
        }}
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)