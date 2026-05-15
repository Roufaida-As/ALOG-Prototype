import asyncio
import json
import threading
import time
import os
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from confluent_kafka import Consumer

# --- Configuration ---
BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_CRITIQUE = "sensor-critical"
TOPIC_NORMAL   = "sensor-qualified"

# --- État Global ---
clients_connectes: list[WebSocket] = []
alerte_queue: asyncio.Queue = None

# ── LOGIQUE KAFKA (Thread séparé) ──────────────────────────────────────────

def consommer_kafka(loop):
    """Consomme Kafka et injecte dans la queue asyncio avec résilience."""
    conf = {
        "bootstrap.servers": BROKER,
        "group.id": "dashboard-final-group",
        "auto.offset.reset": "latest"
    }

    consumer = None
    while consumer is None:
        try:
            consumer = Consumer(conf)
            consumer.list_topics(timeout=2.0)
            print("[DASHBOARD] ✅ Connecté au bus Kafka")
        except Exception as e:
            print(f"[DASHBOARD] ⏳ Attente de Kafka sur {BROKER}... ({e})")
            time.sleep(5)

    consumer.subscribe([TOPIC_CRITIQUE, TOPIC_NORMAL])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            mesure = json.loads(msg.value().decode("utf-8"))
            type_alerte = "CRITIQUE" if msg.topic() == TOPIC_CRITIQUE else "NORMAL"

            payload = {
                "type": type_alerte,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "zone": mesure.get("zone", "?"),
                "capteur_id": mesure.get("capteur_id", "?"),
                "temperature": mesure.get("temperature"),
                "fumee": mesure.get("fumee"),
                "niveau": mesure.get("niveau", "?"),
                "message": f"{mesure.get('capteur_id')} — {mesure.get('temperature')}°C | Fumée: {mesure.get('fumee')}"
            }

            asyncio.run_coroutine_threadsafe(alerte_queue.put(payload), loop)
    finally:
        consumer.close()

# ── LIFESPAN ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alerte_queue
    alerte_queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=consommer_kafka, args=(loop,), daemon=True)
    thread.start()
    print("=== Dashboard SGFF — http://localhost:8000 ===")
    yield

app = FastAPI(lifespan=lifespan)

# ── ROUTES & WEBSOCKET ──────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients_connectes.append(websocket)
    try:
        while True:
            alerte = await alerte_queue.get()
            # Diffusion à tous les clients
            for client in clients_connectes:
                try:
                    await client.send_text(json.dumps(alerte))
                except:
                    pass
    except WebSocketDisconnect:
        clients_connectes.remove(websocket)

# ── VOTRE PAGE HTML ORIGINALE (STYLE & COMPOSANTS CONSERVÉS) ───────────────
HTML = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>SGFF — Dashboard</title>
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
    <h1>SGFF — Système de Gestion des Feux de Forêt</h1>
    <p class="subtitle">Dashboard temps réel — Tizi Ouzou · Béjaïa · Blida</p>
    <div class="zones">
        <div class="zone-card ok" id="zone-Tizi Ouzou">
            <div class="zone-name">Tizi Ouzou</div>
            <div class="zone-temp ok" id="temp-Tizi Ouzou">--°C</div>
            <div class="zone-meta" id="meta-Tizi Ouzou">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Béjaïa">
            <div class="zone-name">Béjaïa</div>
            <div class="zone-temp ok" id="temp-Béjaïa">--°C</div>
            <div class="zone-meta" id="meta-Béjaïa">En attente...</div>
        </div>
        <div class="zone-card ok" id="zone-Blida">
            <div class="zone-name">Blida</div>
            <div class="zone-temp ok" id="temp-Blida">--°C</div>
            <div class="zone-meta" id="meta-Blida">En attente...</div>
        </div>
    </div>
    <h2>Flux d'alertes en temps réel</h2>
    <div id="alertes"></div>
    <div id="statut">Connexion WebSocket...</div>
    <script>
        const ws = new WebSocket("ws://" + window.location.host + "/ws");
        ws.onopen = () => { document.getElementById("statut").textContent = "✅ Connecté — alertes en temps réel"; };
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            afficherAlerte(data);
            if (data.zone) mettreAJourZone(data);
        };
        ws.onclose = () => { document.getElementById("statut").textContent = "❌ Connexion perdue"; };
        function afficherAlerte(data) {
            const div = document.createElement("div");
            div.className = `alerte ${data.type || "NORMAL"}`;
            div.innerHTML = `
                <span class="alerte-heure">${data.timestamp}</span>
                <span class="alerte-zone">${data.zone || "?"}</span>
                <span class="alerte-detail">${data.message || ""}</span>
                <span class="alerte-niveau">${data.niveau || data.type}</span>
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
            tempEl.textContent = `${data.temperature}°C`;
            document.getElementById(`meta-${zone}`).textContent = `Fumée: ${data.fumee} | ${data.timestamp}`;
        }
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)