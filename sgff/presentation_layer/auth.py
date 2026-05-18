"""
auth.py
-------
Tactique : Limit Access / RBAC
Gère l'authentification et les rôles des utilisateurs.

Rôles :
- OPERATEUR : accès au dashboard temps réel uniquement
- ADMIN     : accès au dashboard + panneau de gestion
"""

import uuid
from datetime import datetime
from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse

# ══════════════════════════════════════════════════════
# RBAC : Utilisateurs et leurs rôles
# ══════════════════════════════════════════════════════
UTILISATEURS = {
    "operateur": {"mot_de_passe": "foret123", "role": "OPERATEUR"},
    "admin":     {"mot_de_passe": "sgff2026", "role": "ADMIN"},
}

# Sessions actives : token → {login, role, connected_at}
sessions_actives: dict = {}


def creer_session(login: str, role: str) -> str:
    """Crée un token de session unique pour l'utilisateur connecté."""
    token = str(uuid.uuid4())
    sessions_actives[token] = {
        "login": login,
        "role": role,
        "connected_at": datetime.now().strftime("%H:%M:%S")
    }
    print(f"[RBAC]  Connexion autorisée — {login} ({role})")
    return token


def get_session(request: Request):
    """Récupère la session depuis le cookie, ou None si non connecté."""
    token = request.cookies.get("session_token")
    return sessions_actives.get(token)


def verifier_login(login: str, mot_de_passe: str):
    """Vérifie les credentials. Retourne le rôle si valide, None sinon."""
    utilisateur = UTILISATEURS.get(login)
    if not utilisateur or utilisateur["mot_de_passe"] != mot_de_passe:
        print(f"[RBAC] ⛔ Accès refusé — identifiants invalides pour '{login}'")
        return None
    return utilisateur["role"]


# ── Pages HTML ────────────────────────────────────────

LOGIN_HTML = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>SGFF — Connexion</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0;
               display: flex; justify-content: center; align-items: center; height: 100vh; }
        .card { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 12px;
                padding: 40px; width: 360px; }
        h2 { color: #ff6b35; margin-bottom: 8px; font-size: 1.3rem; }
        .subtitle { color: #666; font-size: 0.82rem; margin-bottom: 28px; }
        label { font-size: 0.82rem; color: #aaa; display: block; margin-bottom: 6px; }
        input { width: 100%; background: #0f1117; border: 1px solid #2a2d3a; border-radius: 6px;
                color: #e0e0e0; padding: 10px 12px; font-size: 0.9rem; margin-bottom: 16px; }
        button { width: 100%; background: #ff6b35; color: white; border: none;
                 border-radius: 6px; padding: 11px; font-size: 0.95rem; cursor: pointer; margin-top: 4px; }
        button:hover { background: #e55a24; }
        .hint { margin-top: 20px; font-size: 0.75rem; color: #444;
                border-top: 1px solid #2a2d3a; padding-top: 14px; }
        .hint span { color: #666; }
        .erreur { color: #e53935; text-align: center; margin-top: 12px; font-size: 0.85rem; }
    </style>
</head>
<body>
    <div class="card">
        <h2> SGFF — Accès Sécurisé</h2>
        <p class="subtitle">Système de Gestion des Feux de Forêt</p>
        <form method="post" action="/login">
            <label>Identifiant</label>
            <input type="text" name="login" placeholder="operateur ou admin" required>
            <label>Mot de passe</label>
            <input type="password" name="mot_de_passe" placeholder="••••••••" required>
            <button type="submit">Se connecter</button>
        </form>
        <!-- ERREUR -->
        <div class="hint">
            <span>Rôles disponibles :</span><br>
            operateur → alertes temps réel<br>
            admin → alertes + panneau de gestion
        </div>
    </div>
</body>
</html>
"""


def get_admin_html(sessions: dict) -> str:
    """Génère le panneau admin avec la liste des sessions actives."""
    lignes = "".join([
        f"<tr><td>{v['login']}</td><td>{v['role']}</td><td>{v['connected_at']}</td></tr>"
        for v in sessions.values()
    ])
    return f"""
    <html><head><meta charset='UTF-8'>
    <style>
        body {{ font-family: sans-serif; background: #0f1117; color: #e0e0e0; padding: 32px; }}
        h2 {{ color: #ff6b35; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 10px; border: 1px solid #2a2d3a; text-align: left; }}
        th {{ background: #1a1d27; color: #ff6b35; }}
        a {{ color: #ff6b35; }}
    </style></head>
    <body>
        <h2>⚙️ Panneau Admin — Sessions actives</h2>
        <table>
            <tr><th>Login</th><th>Rôle</th><th>Connecté à</th></tr>
            {lignes}
        </table>
        <br><a href="/">← Retour au dashboard</a>
    </body></html>
    """