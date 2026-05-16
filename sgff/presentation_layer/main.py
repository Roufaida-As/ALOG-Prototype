import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from auth import (
    creer_session, get_session, verifier_login,
    sessions_actives, LOGIN_HTML, get_admin_html
)

# Importe l'app FastAPI depuis dashboard.py
from dashboard import app, get_dashboard_html

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    session = get_session(request)
    if not session:
        return RedirectResponse(url="/login")
    return HTMLResponse(get_dashboard_html(session["login"], session["role"]))


# ── Routes RBAC ajoutées par-dessus l'app originale ──

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """[RBAC] Page de connexion."""
    if get_session(request):
        return RedirectResponse(url="/")
    return HTMLResponse(LOGIN_HTML)


@app.post("/login")
async def login_submit(
    request: Request,
    login: str = Form(...),
    mot_de_passe: str = Form(...)
):
    """[RBAC] Verifie les credentials et ouvre une session."""
    role = verifier_login(login, mot_de_passe)
    if not role:
        return HTMLResponse(LOGIN_HTML.replace(
            "<!-- ERREUR -->",
            '<p class="erreur">Identifiants invalides</p>'
        ))
    token = creer_session(login, role)
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie("session_token", token, httponly=True)
    return response


@app.get("/logout")
async def logout(request: Request):
    """[RBAC] Deconnexion — supprime la session."""
    token = request.cookies.get("session_token")
    if token and token in sessions_actives:
        login = sessions_actives[token]["login"]
        del sessions_actives[token]
        print(f"[RBAC] Deconnexion — {login}")
    response = RedirectResponse(url="/login")
    response.delete_cookie("session_token")
    return response


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    """[RBAC] Panneau admin — accessible uniquement par les ADMIN."""
    session = get_session(request)
    if not session:
        return RedirectResponse(url="/login")
    if session["role"] != "ADMIN":
        print(f"[RBAC] Acces /admin refuse — {session['login']} est OPERATEUR")
        return HTMLResponse(
            "<h2 style='color:red;font-family:sans-serif;padding:40px'>"
            "Acces refuse — Reserve aux administrateurs</h2>"
        )
    return HTMLResponse(get_admin_html(sessions_actives))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)