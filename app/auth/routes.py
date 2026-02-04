# app/auth/routes.py
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
import uuid
import secrets
import json
import logging

from app.core.msal_config import msal_app, REDIRECT_URI, SCOPE
from app.core.database import get_db
from app.core.cai import valoath
from app.auth.session_manager import (
    user_sessions, 
    oauth_states,
    add_user_session,
    remove_user_session,
    add_oauth_state,
    remove_oauth_state
)

logger = logging.getLogger(__name__)

# Router para rutas con prefix /auth
auth_router = APIRouter(prefix="/auth", tags=["authentication"])

# Router para rutas legacy (sin prefix)
legacy_router = APIRouter(tags=["authentication"])

#  FUNCIÓN COMÚN PARA LOGIN
async def handle_login(prompt: str = None):
    """Función común para manejar el login"""
    logger.info('Iniciar flujo OAuth con Microsoft')
    state = str(uuid.uuid4())
    add_oauth_state(state)
    
    # Configurar parámetros de autorización
    auth_params = {
        "scopes": SCOPE,
        "state": state,
        "redirect_uri": REDIRECT_URI
    }
    
    # Agregar prompt si está presente
    if prompt:
        auth_params["prompt"] = prompt
        logger.info(f" Usando prompt: {prompt}")
    
    auth_url = msal_app.get_authorization_request_url(**auth_params)
    
    return RedirectResponse(auth_url)

#  RUTAS LEGACY (sin prefix)
@legacy_router.get("/login")
async def legacy_login(prompt: str = None):
    """Ruta legacy /login"""
    logger.info(' Llamada a ruta legacy /login')
    return await handle_login(prompt)

@legacy_router.get("/getAToken")
async def legacy_callback(request: Request, db: Session = Depends(get_db)):
    """Ruta legacy /getAToken"""
    logger.info(' Llamada a ruta legacy /getAToken')
    # Redirigir al callback moderno manteniendo los parámetros
    from urllib.parse import urlencode
    params = dict(request.query_params)
    redirect_url = f"/auth/callback?{urlencode(params)}"
    return RedirectResponse(redirect_url)

@legacy_router.get("/logout")
async def legacy_logout(request: Request):
    """Ruta legacy /logout"""
    logger.info(' Llamada a ruta legacy /logout')
    # Redirigir al logout moderno
    return RedirectResponse("/auth/logout")

#  RUTAS CON PREFIX /auth
@auth_router.get("/")
async def index(request: Request, db: Session = Depends(get_db)):
    session_id = request.cookies.get("session_id")
    
    if session_id and session_id in user_sessions:
        user_data = user_sessions[session_id]
        email = user_data.get("email")
        logger.info(f' Email de sesión: {email}')
        
        response, rc = valoath(email, db)
        
        if rc != 201:
            if session_id in user_sessions:
                remove_user_session(session_id)
            logger.info(f" Usuario no autorizado. Código: {rc}")
            return RedirectResponse('http://localhost:5173/unregistered-user')

        response_json_str = json.dumps(response)
        redirect_url = f'http://localhost:5173/auth_callback?data={response_json_str}'
        
        return RedirectResponse(redirect_url)
    
    logger.info(' No hay sesión activa')
    return RedirectResponse('/auth/login')

@auth_router.get("/login")
async def login(prompt: str = None):
    """
    Iniciar flujo OAuth con Microsoft - Ruta moderna
    """
    return await handle_login(prompt)

@auth_router.get("/callback")
async def authorized(request: Request, db: Session = Depends(get_db)):
    """
    Callback de Microsoft OAuth - Ruta moderna
    """
    logger.info(" Llegó al callback de auth")
    
    state = request.query_params.get('state')
    code = request.query_params.get('code')
    
    logger.info(f" State: {state}")
    logger.info(f" Code: {code}")
    
    # Validar estado
    if state not in oauth_states:
        logger.info(" Estado inválido")
        raise HTTPException(status_code=400, detail="Estado inválido")
    
    if not code:
        logger.info(" Código no proporcionado")
        raise HTTPException(status_code=400, detail="Código no proporcionado")
    
    # Obtener token
    logger.info(" Obteniendo token de Microsoft...")
    result = msal_app.acquire_token_by_authorization_code(
        code,
        scopes=SCOPE,
        redirect_uri=REDIRECT_URI
    )
    
    if "access_token" in result:
        user_email = result["id_token_claims"].get("preferred_username") or result["id_token_claims"].get("email")
        user_name = result["id_token_claims"].get("name")
        
        logger.info(f" Usuario autenticado: {user_email}")
        
        # Crear sesión
        session_id = secrets.token_urlsafe(32)
        user_data = {
            "name": user_name,
            "email": user_email,
            "access_token": result["access_token"]
        }
        add_user_session(session_id, user_data)
        
        # Limpiar estado OAuth usado
        remove_oauth_state(state)
        
        # Redirigir a index
        logger.info(" Redirigiendo a / ...")
        response = RedirectResponse(url='/', status_code=302)
        response.set_cookie(
            key="session_id",
            value=session_id,
            httponly=True,
            secure=False,
            samesite="lax",
            max_age=3600,
            path="/",
            domain=None
        )
        
        logger.info(" Cookie establecida")
        return response
        
    else:
        error_description = result.get('error_description', 'Error desconocido')
        logger.info(f" Error en OAuth: {error_description}")
        return RedirectResponse(f'http://localhost:5173/error?message={error_description}')

@auth_router.get("/logout")
async def logout(request: Request):
    """
    Cerrar sesión - Ruta moderna
    """
    session_id = request.cookies.get("session_id")
    
    logger.info(f" Logout - Session ID: {session_id}")
    
    if session_id and session_id in user_sessions:
        remove_user_session(session_id)
    
    # Crear respuesta de redirección
    response = RedirectResponse(url='http://localhost:5173/')
    
    # Eliminar la cookie
    response.delete_cookie("session_id")
    logger.info(" Cookie eliminada")
    
    # Opcional: redirigir al logout de Microsoft
    logout_url = f'https://login.microsoftonline.com/common/oauth2/v2.0/logout?post_logout_redirect_uri=http://localhost:5173/'
    return RedirectResponse(logout_url)