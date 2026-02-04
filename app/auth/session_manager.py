# app/auth/session_manager.py
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Almacenamiento de sesiones (en producción usa Redis)
user_sessions: Dict[str, Dict[str, Any]] = {}
oauth_states: Dict[str, str] = {}

def get_user_sessions() -> Dict[str, Dict[str, Any]]:
    return user_sessions

def get_oauth_states() -> Dict[str, str]:
    return oauth_states

def add_user_session(session_id: str, user_data: Dict[str, Any]):
    user_sessions[session_id] = user_data
    logger.info(f" Sesión agregada: {session_id}")

def remove_user_session(session_id: str):
    if session_id in user_sessions:
        del user_sessions[session_id]
        logger.info(f" Sesión eliminada: {session_id}")

def add_oauth_state(state: str):
    oauth_states[state] = state
    logger.info(f" Estado OAuth agregado: {state}")

def remove_oauth_state(state: str):
    if state in oauth_states:
        del oauth_states[state]
        logger.info(f" Estado OAuth eliminado: {state}")