# app/middleware/advanced_logger.py
import time
import logging
from fastapi import Request
from app.core.cai import obtener_fecha_actual, get_security_rule, log_response_info
from app.core.state import app_state

logger = logging.getLogger(__name__)

async def advanced_logger_middleware(request: Request, call_next):
    # Obtener IP origen
    if "x-forwarded-for" in request.headers:
        ip_origen = request.headers["x-forwarded-for"].split(",")[0]
    else:
        ip_origen = request.client.host if request.client else "unknown"
    
    # Obtener servicio (path)
    servicio = request.url.path
    
    # Guardar en request.state
    request.state.ip_origen = ip_origen
    request.state.servicio = servicio
    request.state.start_time = time.time()
    
    # Log antes
    logger.info(f"BEFORE: {request.method} {request.url}")
    logger.info(f"IP Origen: {ip_origen}")
    logger.info(f"Servicio: {servicio}")

    start_time = obtener_fecha_actual()
 
    response = await call_next(request)

    try:
        security_param = get_security_rule(ip_origen, servicio)
        if security_param:
            logger.info(f"SECURULE: {security_param.datos}")
        current_jsonenv = app_state.jsonenv
    except Exception as e:
        logger.error(f'Error en security rule: {e}')

    log_response_info(request, start_time, ip_origen, response.status_code)
    
    # Calcular duración
    process_time = time.time() - request.state.start_time
    
    # Log después
    logger.info(f"AFTER: {request.method} {request.url} - Status: {response.status_code}")
    logger.info(f"Duración: {process_time:.3f}s")
    logger.info("-" * 50)
    
    return response