"""
python -m venv aspay
.\aspay\Scripts\activate
uvicorn main:app --reload --port 9000

"""
from fastapi import FastAPI , Request , HTTPException , Depends
from app.core.database import Base, engine,  get_db
from app.core.cai import valoath
from app.config.routes import router as config_router
from app.users.routes import router as users_router
from app.roles.routes import router as roles_router
from app.entidad.routes import router as entidad_router
from app.control.routes import router as control_router

from app.operaciones.traspaso.routes import router as operaciones_router_trasp
from app.operaciones.layout.routes import router as operaciones_router_layout
from app.operaciones.files.routes import router as operaciones_router_files
from app.operaciones.pendientes.routes import router as operaciones_router_pendientes
from app.operaciones.procesados.routes import router as operaciones_router_procesados



from app.datgen.routes import router as datgen_router
from app.datcomp.routes import router as datcomp_router
from app.direccion.routes import router as dir_router
from app.relacion.routes import router as rel_router
from app.cuentas.routes import router as cta_router
from app.docum.routes import router as doc_router
from app.kyc.routes import router as kyc_router

from app.categoria.routes import router as categoria_router 
from app.agregadora.routes import router as agregadora_router

from app.auth.routes import auth_router, legacy_router 
from app.auth.session_manager import user_sessions, remove_user_session
from app.middleware.advanced_logger import advanced_logger_middleware
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="ASPAY")

# Configura CORS para el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.middleware("http")(advanced_logger_middleware)


# Ruta raíz
@app.get("/")
async def root(request: Request, db: Session = Depends(get_db)):
    session_id = request.cookies.get("session_id")
    
    if session_id and session_id in user_sessions:
        user_data = user_sessions[session_id]
        email = user_data.get("email")
        logger.info(f'Email de sesión: {email}')
        logger.info(f'Session ID: {session_id}')
        
        response, rc = valoath(email, db)
        
        logger.info(f' RC recibido: {rc}')
        logger.info(f' Response tipo: {type(response)}')
        logger.info(f' Response contenido: {response}')
        
        if rc != 201:
            if session_id in user_sessions:
                remove_user_session(session_id)
            logger.info(f" Usuario no autorizado. Código: {rc}")
            return RedirectResponse('http://localhost:5173/unregistered-user')

        # Debug del response antes de enviar
        response_json_str = json.dumps(response)
        logger.info(f' Response JSON: {response_json_str}')
        logger.info(f' Longitud response: {len(response_json_str)}')
        
        redirect_url = f'http://localhost:5173/auth_callback?data={response_json_str}'
        logger.info(f' Redirigiendo a frontend...')
        
        return RedirectResponse(redirect_url)
    
    logger.info(' No hay sesión activa')
    return RedirectResponse('/auth/login')

# Routers
# Routers YEYE
app.include_router(legacy_router) 
app.include_router(auth_router)    
app.include_router(config_router)
app.include_router(control_router)
app.include_router(users_router)
app.include_router(roles_router)
app.include_router(entidad_router)
app.include_router(operaciones_router_trasp)
app.include_router(operaciones_router_layout)
app.include_router(operaciones_router_files)
app.include_router(operaciones_router_pendientes)
app.include_router(operaciones_router_procesados)
app.include_router(datgen_router)
app.include_router(datcomp_router)
app.include_router(dir_router)
app.include_router(rel_router)
app.include_router(cta_router)
app.include_router(doc_router)
app.include_router(kyc_router)
app.include_router(categoria_router) 
app.include_router(agregadora_router)


