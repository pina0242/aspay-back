from fastapi import APIRouter, Depends, Request ,Response
from fastapi.responses import JSONResponse,PlainTextResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required, get_current_user
from app.core.security import hash_password
from app.core.cai import valrole 
from app.aspay.envio.services import EnvService
from typing import Dict, Any
import json





router = APIRouter()

def get_env_service(db: Session = Depends(get_db)):
    return EnvService(db_session=db)

@router.post("/envio")
async def envio_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    env_service: EnvService = Depends(get_env_service)
):
    body = await request.body()
    datos = body.decode('utf-8')
    ip_origen = request.state.ip_origen
    service = request.state.servicio
    access , user , msg =  valrole(claims,service, ip_origen,datos,db)
    if not access:
        result = []
        result.append({'response':msg})
        return Response(
            content=json.dumps(result),
            status_code=401,
            media_type="application/json"
        )
    response, rc = env_service.envio(datos, db)
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

