from fastapi import APIRouter, Depends, Request ,Response
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required
from app.core.cai import valrole 
from app.operaciones.procesados.services import OperServiceProcesados
from typing import Dict, Any
import json


router = APIRouter()

def get_oper_service_procesados(db: Session = Depends(get_db)):
    return OperServiceProcesados(db_session=db)

@router.post("/listproc")
async def listproc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    procesados_service: OperServiceProcesados = Depends(get_oper_service_procesados)
):
    body = await request.body()
    datos = body.decode('utf-8')
    ip_origen = request.state.ip_origen
    servicio = request.state.servicio
    print('entre')
    access , user , msg =  valrole(claims,servicio, ip_origen,datos,db)
    if not access:
        result = []
        result.append({'response':msg})
        return Response(
            content=json.dumps(result),
            status_code=401,
            media_type="application/json"
        )
        

    response, rc = procesados_service.listproc(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/listpdet")
async def listpdet_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    procesados_service: OperServiceProcesados = Depends(get_oper_service_procesados)
):
    body = await request.body()
    datos = body.decode('utf-8')
    ip_origen = request.state.ip_origen
    servicio = request.state.servicio
    access , user , msg =  valrole(claims,servicio, ip_origen,datos,db)
    if not access:
        result = []
        result.append({'response':msg})
        return Response(
            content=json.dumps(result),
            status_code=401,
            media_type="application/json"
        )
        

    response, rc = procesados_service.listpdet(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )



