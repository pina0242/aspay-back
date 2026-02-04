from fastapi import APIRouter, Depends, Request ,Response
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required
from app.core.cai import valrole 
from app.operaciones.pendientes.services import OperServicePendientes
from typing import Dict, Any
import json


router = APIRouter()

def get_oper_service_pendientes(db: Session = Depends(get_db)):
    return OperServicePendientes(db_session=db)

@router.post("/listsing")
async def listsing_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    pendientes_service: OperServicePendientes = Depends(get_oper_service_pendientes)
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
        

    response, rc = pendientes_service.listsing(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/listsdet")
async def listsdet_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    pendientes_service: OperServicePendientes = Depends(get_oper_service_pendientes)
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
        

    response, rc = pendientes_service.listsdet(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/delsing")
async def delsing_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    pendientes_service: OperServicePendientes = Depends(get_oper_service_pendientes)
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
        

    response, rc = pendientes_service.delsing(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )


@router.post("/ejecarch")
async def ejecarch_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    pendientes_service: OperServicePendientes = Depends(get_oper_service_pendientes)
):
    body = await request.body()
    datos = body.decode('utf-8')
    ip_origen = request.state.ip_origen
    servicio = request.state.servicio
    time_start = request.state.start_time
    access , user , msg =  valrole(claims,servicio, ip_origen,datos,db)
    if not access:
        result = []
        result.append({'response':msg})
        return Response(
            content=json.dumps(result),
            status_code=401,
            media_type="application/json"
        )
        

    response, rc = pendientes_service.ejecarch(datos,db,ip_origen,time_start)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/selproej")
async def selproej_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    pendientes_service: OperServicePendientes = Depends(get_oper_service_pendientes)
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
        

    response, rc = pendientes_service.selproej(datos,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )