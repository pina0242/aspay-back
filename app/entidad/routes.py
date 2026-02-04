from fastapi import APIRouter, Depends, Request ,Response
from fastapi.responses import JSONResponse,PlainTextResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required, get_current_user
from app.core.security import hash_password
from app.core.cai import valrole 
from app.entidad.services import EntService
from typing import Dict, Any
import json





router = APIRouter()

def get_ent_service(db: Session = Depends(get_db)):
    return EntService(db_session=db)

@router.post("/regent")
async def regent_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    ent_service: EntService = Depends(get_ent_service)
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
    response, rc = ent_service.regent(datos, db)
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/listent")
async def listent_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    ent_service: EntService = Depends(get_ent_service)
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
    response, rc = ent_service.listent(datos, db)
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/updent")
async def updent_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    ent_service: EntService = Depends(get_ent_service)
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
        

    response, rc = ent_service.updent(datos, user,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/delent")
async def delent_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    ent_service: EntService = Depends(get_ent_service)
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
        

    response, rc = ent_service.delent(datos, user,db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )