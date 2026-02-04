from fastapi import APIRouter, Depends, Request ,Response
from fastapi.responses import JSONResponse,PlainTextResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required, get_current_user
from app.core.security import hash_password
from app.core.cai import valrole 
from app.cuentas.services import CtaService
from typing import Dict, Any
import json


router = APIRouter()

def get_cta_service(db: Session = Depends(get_db)):
    return CtaService(db_session=db)

@router.post("/regcta")
async def regcta_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    cta_service: CtaService = Depends(get_cta_service)
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
        
    response, rc = cta_service.regcta(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/selctas")
async def selctas_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    cta_service: CtaService = Depends(get_cta_service)
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
        
    response, rc = cta_service.selctas(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/updcta")
async def updcta_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    cta_service: CtaService = Depends(get_cta_service)
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
        
    response, rc = cta_service.updcta(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/delcta")
async def delcta_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    cta_service: CtaService = Depends(get_cta_service)
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
        
    response, rc = cta_service.delcta(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )
