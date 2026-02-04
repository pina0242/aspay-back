from fastapi import APIRouter, Depends, Request ,Response
from fastapi.responses import JSONResponse,PlainTextResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required, get_current_user
from app.core.security import hash_password
from app.core.cai import valrole 
from app.docum.services import DocService
from typing import Dict, Any
import json


router = APIRouter()

def get_doc_service(db: Session = Depends(get_db)):
    return DocService(db_session=db)

@router.post("/regdoc")
async def regdoc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    doc_service: DocService = Depends(get_doc_service)
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
        
    response, rc = doc_service.regdoc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/seldoc")
async def seldoc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    doc_service: DocService = Depends(get_doc_service)
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
        
    response, rc = doc_service.seldoc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/deldoc")
async def deldoc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    doc_service: DocService = Depends(get_doc_service)
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
        
    response, rc = doc_service.deldoc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )
