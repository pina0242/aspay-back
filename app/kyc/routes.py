from fastapi import APIRouter, Depends, Request ,Response
from fastapi.responses import JSONResponse,PlainTextResponse
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.auth import jwt_required, get_current_user
from app.core.security import hash_password
from app.core.cai import valrole 
from app.kyc.services import KycService
from typing import Dict, Any
import json


router = APIRouter()

def get_kyc_service(db: Session = Depends(get_db)):
    return KycService(db_session=db)

@router.post("/regkyc")
async def regkyc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.regkyc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/selkyc")
async def selkyc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.selkyc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )


@router.post("/delkyc")
async def delkyc_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.delkyc(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )



@router.post("/califopc1")
async def califopc1_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.califopc1(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/califopc23")
async def califopc23_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.califopc23(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/califopc4")
async def califopc4_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.califopc4(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/califopc5")
async def califopc5_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.califopc5(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/regcalif")
async def regcalif_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.regcalif(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )

@router.post("/selcalif")
async def selcalif_endpoint(
    request: Request, 
    claims: Dict[str, Any] = Depends(jwt_required),
    db: Session = Depends(get_db),  
    kyc_service: KycService = Depends(get_kyc_service)
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
        
    response, rc = kyc_service.selcalif(datos, user, db)
    
    return Response(
            content=json.dumps(response),
            status_code=rc,
            media_type="application/json"
        )


