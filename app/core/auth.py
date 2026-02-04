from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import jwt  # PyJWT
from jwt import InvalidTokenError, ExpiredSignatureError
from app.core.config import settings
from sqlalchemy import  text
from app.core.database import get_db

security = HTTPBearer()

# Función principal de validación JWT
async def jwt_required(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    Dependencia principal para validar JWT
    """

    try:
        token = credentials.credentials
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.JWT_ALGORITHM]  
        )

        return payload
    except ExpiredSignatureError:
        try:
                payload = jwt.decode(
                    token, 
                    settings.SECRET_KEY, 
                    algorithms=[settings.JWT_ALGORITHM],
                    options={"verify_exp": False}
                )
        except InvalidTokenError:

                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
                    

        clvcol = payload.get('clvcol')
        if not clvcol:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Datos de usuario incompletos en el token")



        db_generator = get_db()

        try:
          
            db = next(db_generator)

            sql_query_sel = """
            SELECT EXTRACT(EPOCH FROM (NOW() - timestar)) / 60 AS minutos_transcurridos
            FROM public."DBLOGENTRY"
            WHERE "nombre" = :nombre
            ORDER BY EXTRACT(EPOCH FROM timestar) - EXTRACT(EPOCH FROM NOW()) DESC
            LIMIT 1;
            """
            

            resultado_cursor = db.execute(
                text(sql_query_sel), 
                {'nombre': clvcol} 
            )
            

            fila = resultado_cursor.fetchone()
            
        
            minutos = fila[0] if fila else 999

            print(f" Query ejecutado con éxito.")
            print(f"Minutos transcurridos: {minutos}")
  
        finally:
            # YIELD NOTA investigarlo mas a fondo Diana
            try:
                next(db_generator)
            except StopIteration:

                pass
        if minutos > 5: 
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expirado"
        
            )
        else:
            
            return payload

    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Error de autenticación: {str(e)}"
        )

# Alias para mantener compatibilidad
get_jwt_claims = jwt_required

async def get_current_user(
    claims: Dict[str, Any] = Depends(jwt_required)
) -> Dict[str, Any]:
    """
    Dependencia para obtener el usuario actual
    """
    return {
        "id": claims.get("sub"),
        "username": claims.get("username"),
        "roles": claims.get("roles", []),
        "email": claims.get("email"),
        "user_id": claims.get("user_id"),
        "clvcol": claims.get("clvcol"),
        "role": claims.get("role"),
        "entidad": claims.get("entidad")
    }

def create_access_token(
    identity: str,
    additional_claims: Dict[str, Any] = None,
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Crea un JWT token de acceso 
    """
    #  Usar datetime.now(timezone.utc) en lugar de utcnow() deprecado
    now = datetime.now(timezone.utc)
    
    if expires_delta:
        expire = now + expires_delta
    else:
        #  Usar la configuración centralizada
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    # Claims base del token
    payload = {
        "sub": identity,  # Subject
        "exp": expire,    # Expiration
        "iat": now,       # Issued at (usando now)
        "type": "access"  # Token type
    }
    
    # Agregar claims adicionales si se proporcionan
    if additional_claims:
        payload.update(additional_claims)
    
    # Codificar el token
    token = jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    
    return token
