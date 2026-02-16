# config.py
from pydantic_settings import BaseSettings
from pydantic import ConfigDict
from pathlib import Path
import os

# Encontrar el directorio raíz del proyecto
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent  # app/core -> app -> proyecto raíz
env_path = project_root / '.env'

print(f"Buscando .env en: {env_path}")

class Settings(BaseSettings):
    DATABASE_URL: str
    PASSCYPH: str  
    SECRET_KEY: str
    BANK_EMAIL: str
    BANK_PASS: str
    API_KEY : str
    BASE_URL : str
    DEBUG: bool = True
    
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    JWT_ALGORITHM: str = "HS256"
    PROJECT_NAME: str = "Balquiler API"
    VERSION: str = "1.0.0"

    # Microsoft OAuth
    MICROSOFT_CLIENT_ID: str
    MICROSOFT_CLIENT_SECRET: str
    MICROSOFT_TENANT_ID: str
    MICROSOFT_REDIRECT_URI: str = "http://localhost:9000/getAToken"
    call_bank: str 
    
    model_config = ConfigDict(
        env_file=env_path,
        env_file_encoding="utf-8",
        case_sensitive=True
    )

# Verificar que el archivo .env existe
if env_path.exists():
    print(f"Archivo .env encontrado: {env_path}")
else:
    print(f"Archivo .env NO encontrado en: {env_path}")

try:
    settings = Settings()
    print(f"CONFIGURACIÓN CORRECTA:")
    print(f".env cargado desde: {env_path}")
    print(f"PASSCYPH: {'*' * len(settings.PASSCYPH)}")
    print(f"DATABASE_URL: {settings.DATABASE_URL.split('@')[-1] if '@' in settings.DATABASE_URL else settings.DATABASE_URL}")
    print(f"SECRET_KEY: {'*' * len(settings.SECRET_KEY)}")
except Exception as e:
    print(f"ERROR cargando configuración: {e}")
    raise