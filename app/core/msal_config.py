import msal
import os
from app.core.config import settings  # Importar tu configuración

# Configuración de Microsoft desde variables de entorno
CLIENT_ID = settings.MICROSOFT_CLIENT_ID
CLIENT_SECRET = settings.MICROSOFT_CLIENT_SECRET
TENANT_ID = settings.MICROSOFT_TENANT_ID
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
REDIRECT_URI = settings.MICROSOFT_REDIRECT_URI
SCOPE = ['User.Read']

# MSAL Client
msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID, 
    authority=AUTHORITY, 
    client_credential=CLIENT_SECRET
)

# Debug para verificar que se cargaron correctamente
print(f"MSAL Configurado:")
print(f"Client ID: {CLIENT_ID[:10]}...")
print(f"Tenant ID: {TENANT_ID}")
print(f"Redirect URI: {REDIRECT_URI}")