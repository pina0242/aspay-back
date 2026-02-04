# app/core/security.py - Versión temporal con hashlib
import hashlib
import secrets
import base64

def hash_password(password: str) -> str:
    """
    Hash seguro de password usando PBKDF2 (compatible y seguro)
    """
    # Generar un salt único
    salt = secrets.token_bytes(32)
    # Usar PBKDF2 para hashing (más seguro que SHA256 solo)
    hashed = hashlib.pbkdf2_hmac(
        'sha256', 
        password.encode('utf-8'), 
        salt, 
        100000  # 100,000 iteraciones
    )
    # Combinar salt y hash para almacenamiento
    storage = salt + hashed
    # Codificar en base64 para almacenar como string
    return f"pbkdf2_sha256$100000${base64.b64encode(storage).decode('utf-8')}"

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verificar password contra su hash
    """
    try:
        if not hashed_password.startswith("pbkdf2_sha256$100000$"):
            # Si no es nuestro formato, asumir que es texto plano (para migración)
            return plain_password == hashed_password
        
        # Extraer y decodificar los datos
        encoded_storage = hashed_password.split('$')[2]
        storage = base64.b64decode(encoded_storage.encode('utf-8'))
        
        # Extraer salt (primeros 32 bytes) y hash almacenado (resto)
        salt = storage[:32]
        stored_hash = storage[32:]
        
        # Calcular hash del password proporcionado
        computed_hash = hashlib.pbkdf2_hmac(
            'sha256', 
            plain_password.encode('utf-8'), 
            salt, 
            100000
        )
        
        # Comparar de forma segura contra timing attacks
        return secrets.compare_digest(computed_hash, stored_hash)
        
    except Exception as e:
        print(f" Error en verify_password: {e}")
        return False