from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import base64
import os


def encrypt_message(password: str, message: str) -> tuple[str, bool]:
    """
    Encripta un mensaje usando AES-CBC con PBKDF2 de forma congruente con el frontend
    
    Args:
        password: Contraseña para derivar la clave
        message: Mensaje a encriptar
        
    Returns:
        tuple: (mensaje_encriptado_en_base64, estado_operacion)
    """
    try:
        # Validación de entrada
        if not password or not message:
            return "error: password and message required", False

        # 1. Generar componentes aleatorios
        salt = os.urandom(16)  # 16 bytes para salt
        iv = os.urandom(16)    # 16 bytes para CBC (no 12 como en GCM)

        # 2. Derivar clave usando PBKDF2
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256 bits
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive(password.encode())

        # 3. Aplicar padding PKCS7 manualmente
        pad_length = 16 - (len(message.encode()) % 16)
        padded_message = message.encode() + bytes([pad_length] * pad_length)

        # 4. Configurar cifrador CBC
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()

        # 5. Cifrar el mensaje
        ciphertext = encryptor.update(padded_message) + encryptor.finalize()

        # 6. Combinar componentes: salt (16) + iv (16) + ciphertext
        combined = salt + iv + ciphertext

        # 7. Codificar en Base64 URL-safe (sin padding)
        encrypted_data = base64.urlsafe_b64encode(combined).decode().rstrip('=')
        
        return encrypted_data, True

    except Exception as e:
        print(f"Encryption error: {str(e)}")
        return f"error: {str(e)}", False


def decrypt_message(password: str, encrypted_data: str) -> tuple[str, bool]:
    try:
        # 1. Decodificar Base64 URL-safe (agregar padding '=' si es necesario)
        padding = len(encrypted_data) % 4
        if padding:
            encrypted_data += '=' * (4 - padding)
        decoded = base64.urlsafe_b64decode(encrypted_data)

        # 2. Extraer componentes (salt:16, iv:16, ciphertext:resto)
        salt = decoded[:16]
        iv = decoded[16:32]
        ciphertext = decoded[32:]

        # 3. Derivar clave con PBKDF2
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = kdf.derive(password.encode())

        # 4. Descifrar con AES-CBC
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(ciphertext) + decryptor.finalize()

        # 5. Remover padding PKCS#7 manualmente
        pad_length = decrypted[-1]
        decrypted = decrypted[:-pad_length]  # Eliminar bytes de padding

        # 6. Decodificar a UTF-8 y retornar
        return decrypted.decode('utf-8'), True

    except Exception as e:
        print(e)
        estado = False
        return 'error', estado
        #raise ValueError(f"Error de descifrado: {str(e)}")