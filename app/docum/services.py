
from app.core.models import DBDGENPERS, DBDOCPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, genfol06
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import json 
import base64

logger = logging.getLogger(__name__)

class DocService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    
    def regdoc(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        # # Configuración de API FoxID
        # FOXID_TOKEN_URL = "https://app.foxid.eu/api/public/security/token/create"
        # FOXID_VALIDATE_URL = "https://app.foxid.eu/api/public/mobile/createAndIdentify"
    
        # # Credenciales FoxID (deberían estar en variables de entorno)
        # FOXID_CREDENTIALS = {
        #     "username": "tu_usuario",
        #     "userSecret": "tu_user_secret",
        #     "clientId": "tu_client_id",
        #     "clientSecret": "tu_client_secret"
        # }
    
        if estado:
            dataJSON = json.loads(decriptMsg)
            param_keys = ['num_id','tipo_docto','pais_emis_docto','fecha_vencim','image_docto','filename']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id          = dataJSON["num_id"]   
                tipo_docto      = dataJSON["tipo_docto"]   
                pais_emis_docto = dataJSON["pais_emis_docto"]
                fecha_vencim    = dataJSON["fecha_vencim"]
                nombre_archivo  = dataJSON["filename"]
                #image_docto     = dataJSON["image_docto"].encode('utf-8')  # Convertir string a bytes
                image_docto_base64 = dataJSON["image_docto"]
                
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
    
        # Validaciones básicas de campos
        if estado:
            if not valcampo('long',num_id,10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                return result, rc
            if not valcampo('long',tipo_docto,15):
                result.append({'response':'tipo_docto'})
                estado = False
                rc = 400
                return result, rc
            if not valcampo('long',pais_emis_docto,2):
                result.append({'response':'pais_emis_docto'}) 
                estado = False
                rc = 400
                return result, rc  
            if not valcampo('date',fecha_vencim,10):
                result.append({'response':'fecha_vencim'}) 
                estado = False
                rc = 400
                return result, rc 
            if not valcampo('long',nombre_archivo,255):
                result.append({'response':'nombre_archivo'}) 
                estado = False
                rc = 400
                return result, rc
            
    
        # Verificar existencia de persona
        if estado:                         
            per = db.query(DBDGENPERS).order_by(DBDGENPERS.id.desc()).where(
                                                    DBDGENPERS.num_id == num_id, 
                                                    DBDGENPERS.estatus == 'A'
                                                    ).first() 
            if per:
                num_id  = per.num_id
                entidad = per.entidad
            else:
                result.append({'response':'No existe persona'}) 
                estado = False
                rc = 400
                return result, rc   
    
        # Verificar documento existente
        if estado:
            try:
                docto_existente = db.query(DBDOCPERS).where(
                    DBDOCPERS.num_id == num_id,
                    DBDOCPERS.tipo_docto == tipo_docto,
                    DBDOCPERS.estatus == 'A'
                ).first()
            
                if docto_existente:
                    estado = False
                    result.append({'response': f'Ya existe un documento de tipo {tipo_docto} para esta persona'})
                    rc = 400
                    return result, rc
                
            except Exception as e:
                print('Error en validación de documento existente:', e)
                estado = False
                result.append({'response': 'Error en validación'})
                rc = 400
                return result, rc

        #  OBTENER TOKEN DE FOXID
    #     foxid_token = None
    #     if estado:
    #         try:
    #             print("Obteniendo token de FoxID...")
    #             response = requests.post(
    #                 FOXID_TOKEN_URL,
    #                 json=FOXID_CREDENTIALS,
    #                 headers={'Content-Type': 'application/json'},
    #                 timeout=30
    #             )
            
    #             if response.status_code == 200:
    #                 token_data = response.json()
    #                 foxid_token = token_data.get('token')
                
    #                 if not foxid_token:
    #                     estado = False
    #                     result.append({'response': 'Error al obtener token de FoxID'})
    #                     rc = 400
    #                     return result, rc
                    
    #                 print("Token obtenido exitosamente")
    #             else:
    #                 estado = False
    #                 result.append({'response': f'Error en API FoxID (token): {response.status_code}'})
    #                 rc = 400
    #                 return result, rc
                
    #         except requests.exceptions.RequestException as e:
    #             estado = False
    #             result.append({'response': f'Error de conexión con FoxID: {str(e)}'})
    #             rc = 400
    #             return result, rc
    #         except Exception as e:
    #             estado = False
    #             result.append({'response': f'Error inesperado al obtener token: {str(e)}'})
    #             rc = 400
    #             return result, rc

    # #  VALIDAR DOCUMENTO CON FOXID
    #     foxid_data = None
    #     if estado and foxid_token:
    #         try:
    #             print("Validando documento con FoxID...")
            
    #         # Preparar datos para FoxID
    #             foxid_payload = {
    #                 "front_document": image_docto,  # Imagen en base64
    #                 "lang": "es"  # Idioma español
    #             }
            
    #             headers = {
    #                 'Content-Type': 'application/json',
    #                 'Authorization': f'Bearer {foxid_token}'
    #             }
            
    #             response = requests.post(
    #                 FOXID_VALIDATE_URL,
    #                 json=foxid_payload,
    #                 headers=headers,
    #                 timeout=60  # Timeout mayor para procesamiento de imagen
    #             )
            
    #             if response.status_code == 200:
    #                 foxid_data = response.json()
    #                 print("Documento validado por FoxID:", foxid_data)
    #             else:
    #                 estado = False
    #                 result.append({'response': f'Error en validación FoxID: {response.status_code}'})
    #                 rc = 400
    #                 return result, rc
                
    #         except requests.exceptions.RequestException as e:
    #             estado = False
    #             result.append({'response': f'Error de conexión en validación FoxID: {str(e)}'})
    #             rc = 400
    #             return result, rc
    #         except Exception as e:
    #             estado = False
    #             result.append({'response': f'Error inesperado en validación: {str(e)}'})
    #             rc = 400
    #             return result, rc

    # #  PROCESAR RESPUESTA DE FOXID Y PREPARAR DATOS
    #     if estado and foxid_data:
    #         try:
    #         # Extraer datos de la respuesta de FoxID
    #             numero_docto = foxid_data.get('document_number', '')
    #             nombre_archivo = f"docto_{num_id}_{tipo_docto}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
    #         # Procesar fechas (ajustar según formato de FoxID)
    #             fecha_caducidad_docto = None
            
    #             if 'expiry_date' in foxid_data:
    #                  try:
    #                     fecha_caducidad_docto = datetime.strptime(foxid_data['expiry_date'], '%Y-%m-%d')
    #                  except:
    #                     pass
            
    #         # Determinar estatus basado en validación FoxID
    #             estatus_validacion_docto = 'RECHAZADO'
    #             motivo_rechazo = 'Validación FoxID fallida'
            
    #             if foxid_data.get('validation_status') == 'VALID':
    #                 estatus_validacion_docto = 'VALIDADO'
    #                 motivo_rechazo = None
    #             elif foxid_data.get('validation_status') == 'WARNING':
    #                 estatus_validacion_docto = 'OBSERVADO'
    #                 motivo_rechazo = foxid_data.get('validation_notes', 'Observaciones en validación')
            
    #             #fecha_validac_docto = datetime.now()
            
    #         # Campos de compliance europeo
    #             ind_verificado_electronico = foxid_data.get('electronic_verification', False)
    #             nivel_autenticacion = foxid_data.get('authentication_level', 'baja')
    #             sello_tiempo = datetime.now() if ind_verificado_electronico else None
            
    #         # Decodificar imagen
    #             image_docto_binary = base64.b64decode(image_docto)
            
    #         except Exception as e:
    #             estado = False
    #             result.append({'response': f'Error procesando respuesta FoxID: {str(e)}'})
    #             rc = 400
    #             return result, rc

    #  GUARDAR EN BASE DE DATOS
        if estado:
            try:  
                estatus        = 'A'
                #fecha_alta = obtener_fecha_actual() 
                usuario_mod    = ' ' 
                fecha_mod      = datetime.min
                usuario_alta   = user  
                # Se informarán datos en cuanto sepamos cómo se validarán los doctos
                numero_docto   = genfol06(db)
                nombre_archivo = nombre_archivo
                documento_bytes = base64.b64decode(image_docto_base64)
                #fecha_caducidad_docto      = datetime.max
                fecha_caducidad_docto      = fecha_vencim
                estatus_validacion_docto   = 'POR VALIDAR'
                motivo_rechazo             = ' '
                ind_verificado_electronico = True
                nivel_autenticacion        = 'Baja'
                sello_tiempo = obtener_fecha_actual()
            
            # Crear registro del documento
                regDoc = DBDOCPERS(
                    num_id=num_id,
                    entidad = entidad,
                    tipo_docto=tipo_docto,
                    pais_emis_docto=pais_emis_docto,
                    numero_docto=numero_docto,
                    nombre_archivo=nombre_archivo,
                    image_docto=documento_bytes,
                    fecha_caducidad_docto=fecha_caducidad_docto,
                    estatus=estatus,
                    estatus_validacion_docto=estatus_validacion_docto,
                    motivo_rechazo=motivo_rechazo,
                    ind_verificado_electronico=ind_verificado_electronico,
                    nivel_autenticacion=nivel_autenticacion,
                    sello_tiempo=sello_tiempo,
                    usuario_alta=usuario_alta,
                    fecha_mod=fecha_mod,
                    usuario_mod=usuario_mod
                )
            
                db.add(regDoc)

            # Actualizar progreso de registro si es necesario
                if per:
                    per.avanreg     = 100
                    per.usuario_mod = user   
                    per.fecha_mod   = obtener_fecha_actual()   
                    db.add(per)   

            # Confirmar cambios
                db.commit()

            # Preparar respuesta exitosa
                result.append({
                    'response': 'exito',
                    'foxid_validation': estatus_validacion_docto,
                    'document_number': numero_docto
                                })        
                rc = 201 
            
                message = json.dumps(result)
                app_state.jsonenv = message 
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            
            except Exception as e:
                db.rollback()
                print('Error al guardar documento:', e)
                rc = 400  
                result.append({'response': 'Error fatal al guardar documento'})            

        return result, rc 
    
    def seldoc(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['num_id', 'tipo_docto']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id     = dataJSON.get("num_id")
                tipo_docto = dataJSON.get("tipo_docto")       
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:                      
            if not valcampo('long',num_id,10):
                result.append({'response':'num_id'}) 
                estado = False
                rc = 400
                return result , rc                      
            if not valcampo('long',tipo_docto,15):
                result.append({'response':'tipo_docto'}) 
                estado = False
                rc = 400
                return result , rc                          
                                    
        if estado :
            try:  
                doc = db.query(DBDOCPERS).order_by(DBDOCPERS.id.desc()).where(DBDOCPERS.num_id == num_id,
                                                                                DBDOCPERS.estatus == 'A').first()
                if doc:
                    #print ('ok')
                    docto_base64 = base64.b64encode(doc.image_docto).decode('utf-8')
                    fecha_venc_str = doc.fecha_caducidad_docto.isoformat() if isinstance(doc.fecha_caducidad_docto, (date, datetime)) else str(doc.fecha_caducidad_docto)
                    fecha_alta_str = doc.fecha_alta.isoformat() if isinstance(doc.fecha_alta, (date, datetime)) else str(doc.fecha_alta)
                    result.append({ 
                            'num_id': doc.num_id,
                            'tipo_docto': doc.tipo_docto,
                            'nombre_archivo': doc.nombre_archivo,
                            'image_docto': docto_base64,
                            'estatus_validacion_docto': doc.estatus_validacion_docto,
                            'motivo_rechazo': doc.motivo_rechazo,
                            'fecha_caducidad_docto': fecha_venc_str,
                            'fecha_alta': fecha_alta_str,                                                                                                         
                                })
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                else:
                    result.append({'response':'No existe documento a mostrar'})                 
                    rc = 400                    
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error Fatal'})   
        return result , rc
    
    def deldoc(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            #print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['num_id','tipo_docto']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id     = dataJSON["num_id"]
                tipo_docto = dataJSON["tipo_docto"]   
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('long',num_id, 10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc
            if not valcampo('long',tipo_docto, 15):
                result.append({'response':'tipo_docto'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            doc  = db.query(DBDOCPERS).order_by(DBDOCPERS.id.desc()).where(DBDOCPERS.num_id==num_id,
                                                                                DBDOCPERS.tipo_docto==tipo_docto,
                                                                                DBDOCPERS.estatus=='A').first()           
            if doc:
                doc.estatus     = 'B'
                doc.fecha_mod   = obtener_fecha_actual()
                doc.usuario_mod = user
                db.add(doc)
                db.commit()
                result.append({'response':'Exito'
                                })        
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result)
                app_state.jsonenv = message 
                #print('message :', message) 
                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'documento inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc 
    