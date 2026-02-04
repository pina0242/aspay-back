
from app.core.models import DBDGENPERS, DBDIRPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, validar_dato_obligatorio, busca_cp
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import json 
logger = logging.getLogger(__name__)

class DirService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regdir(self, datos , user, db):
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
            param_keys = ['num_id','tipo_dir' , 'direccion', 'cod_postal','ciudad','pais','latitud','longitud']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id     = dataJSON["num_id"]   
                tipo_dir   = dataJSON["tipo_dir"]   
                direccion  = dataJSON["direccion"]
                cod_postal = dataJSON["cod_postal"] 
                ciudad     = dataJSON["ciudad"]   
                pais       = dataJSON["pais"]
                latitud    = dataJSON["latitud"]
                longitud   = dataJSON["longitud"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id': ('long', 10),
                'tipo_dir': ('long', 1),
                'direccion': ('long', 50),
                'cod_postal': ('long', 5),
                'ciudad': ('long', 30),
                'pais': ('long', 15),
                'latitud': ('float', 15),
                'longitud': ('float', 15)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
                    
            # if not valcampo('long',num_id,10):
            #     result.append({'response':'num_id'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',tipo_dir,1):
            #     result.append({'response':'tipo_dir'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',direccion,50):
            #     result.append({'response':'direccion'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',cod_postal,5):
            #     result.append({'response':'cod_postal'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',ciudad,30):
            #     result.append({'response':'ciudad'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc              
            # if not valcampo('long',pais,15):
            #     result.append({'response':'pais'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc           
            # if not valcampo('float',latitud,15):
            #     result.append({'response':'latitud'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('float',longitud,15):
            #     result.append({'response':'longitud'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            if not validar_dato_obligatorio(cod_postal,'string'):
                result.append({'response':'cod_postal no informado'}) 
                estado = False
                rc = 400
                return result , rc  
            if not validar_dato_obligatorio(pais,'string'):
                result.append({'response':'pais no informado'}) 
                estado = False
                rc = 400
                return result , rc  
            #aquicp
        if estado:
            datos_cp = busca_cp(pais,cod_postal,db)
        
            if 'cod_postal' in datos_cp:
                latitud = datos_cp.get('latitud', '')
                longitud = datos_cp.get('longitud', '')
                ciudad = datos_cp.get('ciudad', '')
                estado = True 
        
    # Verificar si la función retornó un diccionario de ERROR
            elif 'response' in datos_cp:
        # Es un mensaje de error
                #result.append(datos_cp)
                estado = False
        # Determinar el código de error apropiado
                error_message = datos_cp['response'].lower()
                if '404' in error_message:
                    result.append(f"{datos_cp} - cp no encontrado")
                    rc = 404
                elif '408' in error_message:
                    result.append(f"{datos_cp} - timeout")
                    rc = 408
                elif '503' in error_message or 'connection' in error_message:
                    result.append(f"{datos_cp} - error conexion")
                    rc = 503
                elif '500' in error_message:
                    result.append(datos_cp)
                    rc = 500
                else:
                    rc = 400
    #Manejar cualquier otro caso donde 'datos_cp' no tiene el formato esperado
            else:
                result.append({'response': 'Error interno: formato de respuesta de API CP no reconocido'})
                estado = False
                rc = 500

        if estado:                         
            per  = db.query(DBDGENPERS).order_by(DBDGENPERS.id.desc()).where(DBDGENPERS.num_id==num_id, DBDGENPERS.estatus=='A').first() 
            if per:
                num_id  = per.num_id
                entidad = per.entidad
            else:
                result.append({'response':'No existe persona'}) 
                estado = False
                rc = 400
                return result , rc   
                        
        if estado:
            try:
            # Buscar si ya existe una dirección activa con el mismo tipo_dir para este num_id
                dir_existente = db.query(DBDIRPERS).where(
                    DBDIRPERS.num_id == num_id,
                    DBDIRPERS.tipo_dir == tipo_dir,
                    DBDIRPERS.estatus == 'A'
                ).first()
            
                if dir_existente:
                  estado = False
                  result.append({'response': f'Ya existe una dirección de tipo {tipo_dir} para esta persona'})
                  rc = 400
                  return result, rc
                
            except Exception as e:
                print('Error en validación de dirección existente:', e)
                estado = False
                result.append({'response': 'Error en validación'})
                rc = 400
                return result, rc
                                  
        if estado :
            try:  
                estatus      = 'A'
                #fecha_alta   = obtener_fecha_actual() 
                usuario_mod  = '' 
                fecha_mod    = datetime.min 
                usuario_alta  = user  
                                                         
                dirp = DBDIRPERS(num_id,entidad,tipo_dir,direccion,cod_postal,ciudad,pais,latitud,longitud,estatus,usuario_alta,fecha_mod,usuario_mod)
                db.add(dirp) 

                if per:
                    per.avanreg = 50  # Actualizar campo avanreg con valor 50
                    per.usuario_mod = user   
                    per.fecha_mod = obtener_fecha_actual()   
                    db.add(per)   

                # Confirmar ambos cambios en la base de datos
                db.commit()

                result.append({'response':'Exito'
                                    })        
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result)
                app_state.jsonenv = message
                #print('message :', message) 
                encripresult , estado = encrypt_message(password, message)
                if estado:
                    return encripresult, rc
                else:
                    result = [{'response': 'Error encriptando la respuesta'}]
                    return result, 500
                #print('encripresult :', encripresult)
                #result = encripresult
            except Exception as e:
                # En caso de error, hacer rollback de la transacción
                db.rollback()
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error Fatal'})            

        return result , rc 
    
    def seldir(self, datos , user, db):
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
            param_keys = ['num_id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id = dataJSON["num_id"]       
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
                                    
        if estado :
            try:  
                dircli = db.query(DBDIRPERS).order_by(DBDIRPERS.id.desc()).where(DBDIRPERS.num_id==num_id, DBDIRPERS.estatus=='A').all() 
                if dircli:
                    for listdir in dircli:
                        #print ('ok')
                        result.append({ 
                            'id':listdir.id,
                            'num_id':listdir.num_id, 
                            'tipo_dir':listdir.tipo_dir, 
                            'direccion':listdir.direccion,
                            'cod_postal':listdir.cod_postal,
                            'ciudad':listdir.ciudad,
                            'pais':listdir.pais,
                            'latitud':listdir.latitud,
                            'longitud':listdir.longitud,
                            'fecha_alta':str(listdir.fecha_alta),
                            'usuario_alta':listdir.usuario_alta ,
                            'fecha_mod':str(listdir.fecha_mod ),
                            'usuario_mod':listdir.usuario_mod                                                                                                          
                                })
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                else:
                    result.append({'response':'No existen datos a listar'})                 
                    rc = 400                    
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error Fatal'})   
        return result , rc 
    
    def deldir(self, datos , user, db):
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
            param_keys = ['num_id','tipo_dir']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id   = dataJSON["num_id"]   
                tipo_dir = dataJSON["tipo_dir"]   
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('longEQ',num_id, 10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                #print('result :', result)
                return result , rc
            if not valcampo('longEQ',tipo_dir, 1):
                result.append({'response':'tipo_dir'})
                estado = False
                rc = 400
                #print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            dir  = db.query(DBDIRPERS).order_by(DBDIRPERS.id.desc()).where(DBDIRPERS.num_id==num_id, DBDIRPERS.tipo_dir==tipo_dir, DBDIRPERS.estatus=='A').first()           
            if dir:
                dir.estatus = 'B'
                dir.fecha_mod = obtener_fecha_actual()
                dir.usuario_mod = user
                db.add(dir)
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
                result.append({'response':'direccion inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc 
    
    def upddir(self, datos , user, db):
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
            param_keys = ['num_id','tipo_dir' , 'direccion', 'cod_postal','ciudad','pais','latitud','longitud']    
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id     = dataJSON["num_id"]   
                tipo_dir   = dataJSON["tipo_dir"]   
                direccion  = dataJSON["direccion"]
                cod_postal = dataJSON["cod_postal"] 
                ciudad     = dataJSON["ciudad"]   
                pais       = dataJSON["pais"]      
                latitud    = dataJSON["latitud"]
                longitud   = dataJSON["longitud"]     
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id': ('long', 10),
                'tipo_dir': ('long', 1),
                'direccion': ('long', 50),
                'cod_postal': ('long', 5),
                'ciudad': ('long', 30),
                'pais': ('long', 15),
                'latitud': ('float', 15),
                'longitud': ('float', 15)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
                    
            # if not valcampo('long',num_id,10):
            #     result.append({'response':'num_id'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',tipo_dir,1):
            #     result.append({'response':'tipo_dir'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',direccion,50):
            #     result.append({'response':'direccion'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',cod_postal,5):
            #     result.append({'response':'cod_postal'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',ciudad,30):
            #     result.append({'response':'ciudad'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc              
            # if not valcampo('long',pais,15):
            #     result.append({'response':'pais'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc      
            # if not valcampo('float',latitud,15):
            #     result.append({'response':'latitud'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('float',longitud,15):
            #     result.append({'response':'longitud'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                                   
            if not validar_dato_obligatorio(cod_postal,'string'):
                result.append({'response':'cod_postal no informado'}) 
                estado = False
                rc = 400
                #print('result :', result)
                return result , rc                               
        
        if estado:
            datos_cp = busca_cp(pais,cod_postal,db)
            print('datos_cp',datos_cp)
        
            if 'cod_postal' in datos_cp:
                latitud_nva = datos_cp.get('latitud', '')
                longitud_nva = datos_cp.get('longitud', '')
                ciudad_nva = datos_cp.get('ciudad', '')
                estado = True 
        
    # Verificar si la función retornó un diccionario de ERROR
            elif 'response' in datos_cp:
        # Es un mensaje de error
                #result.append(datos_cp)
                estado = False
        # Determinar el código de error apropiado
                error_message = datos_cp['response'].lower()
                if '404' in error_message:
                    result.append(f"{datos_cp} - cp no encontrado")
                    rc = 404
                elif '408' in error_message:
                    result.append(f"{datos_cp} - timeout")
                    rc = 408
                elif '503' in error_message or 'connection' in error_message:
                    result.append(f"{datos_cp} - error conexion")
                    rc = 503
                elif '500' in error_message:
                    result.append(datos_cp)
                    rc = 500
                else:
                    rc = 400
    #Manejar cualquier otro caso donde 'datos_cp' no tiene el formato esperado
            else:
                result.append({'response': 'Error interno: formato de respuesta de API CP no reconocido'})
                estado = False
                rc = 500    
                                   
        if estado :                                                          
            dir =  db.query(DBDIRPERS).where(DBDIRPERS.num_id==num_id, DBDIRPERS.tipo_dir==tipo_dir, DBDIRPERS.estatus=='A').first()           
            if dir:
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in dir.__table__.columns:
                        if col.name not in ['id', 'fecha_alta']:
                            campos_originales[col.name] = getattr(dir, col.name)
                
                    campos_originales['estatus'] = 'M'  # eStatus histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    #print('campos_originales :', campos_originales)
                
                    dir_historico = DBDIRPERS(**campos_originales)
                    db.add(dir_historico)

                # 2. ACTUALIZAR registro original (mantener estatus 'A')
                    dir.direccion   = direccion
                    dir.cod_postal  = cod_postal
                    dir.ciudad      = ciudad_nva
                    dir.pais        = pais
                    dir.latitud     = latitud_nva
                    dir.longitud    = longitud_nva
                    dir.usuario_mod = user
                    dir.fecha_mod   = obtener_fecha_actual() 
                    
                    db.add(dir)    
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
                except Exception as e:
                    print ('e',e)
                    result.append({'response':'Error DB'}) 
                    estado = False
                    rc = 400   
            else:
                result.append({'response':'registro inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc
    
    def verifcp(self, datos , user, db):
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

        if estado:
            dataJSON = json.loads(decriptMsg)
            param_keys = ['cod_postal','pais']      
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                cod_postal = dataJSON["cod_postal"] 
                pais       = dataJSON["pais"]           
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        
        if estado:
            if not valcampo('long', cod_postal, 5):
                result.append({'response':'cod_postal'}) 
                estado = False
                rc = 400
                return result, rc  
            if not valcampo('long', pais, 15):
                result.append({'response':'pais'}) 
                estado = False
                rc = 400
                return result, rc                         
            if not validar_dato_obligatorio(cod_postal,'string'):
                result.append({'response':'cod_postal no informado'}) 
                estado = False
                rc = 400
                return result, rc    
            if not validar_dato_obligatorio(pais,'string'):
                result.append({'response':'pais no informado'}) 
                estado = False
                rc = 400
                return result, rc
         #aquicp                              
        if estado:
            datos_cp = busca_cp(pais,cod_postal,db)
        
            if 'cod_postal' in datos_cp:
                result.append(datos_cp)
                estado = True 
                rc = 201
        
    # Verificar si la función retornó un diccionario de ERROR
            elif 'response' in datos_cp:
        # Es un mensaje de error
                #result.append(datos_cp)
                estado = False
        # Determinar el código de error apropiado
                error_message = datos_cp['response'].lower()
                if '404' in error_message:
                    rc = 404
                    result.append(f"{datos_cp} - cp no encontrado")
                elif '408' in error_message:
                    result.append(f"{datos_cp} - timeout")
                    rc = 408
                elif '503' in error_message or 'connection' in error_message:
                    result.append(f"{datos_cp} - error conexion")
                    rc = 503
                elif '500' in error_message:
                    result.append(datos_cp)
                    rc = 500
                else:
                    rc = 400
    #Manejar cualquier otro caso donde 'datos_cp' no tiene el formato esperado
            else:
                result.append({'response': 'Error interno: formato de respuesta de API CP no reconocido'})
                estado = False
                rc = 500       
            
    # Encriptar respuesta si todo fue exitoso
        if estado and rc == 201:
            result.append({'response': 'Exito'})
            message = json.dumps(result)
            app_state.jsonenv = message
            encripresult, estado_enc = encrypt_message(password, message)
            if estado_enc:
                result = encripresult
            else:
                result = [{'response': 'Error al encriptar respuesta'}]
                rc = 500
                            
        return result, rc