
from app.core.models import DBENTIDAD
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import  func
import logging
import json 



logger = logging.getLogger(__name__)


class EntService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regent(self, datos , db): 
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            
            decriptMsg , estado = decrypt_message(password, message)
            print('decriptMsg :', decriptMsg)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['entidad' , 'nombre']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   
            else:                                                           
                entidad =  dataJSON["entidad"]
                nombre  =  dataJSON["nombre"]
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400
        if estado :
            validations = {
            'entidad': ('long', 8),
            'nombre': ('long', 60)
            }
            for key, (tipo, limite) in validations.items():
                if not valcampo(tipo, dataJSON[key], limite):
                    response_key = key
                    result = [{'response': response_key}]
                    rc = 400
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    return result, rc  
        if estado:
            status = 'A'  
            usuario_alta  =  app_state.user_id
            fecha_alta   = obtener_fecha_actual()      
            fecha_mod = '0001-01-01'
            usuario_mod =''
            regEnt = DBENTIDAD(entidad,nombre,status,fecha_alta,usuario_alta,fecha_mod,usuario_mod)
            db.add(regEnt) 
            db.commit()

            result.append({'response':'Exito'
                                })        
            rc = 201 
            message = json.dumps(result)
            app_state.jsonenv = message
            encripresult , estado = encrypt_message(password, message)
            result = encripresult
        return result , rc  
    
    def listent(self,datos, db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg

        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['fecha_inicio' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})      
                message = json.dumps(result)
                app_state.jsonenv = message                  
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_inicio  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400
        if estado:
            validations = {
                'fecha_inicio': ('date', 10),  
                'fecha_fin': ('date', 10)  
            }
            for key, (tipo, limite) in validations.items():
                if not valcampo(tipo, dataJSON[key], limite):
                    response_key = key
                    result = [{'response': response_key}]
                    rc = 400
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    return result, rc  
            
        if estado :                                                
            entidad = db.query(DBENTIDAD).filter(
                func.date(DBENTIDAD.fecha_alta).between(fecha_inicio, fecha_fin),
                DBENTIDAD.status == 'A' 
            ).order_by(
                DBENTIDAD.id.desc()
            ).all()

            if entidad:
                for selent in entidad:
                    #print ('ok')
                    result.append({ 
                        'id':selent.id,
                        'entidad':selent.entidad, 
                        'nombre':selent.nombre,
                        'fecha_alta':str(selent.fecha_alta)                                                                                                       
                            })
                rc = 201 
                message = json.dumps(result) 
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'})                  
                message = json.dumps(result)
                app_state.jsonenv = message   
                rc = 400         
        return result , rc 

    def updent(self,datos, user, db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')

        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg
            #print('decriptMsg :', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                id = dataJSON["id"]
                entidad = dataJSON["entidad"] 
                nombre = dataJSON["nombre"] 
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            validations = {
                'id': ('float', 1),
                'entidad': ('long', 8),
                'nombre': ('long', 50)
            }
            for key, (tipo, limite) in validations.items():
                if not valcampo(tipo, dataJSON[key], limite):
                    response_key = key
                    result = [{'response': response_key}]
                    rc = 400
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    return result, rc  
            
        if estado :                                                          
            updEnt =  db.query(DBENTIDAD).where(DBENTIDAD.id==id , DBENTIDAD.status=='A').first()            
            if updEnt:

                campos_originales = {}
                for col in updEnt.__table__.columns:
                    if col.name not in ['id','usuario_alta','fecha_alta']:
                        campos_originales[col.name] = getattr(updEnt, col.name)
                campos_originales['usuario_alta'] = user
                campos_originales['status'] = 'H'
                print('campos_originales :', campos_originales)
                
                cred_historico = DBENTIDAD(**campos_originales)
                db.add(cred_historico)

                updEnt.entidad = entidad
                updEnt.nombre  = nombre
                updEnt.usuario_mod  = app_state.user_id
                updEnt.fecha_mod  = obtener_fecha_actual()
                db.add(updEnt) 
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
                result.append({'response':'registro inexistente'}) 
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message   
                rc = 400                            
        return result , rc
    
    def delent(self,datos, user,db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')

        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decriptMsg , estado = decrypt_message(password, message)
            app_state.jsonrec = decriptMsg
            #print('decriptMsg :', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                       
                rc = 400 
            else:                
                id = dataJSON["id"]             
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            if not valcampo('float',id,1):
                result.append({'response':'id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc  
            

        if estado :                                                          
            delEnt =  db.query(DBENTIDAD).where(DBENTIDAD.id==id , DBENTIDAD.status=='A').first()            
            if delEnt:

                campos_originales = {}
                for col in delEnt.__table__.columns:
                    if col.name not in ['id','fecha_alta']:
                        campos_originales[col.name] = getattr(delEnt, col.name)
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico
                campos_originales['tipo'] = 'B'
                print('campos_originales :', campos_originales)
                
                cred_historico = DBENTIDAD(**campos_originales)
                db.add(cred_historico)
                db.delete(delEnt)

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
                result.append({'response':'registro inexistente'}) 
                message = json.dumps(result)
                app_state.jsonenv = message   
                estado = False
                rc = 400                            
        return result , rc
    
    def selent(entidad,session):                         
        selentidad = session.query(DBENTIDAD).filter(
            DBENTIDAD.entidad==entidad,
            DBENTIDAD.status == 'A' 
        ).order_by(
            DBENTIDAD.id.desc()
        ).all()

        if selentidad:
            return True    
        else:
            return False
