
from app.core.models import DBCATEG
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import  func
from datetime import timedelta, datetime
import logging
import json 
logger = logging.getLogger(__name__)
class CategService:

    
    def __init__(self, db_session=None):
        self.db = db_session
    
    
    def listcateg(self,datos, db):
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
            param_keys = ['categoria']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                 
                categoria = dataJSON["categoria"]                                                                              
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
 
        if estado :    
            if  categoria == 'ALL': 
                                             
                categoriaok = db.query(DBCATEG).order_by(DBCATEG.id.desc()).where(
                    DBCATEG.status=='A').all() 
            else:
                categoriaok = db.query(DBCATEG).order_by(DBCATEG.id.desc()).where(
                    DBCATEG.categoria ==categoria,
                    DBCATEG.status=='A').all() 

            if categoriaok:
                for selcategoria in categoriaok:
                    print ('ok')
                    result.append({ 
                        'id':selcategoria.id,
                        'entidad':selcategoria.entidad, 
                        'categoria':selcategoria.categoria, 
                        'nombre':selcategoria.nombre, 
                        'status':selcategoria.status,
                        'fecha_alta':str(selcategoria.fecha_alta),
                        'usuario_alt':selcategoria.usuario_alt                                                                                                     
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
        return result , rc 

    def regcateg(self,datos,user, db):
        result = []
        estado = True
        status = 'A'
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
            param_keys = ['entidad','categoria' , 'nombre']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:   
                entidad = dataJSON["entidad"]                
                categoria = dataJSON["categoria"]   
                nombre = dataJSON["nombre"]  
                          
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'entidad': ('int', 4),
                'categoria': ('int', 20),
                'nombre': ('long', 256),
                'status': ('float', 1)
            }
                                     
                                    
        if validations :
            usuario_alt  =  user                                    
            regcateg = DBCATEG(entidad,categoria,nombre,status,usuario_alt)
            db.add(regcateg)
            db.commit() 
            result.append({'response':'Exito'
                                })        
            rc = 201 
            #print('result :', result) 
            message = json.dumps(result) 
            app_state.jsonenv = message

            encripresult , estado = encrypt_message(password, message)
            #print('encripresult :', encripresult)
            result = encripresult
      
        return result , rc 
      
    def updcateg(self,datos,user, db):
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
        print(' decriptMsg:', decriptMsg)
        if estado :
            dataJSON = json.loads(decriptMsg)
            param_keys = ['id','entidad','categoria','nombre']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                id = dataJSON["id"]
                entidad = dataJSON["entidad"] 
                categoria = dataJSON["categoria"] 
                nombre = dataJSON["nombre"] 
     

        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'id': ('float', 1),
                'entidad': ('long', 4),
                'categoria': ('long', 20),
                'nombre': ('long', 256),

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
            updcateg =  db.query(DBCATEG).where(DBCATEG.id==id,DBCATEG.status=='A').first()            
            if updcateg:
                campos_originales = {}
                for col in updcateg.__table__.columns:
                    if col.name not in ['id', 'fecha_alta']:
                        campos_originales[col.name] = getattr(updcateg, col.name)
                
                campos_originales['status'] = 'M'  # Status histórico
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico

                print('campos_originales :', campos_originales)
                
                cta_hist = DBCATEG(**campos_originales)
                db.add(cta_hist)


                updcateg.id  = id
                updcateg.entidad     = entidad
                updcateg.categoria   = categoria
                updcateg.nombre      = nombre
                updcateg.status      = 'A'
                updcateg.usuario_alt = user
            
                db.add(updcateg)  
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
                rc = 400                            
        return result , rc

    def delcateg(self,datos,user, db):
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
            param_keys = ['id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                id = dataJSON["id"]             
        else:
            estado = False
            result.append({'response':'Información incorrecta'}) 
            rc = 400
        if estado:
            if not valcampo('float',id,1):
                result.append({'response':'idnivtran'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            delcateg =  db.query(DBCATEG).where(DBCATEG.id==id,DBCATEG.status=='A').first()            
            if delcateg:
                campos_originales = {}
                for col in delcateg.__table__.columns:
                    if col.name not in ['id', 'fecha_alta']:
                        campos_originales[col.name] = getattr(delcateg, col.name)
                
                campos_originales['status'] = 'B'  # Status histórico
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico

                print('campos_originales :', campos_originales)
                
                cta_hist = DBCATEG(**campos_originales)
                db.add(cta_hist)


                db.delete(delcateg) 
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
                rc = 400                            
        return result , rc 