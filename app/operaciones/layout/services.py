
from app.core.models import DBLAYOUT
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.entidad.services import EntService
from app.core.state import app_state
from sqlalchemy import  func
import logging
import json 



logger = logging.getLogger(__name__)


class LayoutService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def reglayout(self, datos , db): 
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
            param_keys = ['entidad' , 'llave', 'clave', 'datos_in']
            if not all (key in dataJSON for key in param_keys):
                estado = False   
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   
            else:                                                           
                entidad =  dataJSON["entidad"]
                llave  =  dataJSON["llave"]
                clave  =  dataJSON["clave"]
                datos_in  =  dataJSON["datos_in"]
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        estado = False   
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        rc = 400   

            else:
                if entidad_usu != '0001':
                    estado = False   
                    result.append({'response':'No existe entidad'})    
                    message = json.dumps(result)
                    app_state.jsonenv = message                                 
                    rc = 400   
        if estado:
            if llave != 'MASIHEAD':
                estado = False   
                result.append({'response':'La llave siempre es MASIHEAD'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   

            
        if estado :
            validations = {
            'entidad': ('long', 8),
            'llave': ('long', 8),
            'clave': ('long', 15),
            'datos_in': ('long', 150)
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
            usuario_mod =''
            fecha_mod = '0001-01-01'
            regLay = DBLAYOUT(entidad,llave,clave,datos_in,status,usuario_alta,fecha_mod,usuario_mod)
            db.add(regLay) 
            db.commit()

            result.append({'response':'Exito'
                                })        
            rc = 201 
            message = json.dumps(result)
            app_state.jsonenv = message
            encripresult , estado = encrypt_message(password, message)
            result = encripresult
        return result , rc  
    
    def listlayout(self,datos, db):
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
            param_keys = ['entidad','fecha_ini' , 'fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})      
                message = json.dumps(result)
                app_state.jsonenv = message                  
                rc = 400 
            else:     
                entidad = dataJSON["entidad"]              
                fecha_ini = dataJSON["fecha_ini"]   
                fecha_fin = dataJSON["fecha_fin"]                                                                              
                print ('fecs :', fecha_ini  , fecha_fin )
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        estado = False   
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        rc = 400   

            else:
                if entidad_usu != '0001':
                    estado = False   
                    result.append({'response':'No existe entidad'})    
                    message = json.dumps(result)
                    app_state.jsonenv = message                                 
                    rc = 400   


        if estado:
            validations = {
                'entidad': ('long', 8),  
                'fecha_ini': ('date', 10),  
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
            layout = db.query(DBLAYOUT).filter(
                DBLAYOUT.entidad == entidad,
                func.date(DBLAYOUT.fecha_alta).between(fecha_ini, fecha_fin),
                DBLAYOUT.status == 'A' 
            ).order_by(
                DBLAYOUT.id.asc(),
                DBLAYOUT.clave.asc()
            ).all()

            if layout:
                for sellay in layout:
                    #print ('ok')
                    result.append({ 
                        'id':sellay.id,
                        'entidad':sellay.entidad, 
                        'llave':sellay.llave,
                        'clave':sellay.clave,
                        'datos':sellay.datos                                                                                                       
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

    def updlayout(self,datos, user, db):
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
            param_keys = ['id','entidad','llave','clave','datos_in']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})    
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                id = dataJSON["id"]
                entidad = dataJSON["entidad"] 
                llave = dataJSON["llave"] 
                clave = dataJSON["clave"] 
                datos_in = dataJSON["datos_in"] 
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        estado = False   
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        rc = 400   

            else:
                if entidad_usu != '0001':
                    estado = False   
                    result.append({'response':'No existe entidad'})    
                    message = json.dumps(result)
                    app_state.jsonenv = message                                 
                    rc = 400   

        if estado:
            if llave != 'MASIHEAD':
                estado = False   
                result.append({'response':'La llave siempre es MASIHEAD'})    
                message = json.dumps(result)
                app_state.jsonenv = message                                 
                rc = 400   

        if estado:
            validations = {
                'id': ('float', 1),
                'entidad': ('long', 8),
                'llave': ('long', 8),
                'clave': ('long', 15),
                'datos_in': ('long', 150)
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
            updLay =  db.query(DBLAYOUT).where(DBLAYOUT.id==id , DBLAYOUT.status=='A').first()            
            if updLay:

                campos_originales = {}
                for col in updLay.__table__.columns:
                    if col.name not in ['id','usuario_alta','fecha_alta']:
                        campos_originales[col.name] = getattr(updLay, col.name)
                campos_originales['usuario_alta'] = user
                campos_originales['status'] = 'H'
                print('campos_originales :', campos_originales)
                
                cred_historico = DBLAYOUT(**campos_originales)
                db.add(cred_historico)

                updLay.entidad = entidad
                updLay.llave  = llave
                updLay.clave  = clave
                updLay.datos  = datos_in
                updLay.usuario_mod  = app_state.user_id
                updLay.fecha_mod  = obtener_fecha_actual()
                db.add(updLay) 
                db.commit()    
                result.append({'response':'Exito'
                                })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response':'registro inexistente'}) 
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message   
                rc = 400                            
        return result , rc
    
    def dellayout(self,datos, user,db):
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
            param_keys = ['id', 'entidad']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                       
                rc = 400 
            else:                
                id = dataJSON["id"]     
                entidad = dataJSON["entidad"]           
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message   
            rc = 400

        if estado:
            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        estado = False   
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        rc = 400   

            else:
                if entidad_usu != '0001':
                    estado = False   
                    result.append({'response':'No existe entidad'})    
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
            delLay =  db.query(DBLAYOUT).where(DBLAYOUT.id==id , DBLAYOUT.status=='A').first()            
            if delLay:

                campos_originales = {}
                for col in delLay.__table__.columns:
                    if col.name not in ['id','fecha_alta']:
                        campos_originales[col.name] = getattr(delLay, col.name)
                campos_originales['usuario_alta'] = user  # Usuario que crea el histórico
                campos_originales['status'] = 'B'
                print('campos_originales :', campos_originales)
                
                cred_historico = DBLAYOUT(**campos_originales)
                db.add(cred_historico)
                db.delete(delLay)

                db.commit()    
                result.append({'response':'Exito'
                                })        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response':'registro inexistente'}) 
                message = json.dumps(result)
                app_state.jsonenv = message   
                estado = False
                rc = 400                            
        return result , rc