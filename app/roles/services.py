from app.core.models import DBSERVNIV
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import  func
from datetime import timedelta
import logging
import json 
logger = logging.getLogger(__name__)
class RolesService:

    
    def __init__(self, db_session=None):
        self.db = db_session
    
    
    def listnivtran(self,datos,entidad, db):
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
                'fecha_inicio': ('date', 1),
                'fecha_fin': ('date', 1)
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
            nivtran = db.query(DBSERVNIV).order_by(DBSERVNIV.id.desc()).where(
                func.date(DBSERVNIV.fecha_alta).between(fecha_inicio, fecha_fin),
                DBSERVNIV.status=='A', DBSERVNIV.entidad==entidad).all() 

            if nivtran:
                for selnivtran in nivtran:
                    #print ('ok')
                    result.append({ 
                        'id':selnivtran.id,
                        'entidad':selnivtran.entidad,
                        'Servicio':selnivtran.Servicio, 
                        'nivel':selnivtran.nivel,
                        'indauth':selnivtran.indauth,
                        'indcost':selnivtran.indcost,
                        'indmon':selnivtran.indmon,
                        'indexc':selnivtran.indexc,
                        'impmax':selnivtran.impmax,
                        'fecha_alta':str(selnivtran.fecha_alta),
                        'usuario_alt':selnivtran.usuario_alt                                                                                                     
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
                message = json.dumps(result)
                app_state.jsonenv = message                
                rc = 400         
        return result , rc 

    def regnivtran(self,datos,user, db):
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
            param_keys = ['Servicio' , 'nivel','indauth','indcost','indmon','indexc','impmax']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})   
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                Servicio = dataJSON["Servicio"]   
                nivel = dataJSON["nivel"]  
                indauth = dataJSON["indauth"]  
                indcost = dataJSON["indcost"]  
                indmon = dataJSON["indmon"]  
                indexc = dataJSON["indexc"]  
                impmax = dataJSON["impmax"]                             
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        if estado:
            validations = {
                'Servicio': ('long', 20),
                'nivel': ('float', 1),
                'indauth': ('long', 1),
                'indcost': ('long', 1),
                'indmon': ('long', 1),
                'indexc': ('long', 1),
                'impmax': ('float', 1)
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
            status       = 'A'   
            usuario_alt  =  user   
            entidad = app_state.entidad                                          
            regNivtran = DBSERVNIV(entidad,Servicio,nivel,indauth,indcost,indmon,indexc,impmax,usuario_alt,status)
            db.add(regNivtran)
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
      
    def updnivtran(self,datos,user, db):
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
            param_keys = ['idnivtran','Servicio','nivel','indauth','indcost','indmon','indexc','impmax']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})   
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                idnivtran = dataJSON["idnivtran"]
                Servicio = dataJSON["Servicio"] 
                nivel = dataJSON["nivel"] 
                indauth = dataJSON["indauth"] 
                indcost = dataJSON["indcost"]  
                indmon = dataJSON["indmon"]  
                indexc = dataJSON["indexc"]  
                impmax = dataJSON["impmax"]         

        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        if estado:
            validations = {
                'idnivtran': ('float', 1),
                'Servicio': ('long', 20),
                'nivel': ('float', 1),
                'indauth': ('long', 1),
                'indcost': ('long', 1),
                'indmon': ('long', 1),
                'indexc': ('long', 1),
                'impmax': ('float', 1)
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
            updNivtr =  db.query(DBSERVNIV).where(DBSERVNIV.id==idnivtran,DBSERVNIV.entidad==app_state.entidad,DBSERVNIV.status=='A').first()            
            if updNivtr:
                campos_originales = {}
                for col in updNivtr.__table__.columns:
                    if col.name not in ['id', 'fecha_alta']:
                        campos_originales[col.name] = getattr(updNivtr, col.name)
                
                campos_originales['status'] = 'M'  # Status histórico
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico

                print('campos_originales :', campos_originales)
                
                nivtran_hist = DBSERVNIV(**campos_originales)
                db.add(nivtran_hist)

                updNivtr.idnivtran  = idnivtran
                updNivtr.entidad    = app_state.entidad
                updNivtr.Servicio   = Servicio
                updNivtr.nivel      = nivel
                updNivtr.indauth    = indauth
                updNivtr.indcost    = indcost
                updNivtr.indmon     = indmon
                updNivtr.indexc     = indexc
                updNivtr.impmax     = impmax
                updNivtr.usuario_alt = user
            
                db.add(updNivtr)  
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

    def delnivtran(self,datos,user, db):
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
            param_keys = ['idnivtran']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})   
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                idnivtran = dataJSON["idnivtran"]             
        else:
            estado = False
            result.append({'response':'Información incorrecta'}) 
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        if estado:
            if not valcampo('float',idnivtran,1):
                result.append({'response':'idnivtran'})
                estado = False
                message = json.dumps(result)
                app_state.jsonenv = message  
                rc = 400
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            delNivTran =  db.query(DBSERVNIV).where(DBSERVNIV.id==idnivtran,DBSERVNIV.entidad==app_state.entidad,DBSERVNIV.status=='A').first()            
            if delNivTran:
                campos_originales = {}
                for col in delNivTran.__table__.columns:
                    if col.name not in ['id', 'fecha_alta']:
                        campos_originales[col.name] = getattr(delNivTran, col.name)
                
                campos_originales['status'] = 'B'  # Status histórico
                campos_originales['usuario_alt'] = user  # Usuario que crea el histórico

                print('campos_originales :', campos_originales)
                
                nivtran_hist = DBSERVNIV(**campos_originales)
                db.add(nivtran_hist)

                db.delete(delNivTran) 
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

