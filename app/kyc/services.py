
from app.core.models import DBDGENPERS, DBDIRPERS, DBKYCPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, generar_skyc
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import json 
logger = logging.getLogger(__name__)

class KycService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
       
    def regkyc(self, datos , user, db):
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
                #print("num_id:",num_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('longEQ',num_id,10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc
            
            sikyc = db.query(DBKYCPERS).order_by(DBKYCPERS.id.desc()).where(DBKYCPERS.num_id==num_id,DBKYCPERS.estatus=='A').first() 
            if sikyc:
                result.append({'response':'Ya existe calificacion, consulta o borra'}) 
                estado = False
                rc = 400
                return result , rc    
        
        if estado:                                 
            dir  = db.query(DBDIRPERS).order_by(DBDIRPERS.id.desc()).where(DBDIRPERS.num_id==num_id,DBDIRPERS.tipo_dir=='F',DBDIRPERS.estatus=='A').first() 
            if dir:
                pais = dir.pais
                per  = db.query(DBDGENPERS).order_by(DBDGENPERS.id.desc()).where(DBDGENPERS.num_id==num_id,DBDGENPERS.estatus=='A').first() 
                if per:
                    fecha_nac = per.fecha_nac_const
                    act_econ = per.giro
                    entidad  = per.entidad
                    print('act_econ', act_econ)
                else:    
                    result.append({'response':'No existe la persona'}) 
                    estado = False
                    rc = 400
                    return result , rc       
            else:
                result.append({'response':'No existe direccion Fiscal de la persona'}) 
                estado = False
                rc = 400
                return result , rc                                   
        
        if estado :
            try:  
                calif = generar_skyc(num_id,pais,fecha_nac,act_econ,db)
                #print('calif :', calif)      
                edad = calif["persona"]["edad"]  
                riesgo_geog = calif["evaluacion_riesgo"]["riesgo_geografico"]         
                riesgo_edad = calif["evaluacion_riesgo"]["riesgo_edad"]   
                riesgo_act_econ = calif["evaluacion_riesgo"]["riesgo_act_econ"]      
                riesgo_total = calif["evaluacion_riesgo"]["riesgo_total"]            
                puntuac_fico = calif["historial_crediticio"]["puntuacion_fico_simulada"]
                calif_fico = calif["historial_crediticio"]["categoria_fico"]
                tx_frecuentes = calif["patrones_detectados"]["transacciones_frecuentes"]
                tx_mismo_impte = calif["patrones_detectados"]["repeticion_montos"]
                tx_benef_sospech = calif["patrones_detectados"]["multiples_beneficiarios_sospechosos"]
                tx_alto_valor = calif["patrones_detectados"]["transacciones_alto_valor"]
                tx_paises_altrzgo = calif["patrones_detectados"]["paises_alto_riesgo_afectados"]
                tx_con_estruct = calif["patrones_detectados"]["estructuracion_detectada"]
                tx_sospechosas = calif["transacciones_sospechosas_marcadas"]
                aprob_kyc = calif["aprobacion_kyc"]
                razon_aprob = ", ".join(calif["razon_aprobacion_rechazo"]) if calif["razon_aprobacion_rechazo"] else None
                estatus = 'A'  
                fecha_mod   = datetime.min
                usuario_mod = '' 
                usuario_alta  =  user  

                regkyc = DBKYCPERS(num_id,entidad, edad, riesgo_geog, riesgo_edad, riesgo_act_econ,riesgo_total, puntuac_fico, calif_fico, tx_frecuentes, 
                                 tx_mismo_impte, tx_benef_sospech, tx_alto_valor, tx_paises_altrzgo, tx_con_estruct, 
                                 tx_sospechosas, aprob_kyc,razon_aprob,estatus,usuario_alta,fecha_mod,usuario_mod)
                db.add(regkyc) 
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
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Oooopppss'})            

        return result , rc 
    
    def selkyc(self, datos , user, db):
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
                #print("num_id:",num_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('longEQ',num_id,10):
                result.append({'response':'id_per'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc                            
        if estado :
            try:  
                kyc  = db.query(DBKYCPERS).order_by(DBKYCPERS.id.desc()).where(DBKYCPERS.num_id==num_id,DBKYCPERS.estatus=='A').first() 
                if kyc:
                    edad = kyc.edad
                    if edad >= 0:
                    #print ('ok')
                        fecha_alta_str = kyc.fecha_alta.isoformat() if isinstance(kyc.fecha_alta, (date, datetime)) else str(kyc.fecha_alta)
                        result.append({ 
                        'num_id':kyc.num_id,           
                        'edad':kyc.edad,            
                        'riesgo_geog':kyc.riesgo_geog,      
                        'riesgo_edad':kyc.riesgo_edad,      
                        'riesgo_act_econ':kyc.riesgo_act_econ,
                        'riesgo_total':kyc.riesgo_total,     
                        'puntuac_fico':kyc.puntuac_fico,     
                        'calif_fico':kyc.calif_fico,       
                        'tx_frecuentes':kyc.tx_frecuentes,    
                        'tx_mismo_impte':kyc.tx_mismo_impte,   
                        'tx_benef_sospech':kyc.tx_benef_sospech, 
                        'tx_alto_valor':kyc.tx_alto_valor,    
                        'tx_paises_altrzgo':kyc.tx_paises_altrzgo,
                        'tx_con_estruct':kyc.tx_con_estruct,   
                        'tx_sospechosas':kyc.tx_sospechosas,   
                        'aprob_kyc':kyc.aprob_kyc,        
                        'razon_aprob':kyc.razon_aprob,       
                        'fecha_alta':fecha_alta_str,
                        'usuario_alta':kyc.usuario_alta                                                                                 
                            })
                        rc = 201 
                        #print('result :', result) 
                        message = json.dumps(result) 
                        app_state.jsonenv = message
                        encripresult , estado = encrypt_message(password, message)
                        #print('encripresult :', encripresult)
                        result = encripresult     
                else:    
                    result.append({'response':'No existe calificacion'}) 
                    estado = False
                    rc = 400
                    return result , rc       
                
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Oooopppss'})            

        return result , rc 
    
    def delkyc(self, datos , user, db):
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
            if not valcampo('longEQ',num_id, 10):
                result.append({'response':'num_id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            kyc  = db.query(DBKYCPERS).order_by(DBKYCPERS.id.desc()).where(DBKYCPERS.num_id==num_id,DBKYCPERS.estatus=='A').first()           
            if  kyc:
                kyc.estatus = 'B'
                kyc.fecha_mod = obtener_fecha_actual()
                kyc.usuario_mod = user
                db.add(kyc)
                db.commit()
                result.append({'response': 'Exito'})
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response': 'registro inexistente'})
                estado = False
                rc = 400                       
        return result , rc 
    
    