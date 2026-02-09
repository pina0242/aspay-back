
from app.core.models import DBDGENPERS, DBDIRPERS, DBKYCPERS , DBCALIF
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, generar_skyc , calif_opcion1, calif_opcion23, \
    calif_opcion4, calif_opcion5
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from sqlalchemy.orm import Session,sessionmaker
from datetime import timedelta, datetime, date

from app.core.database import get_db, SessionLocal
from contextlib import contextmanager


from app.core.interfaz import login_al_banco, consulta_movagre
import logging
import json 
logger = logging.getLogger(__name__)

class KycService:
    
    def new_session(self) -> Session:
    # Crea una sesión independiente (para hilos / bloques cortos)
        return self.session_factory()

    @contextmanager
    def session_scope_ctx(self):
        """
        Context manager thread-safe para abrir/cerrar una Session corta.
        Usa commit/rollback adecuados.
        """
        s = self.new_session()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    def __init__(self, db_session: Session = None, session_factory: sessionmaker = None):
        # Session del request (NO se usa en hilos)
        self.db = db_session
        # Fábrica de sesiones a usar en hilos y en ciclos internos
        self.session_factory = session_factory or SessionLocal

    
       
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
    
    
    def regcalif(self, datos , user, db):
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
                print('decriptMsg :', decriptMsg)
                app_state.jsonrec = decriptMsg

            if estado :
                dataJSON = json.loads(decriptMsg)
                param_keys = ['docto_id','tipo_id','riesgo_geog','riesgo_act_econ','riesgo_pep','riesgo_list_sanc','riesgo_med_adv','tx_alto_valor',
                            'tx_sospechosas','riesgo_movs','razon_riesgo_movs','score_crediticio','razon_score_cred','cuota_max_sugerida']
                if not all (key in dataJSON for key in param_keys):
                    estado = False
                    result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                    rc = 400 
                else:                
                    docto_id = dataJSON["docto_id"]        
                    tipo_id = dataJSON["tipo_id"]        
                    riesgo_geog = dataJSON["riesgo_geog"]        
                    riesgo_act_econ = dataJSON["riesgo_act_econ"]        
                    riesgo_pep = dataJSON["riesgo_pep"]        
                    riesgo_list_sanc = dataJSON["riesgo_list_sanc"]        
                    riesgo_med_adv = dataJSON["riesgo_med_adv"]        
                    tx_alto_valor = dataJSON["tx_alto_valor"]        
                    tx_sospechosas = dataJSON["tx_sospechosas"]        
                    riesgo_movs = dataJSON["riesgo_movs"]        
                    razon_riesgo_movs = dataJSON["razon_riesgo_movs"]        
                    score_crediticio = dataJSON["score_crediticio"]        
                    razon_score_cred = dataJSON["razon_score_cred"]        
                    cuota_max_sugerida = dataJSON["cuota_max_sugerida"]        
                    #print("docto_id:",docto_id) 
            else:
                estado = False
                result = 'informacion incorrecta'
                rc = 400

            if estado:
                    validations = {
                        'docto_id': ('long', 20),
                        'tipo_id': ('long', 20),
                        'riesgo_geog': ('long', 1),
                        'riesgo_act_econ': ('long', 1),
                        'riesgo_pep': ('long', 1),
                        'riesgo_list_sanc': ('long', 1),
                        'riesgo_med_adv': ('long', 1),
                        'tx_alto_valor': ('num', 2),
                        'tx_sospechosas': ('num', 2),
                        'riesgo_movs': ('long', 1),
                        'razon_riesgo_movs': ('long', 250),
                        'score_crediticio': ('num', 3),
                        'razon_score_cred': ('long', 250),
                        'cuota_max_sugerida': ('float', 10)                    
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
                try:  
                    entidad = app_state.entidad
                    estatus = 'A'  
                    fecha_mod   = datetime.min
                    usuario_mod = '' 
                    usuario_alta  =  user  

                    calif = DBCALIF(entidad,docto_id, tipo_id, riesgo_geog, riesgo_act_econ, riesgo_pep, riesgo_list_sanc,riesgo_med_adv, tx_alto_valor, tx_sospechosas, 
                                    riesgo_movs, razon_riesgo_movs, score_crediticio, razon_score_cred, cuota_max_sugerida, estatus, usuario_alta, fecha_mod, usuario_mod) 
                    db.add(calif) 
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
        
    def selcalif(self, datos , user, db):
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
            param_keys = ['entidad','docto_id']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                entidad = dataJSON["entidad"]        
                docto_id = dataJSON["docto_id"]        
                #print("num_id:",num_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if not valcampo('long',entidad,8):
                result.append({'response':'entidad'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc           
            if not valcampo('long',docto_id,20):
                result.append({'response':'docto_id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc          
                        
        if estado :
            try:  
                calif  = db.query(DBCALIF).order_by(DBCALIF.id.desc()).where(DBCALIF.entidad==entidad,DBCALIF.docto_id==docto_id,DBCALIF.estatus=='A').first() 
                if calif.docto_id != ' ':
                    #print ('ok')
                        fecha_alta_str = calif.fecha_alta.isoformat() if isinstance(calif.fecha_alta, (date, datetime)) else str(calif.fecha_alta)
                        result.append({ 
                        'id': calif.id,
                        'entidad': calif.entidad,
                        'docto_id': calif.docto_id,
                        'tipo_id': calif.tipo_id,
                        'riesgo_geog': calif.riesgo_geog,
                        'riesgo_act_econ': calif.riesgo_act_econ,
                        'riesgo_pep': calif.riesgo_pep,
                        'riesgo_list_sanc': calif.riesgo_list_sanc,
                        'riesgo_med_adv': calif.riesgo_med_adv,
                        'tx_alto_valor': calif.tx_alto_valor,
                        'tx_sospechosas': calif.tx_sospechosas,
                        'riesgo_movs': calif.riesgo_movs,
                        'razon_riesgo_movs': calif.razon_riesgo_movs,
                        'score_crediticio': calif.score_crediticio,
                        'razon_score_cred': calif.razon_score_cred,
                        'cuota_max_sugerida': calif.cuota_max_sugerida                                                                                  
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


        
    def califopc1(self, datos , user, db):
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
            param_keys = ['doc_id','pais','actecon']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                doc_id = dataJSON["doc_id"]       
                pais = dataJSON["pais"]     
                actecon = dataJSON["actecon"]      
                #print("doc_id:",doc_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
                validations = {
                    'doc_id': ('long', 20),
                    'pais': ('long', 30),
                    'actecon': ('long', 30)
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
            try:  
                calif = calif_opcion1(doc_id,pais,actecon,db)
                #print('calif :', calif)      
                riesgo_geog = calif["evaluacion_riesgo"]["riesgo_geografico"]         
                riesgo_act_econ = calif["evaluacion_riesgo"]["riesgo_act_econ"]      
                riesgo_total = calif["evaluacion_riesgo"]["riesgo_total"]            
                result.append({ 
                        'doc_id':doc_id,           
                        'pais':pais,            
                        'activ_econ':actecon,      
                        'riesgo_geog':riesgo_geog,
                        'riesgo_act_econ':riesgo_act_econ,
                        'riesgo_total':riesgo_total                                                                                
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

    def califopc23(self, datos , user, db):
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
            param_keys = ['doc_id','nombre','ap_paterno','ap_materno','fecha_nac_const','nacionalidad','alias','ocupa_giro','pais']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                doc_id = dataJSON["doc_id"]       
                nombre = dataJSON["nombre"]
                ap_paterno = dataJSON["ap_paterno"]
                ap_materno = dataJSON["ap_materno"]
                fecha_nac_const = dataJSON["fecha_nac_const"]
                nacionalidad = dataJSON["nacionalidad"]     
                alias = dataJSON["alias"] 
                ocupa_giro = dataJSON["ocupa_giro"]  
                pais = dataJSON["pais"]      
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
                validations = {
                    'doc_id': ('long', 20),
                    'nombre': ('long', 60),
                    'ap_paterno': ('long', 60),
                    'ap_materno': ('long', 60),
                    'fecha_nac_const': ('date', 10),
                    'nacionalidad': ('long', 30),
                    'alias': ('long', 50),
                    'ocupa_giro': ('long', 30),
                    'pais': ('long', 30)
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
            try:  
                calif = calif_opcion23(nombre,ap_paterno,ap_materno,fecha_nac_const,nacionalidad,alias,ocupa_giro,pais)
                ind_pep = calif["evaluacion"]["ind_pep"]   
                ind_listas_riesgo = calif["evaluacion"]["ind_listas_riesgo"]          
                ind_medios_adversos = calif["evaluacion"]["ind_medios_adversos"]
                razon = calif["salida_busqueda"]          
                #print('razon',razon)  
                resultados_lista = []   
                resultados_lista.append({ 
                    'doc_id': doc_id,           
                    'ind_pep': ind_pep,            
                    'ind_listas_riesgo': ind_listas_riesgo,
                    'ind_medios_adversos': ind_medios_adversos,
                    'razon': razon                                                                                
                    })       
                rc = 201 
                #print('result :', result) 
                message = json.dumps(resultados_lista)
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

    def califopc4(self, datos , user, db):
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
            param_keys = ['doc_id','tkncli','alias']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                doc_id = dataJSON["doc_id"]       
                tkncli = dataJSON["tkncli"]     
                alias = dataJSON["alias"]      
                #print("doc_id:",doc_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
                validations = {
                    'doc_id': ('long', 20),
                    'tkncli': ('long', 36),
                    'alias': ('long', 10)
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
            euser = app_state.user_id
            eIp_Origen = '192.111.1.1'
            etimestar = obtener_fecha_actual()
            with self.session_scope_ctx() as s_login:
                headertoken = login_al_banco(s_login, euser, eIp_Origen, etimestar)
            if not headertoken:
                logger.error("No se pudo autenticar contra el banco.")
                return {"resp": [], "rc": 0}
            try:
                with self.session_scope_ctx() as s_c:
                    datos = consulta_movagre(
                    s_c,
                        euser,
                        eIp_Origen,
                        etimestar,
                        headertoken,
                        tkncli=tkncli,
                        alias=alias
                    )
                # Validar que resp y resp1 sean listas de dicts
                resulta = datos.get("resp", [])
                resulta = datos['resp']
                rcr = datos['rc']
                                    
                if rcr == 201:
                    try:
                        movimientos_list = {
                        "movimientos": resulta
                }
                    except json.JSONDecodeError:
                        try:
                            cadena_intermedia = json.loads(f'"{resulta}"') 
                            movimientos_list = json.loads(cadena_intermedia)
                            
                        except json.JSONDecodeError as e:
                            # Si sigue fallando, es un error real del formato.
                            logger.error(f"Error JSON al procesar movimientos (doble fallo): {e}")
                            result = "El formato JSON de movimientos es inválido."
                            return result, 500
                    message = json.dumps(movimientos_list)
                    print('movimientos_list: ',movimientos_list)
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    result = encripresult
                    rc = 201   
                else:
                    result.append({'response': 'No existen movimientos de la cuenta.'})
                    rc = 400
                    estado = False
            except Exception as e:
                    rc = 400
                    logger.error(f"Error en recmovagre: {e}")
                    return {"resp": [],  "rc": 400}  
                    
        if estado :
            try:  
                calif = calif_opcion4(movimientos_list,db)
                tx_sospechosas = calif["transacciones_sospechosas_marcadas"]     
                aprobacion = calif["aprobacion_calif"]          
                razon = calif["razon_aprobacion_rechazo"]          
                print('razon',razon)  
                resultados_lista = []   
                resultados_lista.append({ 
                    'doc_id': doc_id,           
                    'tkncli': tkncli,            
                    'tx_sospechosas': tx_sospechosas,
                    'aprobacion': aprobacion,
                    'razon4': razon                                                                                
                    })       
                rc = 201 
                #print('result :', result) 
                message = json.dumps(resultados_lista)
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

    def califopc5(self, datos , user, db):
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
            param_keys = ['doc_id','tkncli','alias']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                doc_id = dataJSON["doc_id"]       
                tkncli = dataJSON["tkncli"]     
                alias = dataJSON["alias"]      
                #print("doc_id:",doc_id) 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
                validations = {
                    'doc_id': ('long', 20),
                    'tkncli': ('long', 36),
                    'alias': ('long', 10)
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
            euser = app_state.user_id
            eIp_Origen = '192.111.1.1'
            etimestar = obtener_fecha_actual()
            with self.session_scope_ctx() as s_login:
                headertoken = login_al_banco(s_login, euser, eIp_Origen, etimestar)
            if not headertoken:
                logger.error("No se pudo autenticar contra el banco.")
                return {"resp": [], "rc": 0}
            try:
                with self.session_scope_ctx() as s_c:
                    datos = consulta_movagre(
                    s_c,
                        euser,
                        eIp_Origen,
                        etimestar,
                        headertoken,
                        tkncli=tkncli,
                        alias=alias
                    )
                # Validar que resp y resp1 sean listas de dicts
                resulta = datos.get("resp", [])
                resulta = datos['resp']
                rcr = datos['rc']
                                    
                if rcr == 201:
                    try:
                        movimientos_list = {
                        "movimientos": resulta
                }
                    except json.JSONDecodeError:
                        try:
                            cadena_intermedia = json.loads(f'"{resulta}"') 
                            movimientos_list = json.loads(cadena_intermedia)
                            
                        except json.JSONDecodeError as e:
                            # Si sigue fallando, es un error real del formato.
                            logger.error(f"Error JSON al procesar movimientos (doble fallo): {e}")
                            result = "El formato JSON de movimientos es inválido."
                            return result, 500
                    message = json.dumps(movimientos_list)
                    #print('movimientos_list: ',movimientos_list)
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    result = encripresult
                    rc = 201   
                else:
                    result.append({'response': 'No existen movimientos de la cuenta.'})
                    rc = 400
                    estado = False
            except Exception as e:
                    rc = 400
                    logger.error(f"Error en recmovagre: {e}")
                    return {"resp": [],  "rc": 400}  
                    
        if estado :
            try:  
                calif = calif_opcion5(movimientos_list)
                score_crediticio = calif["score_resultado"]     
                cuota_max_sugerida = calif["cuota_maxima_sugerida"]          
                razon = calif["motivo_principal"]          
                #print('calif',calif)  
                resultados_lista = []   
                resultados_lista.append({ 
                    'doc_id': doc_id,           
                    'tkncli': tkncli,            
                    'score_crediticio': score_crediticio,
                    'cuota_max_sugerida': cuota_max_sugerida,
                    'razon5': razon                                                                                
                    })       
                rc = 201 
                #print('result :', result) 
                message = json.dumps(resultados_lista)
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