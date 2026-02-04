
from app.core.models import DBDGENPERS, DBDCOMPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime
import logging
import json 
logger = logging.getLogger(__name__)

class DatcompService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regdcomper(self, datos , user, db):
        result = []
        estado = True
        access_token = ''
        message = datos
        #message = datos.decode('utf-8')
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
            param_keys = ['num_id','email_princ','email_alt','num_tel1','num_tel2','ind_pep','ingreso_max','period_ingreso','moneda_ingreso','volumen_tx','alias_nom_comer',
                          'pagina_web','red_social1','red_social2','red_social3','direcc_ip','direcc_mac']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:
                num_id          = dataJSON["num_id"]   
                email_princ     = dataJSON["email_princ"] 
                email_alt       = dataJSON["email_alt"]   
                num_tel1        = dataJSON["num_tel1"]
                num_tel2        = dataJSON["num_tel2"]
                ind_pep         = dataJSON["ind_pep"]   
                ingreso_max     = dataJSON["ingreso_max"]
                period_ingreso  = dataJSON["period_ingreso"]
                moneda_ingreso  = dataJSON["moneda_ingreso"] 
                volumen_tx      = dataJSON["volumen_tx"]
                alias_nom_comer = dataJSON["alias_nom_comer"]
                pagina_web      = dataJSON["pagina_web"] 
                red_social1     = dataJSON["red_social1"] 
                red_social2     = dataJSON["red_social2"] 
                red_social3     = dataJSON["red_social3"] 
                direcc_ip       = dataJSON["direcc_ip"] 
                direcc_mac      = dataJSON["direcc_mac"]                 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id': ('long', 10),
                'email_princ': ('long', 50),
                'email_alt': ('long', 50),
                'num_tel1': ('long', 10),
                'num_tel2': ('long', 10),
                'ind_pep': ('long', 1),
                'ingreso_max': ('float', 15),
                'period_ingreso': ('long', 1),
                'moneda_ingreso': ('long', 3),
                'volumen_tx': ('float', 10),
                'alias_nom_comer': ('long', 50),
                'pagina_web': ('long', 50),
                'red_social1': ('long', 50),
                'red_social2': ('long', 50),
                'red_social3': ('long', 50),
                'direcc_ip': ('long', 20),
                'direcc_mac': ('long', 20)
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
            # if not valcampo('long',email_princ,50):
            #     result.append({'response':'email_princ'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',email_alt,50):
            #     result.append({'response':'email_alt'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',num_tel1,10):
            #     result.append({'response':'num_tel1'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',num_tel2,10):
            #     result.append({'response':'num_tel2'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',ind_pep,1):
            #     result.append({'response':'ind_pep'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc               
            # if not valcampo('float',ingreso_max,15):
            #     result.append({'response':'ingreso_max'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            # if not valcampo('long',period_ingreso,1):
            #     result.append({'response':'period_ingreso'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc           
            # if not valcampo('long',moneda_ingreso,3):
            #     result.append({'response':'moneda_ingreso'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc           
            # if not valcampo('float',ingreso_max,15):
            #     result.append({'response':'ingreso_max'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            # if not valcampo('float',volumen_tx,10):
            #     result.append({'response':'volumen_tx'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                                             
            # if not valcampo('long',alias_nom_comer,50):
            #     result.append({'response':'alias_nom_comer'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',pagina_web,50):
            #     result.append({'response':'pagina_web'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                 
            # if not valcampo('long',red_social1,50):
            #     result.append({'response':'red_social1'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc             
            # if not valcampo('long',red_social2,50):
            #     result.append({'response':'red_social2'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',red_social3,50):
            #     result.append({'response':'red_social3'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',direcc_ip,20):
            #     result.append({'response':'direcc_ip'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',direcc_mac,20):
            #     result.append({'response':'direcc_mac'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            #print('guser',g.user)
        if estado:
            try:
            # Buscar si existe la persona
                dgen_existe = db.query(DBDGENPERS).where(
                    DBDGENPERS.num_id == num_id,
                    DBDGENPERS.estatus == 'A'
                ).first()
            
                if not dgen_existe:
                  estado = False
                  result.append({'response': f'No existe la persona'})
                  rc = 400
                  return result, rc
                else:
                    entidad = dgen_existe.entidad
            except Exception as e:
                print('Error en validación de datos compl existente:', e)
                estado = False
                result.append({'response': 'Error en validación'})
                rc = 400
                return result, rc
            
        if estado:
            try:
            # Buscar si ya existen datos complementarios activos para el mismo num_id
                dcom_existe = db.query(DBDCOMPERS).where(
                    DBDCOMPERS.num_id == num_id,
                    DBDCOMPERS.estatus == 'A'
                ).first()
            
                if dcom_existe:
                  estado = False
                  result.append({'response': f'Ya existen datos complementarios para esta persona'})
                  rc = 400
                  return result, rc
                
            except Exception as e:
                print('Error en validación de datos compl existente:', e)
                estado = False
                result.append({'response': 'Error en validación'})
                rc = 400
                return result, rc
            
        if estado :
            try:
                num_id          = num_id
                email_princ     = email_princ
                email_alt       = email_alt
                num_tel1        = num_tel1
                num_tel2        = num_tel2
                ind_pep         = ind_pep
                ingreso_max     = ingreso_max
                period_ingreso  = period_ingreso
                moneda_ingreso  = moneda_ingreso
                volumen_tx      = volumen_tx
                alias_nom_comer = alias_nom_comer
                pagina_web      = pagina_web
                red_social1     = red_social1
                red_social2     = red_social2
                red_social3     = red_social3
                direcc_ip       = direcc_ip
                direcc_mac      = direcc_mac  
                estatus         = 'A'
                fecha_state     = obtener_fecha_actual()  
                fecha_mod       = datetime.min 
                usuario_mod     = '' 
                usuario_alta    = user  
                #print ('fecha_state',fecha_state)
                                                         
                regdcomPer = DBDCOMPERS(num_id,entidad ,email_princ,email_alt,num_tel1,num_tel2,ind_pep,ingreso_max,period_ingreso,moneda_ingreso,volumen_tx,alias_nom_comer,pagina_web,
                                        red_social1,red_social2,red_social3,direcc_ip, direcc_mac,estatus,fecha_state,usuario_alta,fecha_mod,usuario_mod)
                db.add(regdcomPer) 
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
                result.append({'response':'Error DB'})            

        return result , rc 
    
    def seldcomper(self, datos , user, db):
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
                num_id = dataJSON.get("num_id")
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
                dcomp = db.query(DBDCOMPERS).order_by(DBDCOMPERS.id.desc()).where(DBDCOMPERS.num_id == num_id,
                                                                                       DBDCOMPERS.estatus == 'A').first()
                if dcomp:
                    #print ('ok')
                    result.append({ 
                            'num_id'         : dcomp.num_id,
                            'email_princ'    : dcomp.email_princ,
                            'email_alt'      : dcomp.email_alt,
                            'num_tel1'       : dcomp.num_tel1,
                            'num_tel2'       : dcomp.num_tel2,
                            'ind_pep'        : dcomp.ind_pep,
                            'ingreso_max'    : dcomp.ingreso_max,
                            'period_ingreso' : dcomp.period_ingreso,
                            'moneda_ingreso' : dcomp.moneda_ingreso,
                            'volumen_tx'     : dcomp.volumen_tx,
                            'alias_nom_comer': dcomp.alias_nom_comer,
                            'pagina_web'     : dcomp.pagina_web,
                            'red_social1'    : dcomp.red_social1,
                            'red_social2'    : dcomp.red_social2,
                            'red_social3'    : dcomp.red_social3,
                            'direcc_ip'      : dcomp.direcc_ip,
                            'direcc_mac'     : dcomp.direcc_mac
                                })
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message 
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                else:
                    result.append({'response':'No existen datos comp a mostrar'})                 
                    rc = 400                    
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error Fatal'})   
        return result , rc
    
    def upddcomper(self, datos , user, db):
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
            param_keys = ['num_id','email_princ','email_alt','num_tel1','num_tel2','ind_pep','ingreso_max','period_ingreso','moneda_ingreso','volumen_tx','alias_nom_comer',
                          'pagina_web','red_social1','red_social2','red_social3','direcc_ip','direcc_mac']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:
                num_id          = dataJSON["num_id"]   
                email_princ     = dataJSON["email_princ"] 
                email_alt       = dataJSON["email_alt"]   
                num_tel1        = dataJSON["num_tel1"]
                num_tel2        = dataJSON["num_tel2"]
                ind_pep         = dataJSON["ind_pep"]   
                ingreso_max     = dataJSON["ingreso_max"]
                period_ingreso  = dataJSON["period_ingreso"]
                moneda_ingreso  = dataJSON["moneda_ingreso"] 
                volumen_tx      = dataJSON["volumen_tx"]
                alias_nom_comer = dataJSON["alias_nom_comer"]
                pagina_web      = dataJSON["pagina_web"] 
                red_social1     = dataJSON["red_social1"] 
                red_social2     = dataJSON["red_social2"] 
                red_social3     = dataJSON["red_social3"] 
                direcc_ip       = dataJSON["direcc_ip"] 
                direcc_mac      = dataJSON["direcc_mac"]                 
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id': ('long', 10),
                'email_princ': ('long', 50),
                'email_alt': ('long', 50),
                'num_tel1': ('long', 10),
                'num_tel2': ('long', 10),
                'ind_pep': ('long', 1),
                'ingreso_max': ('float', 15),
                'period_ingreso': ('long', 1),
                'moneda_ingreso': ('long', 3),
                'volumen_tx': ('float', 10),
                'alias_nom_comer': ('long', 50),
                'pagina_web': ('long', 50),
                'red_social1': ('long', 50),
                'red_social2': ('long', 50),
                'red_social3': ('long', 50),
                'direcc_ip': ('long', 20),
                'direcc_mac': ('long', 20)
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
            # if not valcampo('long',email_princ,50):
            #     result.append({'response':'email_princ'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',email_alt,50):
            #     result.append({'response':'email_alt'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',num_tel1,10):
            #     result.append({'response':'num_tel1'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',num_tel2,10):
            #     result.append({'response':'num_tel2'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',ind_pep,1):
            #     result.append({'response':'ind_pep'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc               
            # if not valcampo('float',ingreso_max,15):
            #     result.append({'response':'ingreso_max'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            # if not valcampo('long',period_ingreso,1):
            #     result.append({'response':'period_ingreso'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc           
            # if not valcampo('long',moneda_ingreso,3):
            #     result.append({'response':'moneda_ingreso'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc           
            # if not valcampo('float',ingreso_max,15):
            #     result.append({'response':'ingreso_max'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            # if not valcampo('float',volumen_tx,10):
            #     result.append({'response':'volumen_tx'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                                             
            # if not valcampo('long',alias_nom_comer,50):
            #     result.append({'response':'alias_nom_comer'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('long',pagina_web,50):
            #     result.append({'response':'pagina_web'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                 
            # if not valcampo('long',red_social1,50):
            #     result.append({'response':'red_social1'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc             
            # if not valcampo('long',red_social2,50):
            #     result.append({'response':'red_social2'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',red_social3,50):
            #     result.append({'response':'red_social3'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',direcc_ip,20):
            #     result.append({'response':'direcc_ip'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc    
            # if not valcampo('long',direcc_mac,20):
            #     result.append({'response':'direcc_mac'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc   
            #print('guser',g.user)                                            
                                   
        if estado :                                                          
            dcomp =  db.query(DBDCOMPERS).where(DBDCOMPERS.num_id==num_id, DBDCOMPERS.estatus=='A').first()            
            if dcomp:
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in dcomp.__table__.columns:
                        if col.name not in ['id', 'fecha_alta']:
                            campos_originales[col.name] = getattr(dcomp, col.name)
                
                    campos_originales['estatus'] = 'M'  # eStatus histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    #print('campos_originales :', campos_originales)
                
                    dcpers_historico = DBDCOMPERS(**campos_originales)
                    db.add(dcpers_historico)

                # 2. ACTUALIZAR registro original (mantener estatus 'A')
                    dcomp.email_princ     = email_princ
                    dcomp.email_alt       = email_alt
                    dcomp.num_tel1        = num_tel1
                    dcomp.num_tel2        = num_tel2
                    dcomp.ind_pep         = ind_pep
                    dcomp.ingreso_max     = ingreso_max
                    dcomp.period_ingreso  = period_ingreso 
                    dcomp.moneda_ingreso  = moneda_ingreso
                    dcomp.volumen_tx      = volumen_tx
                    dcomp.alias_nom_comer = alias_nom_comer
                    dcomp.pagina_web      = pagina_web
                    dcomp.red_social1     = red_social1
                    dcomp.red_social2     = red_social2
                    dcomp.red_social3     = red_social3
                    dcomp.direcc_ip       = direcc_ip
                    dcomp.direcc_mac      = direcc_mac
                    dcomp.usuario_mod     = user
                    dcomp.fecha_mod       = obtener_fecha_actual() 
                    
                    db.add(dcomp)     
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
                    if estado:
                        return encripresult, rc
                    else:
                        result = [{'response': 'Error encriptando la respuesta'}]
                        return result, 500
                    #result = encripresult
                except Exception as e:
                    print ('e',e)
                    result.append({'response':'oops ocurrio un error'}) 
                    estado = False
                    rc = 400   
            else:
                result.append({'response':'registro inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc
    
    def deldcomper(self, datos , user, db):
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
                num_id   = dataJSON["num_id"]   
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
                                   
        if estado :                                                          
            dcom  = db.query(DBDCOMPERS).order_by(DBDCOMPERS.id.desc()).where(DBDCOMPERS.num_id==num_id, DBDCOMPERS.estatus=='A').first()           
            if dcom:
                dcom.estatus = 'B'
                dcom.fecha_mod = obtener_fecha_actual()
                dcom.usuario_mod = user
                db.add(dcom)
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
                result.append({'response':'Datos complementarios inexistentes'}) 
                estado = False
                rc = 400                            
        return result , rc 