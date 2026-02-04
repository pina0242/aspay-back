
from app.core.models import DBDGENPERS, DBRELPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual, cargar_df_relaciones, cargar_df_personas, obtener_info_persona
from app.core.cai import busca_tipos_rel, verificar_relacion_existente, calcular_suma_porcentajes
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import json 
logger = logging.getLogger(__name__)

class RelService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regrel(self, datos , user, db):
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
            param_keys = ['num_id_princ','num_id_relac', 'tipo_relac', 'nivel_relac','porcentaje_partic','fecha_ini_rel','fecha_fin_rel','docto_referencia']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id_princ       = dataJSON["num_id_princ"]   
                num_id_relac       = dataJSON["num_id_relac"]   
                tipo_relac         = dataJSON["tipo_relac"]
                nivel_relac        = dataJSON["nivel_relac"] 
                porcentaje_partic  = dataJSON["porcentaje_partic"]   
                fecha_ini_rel      = dataJSON["fecha_ini_rel"]
                fecha_fin_rel      = dataJSON["fecha_fin_rel"]
                docto_referencia   = dataJSON["docto_referencia"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
    
        # Validaciones básicas de campos
        if estado:
            validations = {
                'num_id_princ': ('long', 10),
                'num_id_relac': ('long', 10),
                'tipo_relac': ('long', 4),
                'nivel_relac': ('long', 1),
                'fecha_ini_rel': ('date', 10) 
                } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
            # if not valcampo('long',num_id_princ,10):
            #     result.append({'response':'num_id_princ'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long',num_id_relac,10):
            #     result.append({'response':'num_id_relac'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long',tipo_relac,4):
            #     result.append({'response':'tipo_relac'}) 
            #     estado = False
            #     rc = 400
            #     return result, rc  
            # if not valcampo('long',nivel_relac,1):
            #     result.append({'response':'nivel_relac'}) 
            #     estado = False
            #     rc = 400
            #     return result, rc  
            if porcentaje_partic and not valcampo('float',porcentaje_partic,6):
                result.append({'response':'porcentaje_partic'}) 
                estado = False
                rc = 400
                return result, rc
            # if not valcampo('date',fecha_ini_rel,10):
            #     result.append({'response':'fecha_ini_rel'}) 
            #     estado = False
            #     rc = 400
            #     return result, rc 
            # if fecha_fin_rel and not valcampo('date',fecha_fin_rel,10):
            #      result.append({'response':'fecha_fin_rel'}) 
            #      estado = False
            #      rc = 400
            #     return result, rc 
            if docto_referencia and not valcampo('long',docto_referencia,50):
                result.append({'response':'docto_referencia'}) 
                estado = False
                rc = 400
                return result, rc           
    
        # Porcentaje entre 0 y 100
        if estado and porcentaje_partic:
            try:
                porcentaje = float(porcentaje_partic)
                if porcentaje < 0 or porcentaje > 100:
                    result.append({'response': 'El porcentaje de participacion debe estar entre 1 y 100'})
                    estado = False
                    rc = 400
                    return result, rc
            except ValueError:
                result.append({'response': 'Porcentaje no válido'})
                estado = False
                rc = 400
                return result, rc
    
        # Fechas (fin posterior a inicio)
        if estado and fecha_fin_rel:
            try:
                if fecha_fin_rel != '':
                    if fecha_fin_rel <= fecha_ini_rel:
                        result.append({'response': 'La fecha de fin relacion debe ser posterior a la fecha de inicio'})
                        estado = False
                        rc = 400
                        return result, rc
                else:
                    fecha_fin_rel = datetime.max
            except ValueError:
                result.append({'response': 'Formato de fecha no válido'})
                estado = False
                rc = 400
                return result, rc
    
        # Obtener personas de dataframe
        perp = None
        perl = None
        df_relaciones = cargar_df_relaciones(db)
        df_personas = cargar_df_personas(db)

        if estado:                         
            perp = obtener_info_persona(df_personas, num_id_princ)
            #print('perp:',perp)
            if perp is None:
                result.append({'response': 'No existe persona principal'}) 
                estado = False
                rc = 400
                return result, rc
    
        if estado:                         
            perl = obtener_info_persona(df_personas, num_id_relac)
            # print('perl:',perl)
            if perl is None:
                result.append({'response': 'No existe persona a relacionar'}) 
                estado = False
                rc = 400
                return result, rc 
        # AQUI RECUEPRAR ENTIDAD
        if estado and perp and perl:
            if perp['entidad'] != perl['entidad']:
                result.append({'response': 'No se puede relacionar personas con diferente entidad'})
                estado = False
                rc = 400
                return result, rc

        # No relacionar persona consigo misma
        if estado and perp and perl:
            if perp['num_id'] == perl['num_id']:
                result.append({'response': 'Una persona no puede relacionarse consigo misma'})
                estado = False
                rc = 400
                return result, rc
    
        # Relaciones específicas por tipo de persona
        if estado and perp and perl:
            tipos_fisica, tipos_moral = busca_tipos_rel(db)
           # Validacion de tipos de relacion para persona fisica principal
            if perp['tipo_per'] == 'F':
                if tipo_relac in tipos_fisica:
                    if perl['tipo_per'] != 'F':
                        result.append({'response': f'Las relaciones de tipo {tipo_relac} solo debe ser entre personas físicas'})
                        estado = False
                        rc = 400
                        return result, rc
                else:
                    result.append({'response': f'tipo de relacion {tipo_relac} no es valida entre personas fisicas'})
                    estado = False
                    rc = 400
                    return result, rc
        
            # Validacion de tipos de relacion para persona moral principal
            if perp['tipo_per'] == 'M':
                if tipo_relac in tipos_moral:      
                    if perl['tipo_per'] != 'F':          
                        result.append({'response': f'Para relaciones de tipo {tipo_relac}, la persona relacionada debe ser física'})
                        estado = False
                        rc = 400
                        return result, rc
                else:
                    result.append({'response': f'tipo de relacion {tipo_relac} no es valida para persona moral'})
                    estado = False
                    rc = 400
                    return result, rc
    
        # Validación de relación existente entre las mismas personas y tipo de relacion
        if estado:
            try:
                if verificar_relacion_existente(df_relaciones, num_id_princ, num_id_relac, tipo_relac): 
                    estado = False
                    result.append({'response': f'Ya existe una relación de tipo {tipo_relac} entre estas personas'})
                    rc = 400
                    return result, rc
            
            except Exception as e:
                print('Error en validación de relación existente:', e)
                estado = False
                result.append({'response': 'Error en validación de relación'})
                rc = 400
                return result, rc
            
        # Porcentajes de beneficiarios y socios---
        if estado:
            if tipo_relac == 'BENE' and perp['tipo_per'] == 'F':   
                if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'BENE', num_id_relac, 'R') 
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
        
                    if total_porcentaje > 100:
                        estado = False
                        result.append({'response': f'El porcentaje total de beneficiarios no puede exceder el 100%. Porcentaje actual: {porcentajes_existentes}%, Nuevo porcentaje: {porcentaje_nuevo}%'})
                        return result, 400    
                else:
                    result.append({'response': 'El porcentaje de participacion debe estar entre 1 y 100'})
                    estado = False
                    rc = 400
                    return result, rc
            
            # Cálculo para socios (SOCI)
            elif tipo_relac == 'SOCI' and perp['tipo_per'] == 'M':
                if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'SOCI', num_id_relac, 'R')
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
    
                    if total_porcentaje > 100:
                        estado = False
                        result.append({'response': f'El porcentaje total de participación no puede exceder el 100%. Porcentaje actual: {porcentajes_existentes}%, Nuevo porcentaje: {porcentaje_nuevo}%'})
                        return result, 400
                else:
                    result.append({'response': 'El porcentaje de participacion debe estar entre 1 y 100'})
                    estado = False
                    rc = 400
                    return result, rc
                
            # Cálculo para socios (ACCI)
            elif tipo_relac == 'ACCI' and perp['tipo_per'] == 'M':
                if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'ACCI', num_id_relac, 'R')
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
    
                    if total_porcentaje > 100:
                        estado = False
                        result.append({'response': f'El porcentaje total de participación no puede exceder el 100%. Porcentaje actual: {porcentajes_existentes}%, Nuevo porcentaje: {porcentaje_nuevo}%'})
                        return result, 400
                else:
                    result.append({'response': 'El porcentaje de participacion debe estar entre 1 y 100'})
                    estado = False
                    rc = 400
                    return result, rc
                                  
        # Inserción de la relación
        if estado:
            try:  
                estatus      = 'A'
                usuario_mod  = ' ' 
                entidad_t = perp['entidad']
                fecha_mod    = datetime.min
                usuario_alta = user  
                                                         
                regRel = DBRELPERS(
                    num_id_princ=num_id_princ, 
                    num_id_relac=num_id_relac,
                    entidad = entidad_t,
                    tipo_relac=tipo_relac,
                    nivel_relac=nivel_relac,
                    porcentaje_partic=porcentaje_partic,
                    fecha_ini_rel=fecha_ini_rel,
                    fecha_fin_rel=fecha_fin_rel,
                    docto_referencia=docto_referencia,
                    estatus=estatus,
                    usuario_alta=usuario_alta,
                    fecha_mod=fecha_mod,
                    usuario_mod=usuario_mod
                )
                db.add(regRel) 

                perav = db.query(DBDGENPERS).order_by(DBDGENPERS.id.desc()).where(
                                                            DBDGENPERS.num_id == num_id_princ, 
                                                            DBDGENPERS.estatus == 'A'
                                                            ).first() 
                if perav:
                    perav.avanreg = 75
                    perav.usuario_mod = user   
                    perav.fecha_mod = obtener_fecha_actual()   
                    db.add(perav)   

                db.commit()

                result.append({'response': 'Exito'})        
                rc = 201 
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            
            except Exception as e:
                db.rollback()
                print('Error :', e)
                rc = 400  
                result.append({'response': 'Error Fatal'})            

        return result, rc
    
    def selrel(self, datos , user, db):
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
            param_keys = ['num_id_princ']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id_princ = dataJSON["num_id_princ"]       
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:                      
            if not valcampo('long',num_id_princ,10):
                result.append({'response':'num_id_princ'}) 
                estado = False
                rc = 400
                return result , rc                                            
                                    
        if estado :
            try:  
                rel = db.query(DBRELPERS).order_by(DBRELPERS.id.desc()).where(DBRELPERS.num_id_princ==num_id_princ, 
                                                                                   DBRELPERS.estatus=='A').all() 
                if rel:
                    for listrel in rel:
                        #print ('ok')
                        fecha_ini_rel_str = listrel.fecha_ini_rel.isoformat() if isinstance(listrel.fecha_ini_rel, (date, datetime)) else str(listrel.fecha_ini_rel)
                        fecha_fin_rel_str = listrel.fecha_fin_rel.isoformat() if isinstance(listrel.fecha_fin_rel, (date, datetime)) else str(listrel.fecha_fin_rel)
                        fecha_alta_str = listrel.fecha_alta.isoformat() if isinstance(listrel.fecha_alta, (date, datetime)) else str(listrel.fecha_alta)
                        fecha_mod_str = listrel.fecha_mod.isoformat() if isinstance(listrel.fecha_mod, (date, datetime)) else str(listrel.fecha_mod)
                        result.append({ 
                            'id':listrel.id,
                            'num_id_princ':listrel.num_id_princ, 
                            'num_id_relac':listrel.num_id_relac,
                            'tipo_relac':listrel.tipo_relac, 
                            'nivel_relac':listrel.nivel_relac,
                            'porcentaje_partic':listrel.porcentaje_partic,
                            'fecha_ini_rel':fecha_ini_rel_str,
                            'fecha_fin_rel':fecha_fin_rel_str,
                            'docto_referencia':listrel.docto_referencia,
                            'fecha_alta':fecha_alta_str,
                            'usuario_alta':listrel.usuario_alta ,
                            'fecha_mod':fecha_mod_str,
                            'usuario_mod':listrel.usuario_mod                                                                                                          
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
    
    def updrel(self, datos , user, db):
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
            param_keys = ['num_id_princ','num_id_relac','tipo_relac' , 'nivel_relac', 'fecha_ini_rel','fecha_fin_rel','docto_referencia']    
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id_princ   = dataJSON["num_id_princ"]  
                num_id_relac   = dataJSON["num_id_relac"]   
                tipo_relac = dataJSON["tipo_relac"]   
                nivel_relac  = dataJSON["nivel_relac"]
                porcentaje_partic  = dataJSON["porcentaje_partic"] 
                fecha_ini_rel    = dataJSON["fecha_ini_rel"]
                fecha_fin_rel    = dataJSON["fecha_fin_rel"]
                docto_referencia    = dataJSON["docto_referencia"]      
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id_princ': ('long', 10),
                'num_id_relac': ('long', 10),
                'tipo_relac': ('long', 4),
                'nivel_relac': ('long', 1),
                'porcentaje_partic': ('float', 6),
                'fecha_ini_rel': ('date', 10),
                'docto_referencia': ('long', 50) 
                } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
                    
            # if not valcampo('long',num_id_princ,10):
            #     result.append({'response':'num_id_princ'})
            #     estado = False
            #     rc = 400
            #     print('result :', result)
            #     return result , rc
            # if not valcampo('long',num_id_relac,10):
            #     result.append({'response':'num_id_relac'})
            #     estado = False
            #     rc = 400
            #     print('result :', result)
            #     return result , rc
            # if not valcampo('long',tipo_relac,4):
            #     result.append({'response':'tipo_relac'})
            #     estado = False
            #     rc = 400
            #     print('result :', result)
            #     return result , rc
            # if not valcampo('long',nivel_relac,1):
            #     result.append({'response':'nivel_relac'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('float',porcentaje_partic,6):
            #     result.append({'response':'porcentaje_partic'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc
            # if not valcampo('date',fecha_ini_rel,10):
            #     result.append({'response':'fecha_ini_rel'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc              
            # if not valcampo('date',fecha_fin_rel,30):
            #     result.append({'response':'fecha_fin_rel'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc  
            # if not valcampo('long',docto_referencia,50):
            #     result.append({'response':'docto_referencia'}) 
            #     estado = False
            #     rc = 400
            #     return result , rc                

        if estado:
            df_relaciones = cargar_df_relaciones(db)        
            porcentaje = float(porcentaje_partic)
            # Porcentajes de beneficiarios y socios---
            #if tipo_relac == 'BENE' and perp['tipo_per'] == 'F':   
            if tipo_relac == 'BENE':   
                if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'BENE', num_id_relac, 'M') 
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
        
                    if total_porcentaje > 100:
                       estado = False
                       result.append({'response': f'El porcentaje total de BENEF excede el 100%. Porcentaje actual asignado: {porcentajes_existentes}%, Nuevo porcentaje por asignar: {porcentaje_nuevo}%'})
                       rc = 400 
                   #return result, 400    
                else:
                    result.append({'response': 'El porcentaje de participacion debe ser > 0'})
                    estado = False
                    rc = 400
                    return result, rc
            
                # Cálculo para socios (SOCI)
            #elif tipo_relac == 'SOC' and perp['tipo_per'] == 'M':
            elif tipo_relac == 'SOCI':
                 if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'SOCI', num_id_relac, 'M')
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
    
                    if total_porcentaje > 100:
                       estado = False
                       result.append({'response': f'El porcentaje total de participación excede el 100%. Porcentaje actual asignado: {porcentajes_existentes}%, Nuevo porcentaje por asignar: {porcentaje_nuevo}%'})
                       rc = 400 
                 else:
                    result.append({'response': 'El porcentaje de participacion debe ser > 0'})
                    estado = False
                    rc = 400
                    return result, rc
                   #return result, 400                     
                   # 
            # Cálculo para accionistas (ACCI)
            elif tipo_relac == 'ACCI':
                if porcentaje > 0:
                    porcentajes_existentes = calcular_suma_porcentajes(df_relaciones, num_id_princ, 'ACCI', num_id_relac, 'M')
                    porcentaje_nuevo = float(porcentaje_partic)
                    total_porcentaje = porcentajes_existentes + porcentaje_nuevo
    
                    if total_porcentaje > 100:
                        estado = False
                        result.append({'response': f'El porcentaje total de participación excede el 100%. Porcentaje actual asignado: {porcentajes_existentes}%, Nuevo porcentaje por asignar: {porcentaje_nuevo}%'})
                        rc = 400 
                else:
                    result.append({'response': 'El porcentaje de participacion debe ser > 0'})
                    estado = False
                    rc = 400
                    return result, rc        
                                   
        if estado :                                                          
            rel =  db.query(DBRELPERS).where(DBRELPERS.num_id_princ==num_id_princ,
                                                  DBRELPERS.num_id_relac==num_id_relac, 
                                                  DBRELPERS.tipo_relac==tipo_relac,
                                                  DBRELPERS.estatus=='A').first()           
            if rel:
                
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in rel.__table__.columns:
                        if col.name not in ['id', 'fecha_alta']:
                            campos_originales[col.name] = getattr(rel, col.name)
                
                    campos_originales['estatus'] = 'M'  # eStatus histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    print('campos_originales :', campos_originales)
                
                    rel_historico = DBRELPERS(**campos_originales)
                    db.add(rel_historico)

                # 2. ACTUALIZAR registro original (mantener estatus 'A')
                    if fecha_fin_rel == '':
                        fecha_fin_rel = datetime.max
                    rel.nivel_relac        = nivel_relac
                    rel.porcentaje_partic  = porcentaje_partic
                    rel.fecha_ini_rel      = fecha_ini_rel
                    rel.fecha_fin_rel      = fecha_fin_rel
                    rel.docto_referencia   = docto_referencia
                    rel.usuario_mod        = user
                    rel.fecha_mod          = obtener_fecha_actual() 
                    
                    db.add(rel)     
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
                result.append({'response':'relacion inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc
    
    def delrel(self, datos , user, db):
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
            param_keys = ['num_id_princ','num_id_relac','tipo_relac']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})                     
                rc = 400 
            else:                
                num_id_princ   = dataJSON["num_id_princ"]
                num_id_relac   = dataJSON["num_id_relac"]   
                tipo_relac = dataJSON["tipo_relac"]   
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            validations = {
                'num_id_princ': ('long', 10),
                'num_id_relac': ('long', 10),
                'tipo_relac': ('long', 4) 
                } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
                    
            # if not valcampo('longEQ',num_id_princ, 10):
            #     result.append({'response':'num_id_princ'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('longEQ',num_id_relac, 10):
            #     result.append({'response':'num_id_relac'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
            # if not valcampo('long',tipo_relac, 4):
            #     result.append({'response':'tipo_relac'})
            #     estado = False
            #     rc = 400
            #     #print('result :', result)
            #     return result , rc
                                   
        if estado :                                                          
            rel  = db.query(DBRELPERS).order_by(DBRELPERS.id.desc()).where(DBRELPERS.num_id_princ==num_id_princ,
                                                                                DBRELPERS.num_id_relac==num_id_relac, 
                                                                                DBRELPERS.tipo_relac==tipo_relac,
                                                                                DBRELPERS.estatus=='A').first()           
            if rel:
                rel.estatus = 'B'
                rel.fecha_mod = obtener_fecha_actual()
                rel.fecha_fin_rel = obtener_fecha_actual()
                rel.usuario_mod = user
                db.add(rel)
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
                result.append({'response':'relacion inexistente'}) 
                estado = False
                rc = 400                            
        return result , rc 
    
    