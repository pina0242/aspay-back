
from app.core.models import DBTCORP , DBLOGWAF , DBLOGENTRY
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo ,obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import  distinct , and_ , func , text
import concurrent.futures
import pandas as pd

import logging
import json 
logger = logging.getLogger(__name__)
class ConfigService:

    
    def __init__(self, db_session=None):
        self.db = db_session
    
    
    def seltcorp(self,datos, db):
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
            param_keys = ['llave']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                descerror   = 'Algun elemento de la transacción esta faltando'
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                     
                rc = 400 
            else:                
                llave = dataJSON["llave"]                
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400


          
        if estado :  
            resultado = db.query(DBTCORP).where(DBTCORP.llave==llave,DBTCORP.status=='A').all()
            if resultado:
                for soceif in resultado:
                    result.append({ 'id':soceif.id,
                                'llave':soceif.llave,
                                'clave':soceif.clave,
                                'datos':soceif.datos,
                                'status':soceif.status,
                                'fecha_alta':str(soceif.fecha_alta),
                                'usuario_alta':soceif.usuario_alta,
                                'fecha_mod':str(soceif.fecha_mod),
                                'usuario_mod':soceif.usuario_mod   
                                })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No hay datos a listar'})   
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400                 

        return result , rc

    def regtcorp(self,datos, user, db):
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
            param_keys = ['llave' , 'clave' ,'datos','usuario_alta','usuario_mod']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})     
                message = json.dumps(result)
                app_state.jsonenv = message                  
                rc = 400 
            else:                
                llave = dataJSON["llave"]   
                clave = dataJSON["clave"]
                datos = dataJSON["datos"] 
                usuario_alta = dataJSON["usuario_alta"] 
                usuario_mod = dataJSON["usuario_mod"]              
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400
        estado   = True  
        fecha_mod = '0001-01-01'  
        if estado:
            validations = {
                'llave': ('long', 8),
                'clave': ('long', 15),
                'datos': ('long', 150),
                'usuario_alta': ('long',16),
                'usuario_mod': ('long', 16)
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
                usuario_mod = user
                status = 'A'
                regLLA = DBTCORP('0001',llave,clave,datos,status,usuario_alta,usuario_mod)
                db.add(regLLA)
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
    
    def updtacorp(self,datos, user, db):
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
            param_keys = ['id','datos','usuario_mod']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                     
                rc = 400 
            else:                
                id = dataJSON["id"] 
                datos = dataJSON["datos"]  
                usuario_mod = dataJSON["usuario_mod"]                 
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado :
            validations = {
                'datos': ('long', 150),
                'usuario_mod': ('long', 16) 
            }
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key

                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc   
        if  estado:
                lla = db.query(DBTCORP).where(DBTCORP.id==id,DBTCORP.status=='A').first()
                if lla:
                    campos_originales = {}
                    for col in lla.__table__.columns:
                        if col.name not in ['id','fecha_alta','fecha_mod']:
                            campos_originales[col.name] = getattr(lla, col.name)
                    campos_originales['status'] = 'M'
                    campos_originales['usuario_mod'] = user
                    print('campos_originales:', campos_originales)
                    tcorp_historico = DBTCORP(**campos_originales)
                    db.add(tcorp_historico)


                    lla.datos         = datos
                    lla.fecha_mod     = obtener_fecha_actual()
                    lla.usuario_mod   = user
                    db.add(lla)
                    db.commit() 
                    result.append({'response':'Exito'})   
                    rc = 201 
                    #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult         
            
        return result ,rc
    
    def deltacorp(self,datos, user, db):
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
            if not valcampo('num',id,1):
                result.append({'response':'id'})
                estado = False
                rc = 400
                message = json.dumps(result)
                app_state.jsonenv = message  
                print('result :', result)
                return result , rc
                                   
        if estado :                                                          
            delUsr =  db.query(DBTCORP).where(DBTCORP.id==id,DBTCORP.status=='A').first()   
            if delUsr:

                campos_originales = {}
                for col in delUsr.__table__.columns:
                    if col.name not in ['id','fecha_alta','fecha_mod']:
                        campos_originales[col.name] = getattr(delUsr, col.name)
                campos_originales['status'] = 'B'
                campos_originales['usuario_mod'] = user
                #print('campos_originales:', campos_originales)
                tcorp_historico = DBTCORP(**campos_originales)
                db.add(tcorp_historico)

                db.delete(delUsr)
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
    
    def listwaf(self,datos, user, db):
        result = []
        estado = True
        rc = 400 
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
            param_keys = ['fecha_inicio','fecha_fin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'})   
                message = json.dumps(result)
                app_state.jsonenv = message                    
                rc = 400 
            else:                
                fecha_inicio = dataJSON["fecha_inicio"]  
                fecha_fin = dataJSON["fecha_fin"]                 
        else:
            estado = False
            result.append({'response': 'informacion incorrecta'})
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado :
                waf = db.query(
                    distinct(DBLOGWAF.Ip_Origen),
                    DBLOGWAF.status
                ).filter(
                    and_(
                        DBLOGWAF.timestar >= fecha_inicio,
                        DBLOGWAF.timestar <= fecha_fin
                    )
                ).all()
                print ('waf :', waf)
                if waf:
                    for ip, status in waf:
                        result.append({ 
                                        'Ip_Origen':ip ,
                                        'status':status
                                    })
                        rc = 201 
                        #print('result :', result) 
                    message = json.dumps(result) 
                    app_state.jsonenv = message 
                    encripresult , estado = encrypt_message(password, message)
                    #print('encripresult :', encripresult)
                    result = encripresult
                else:
                    result.append({'response':'No hay datos a listar'}) 
                    message = json.dumps(result)
                    app_state.jsonenv = message  

        return result , rc
    
    def wafdet(self,datos, user, db):
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
            param_keys = ['ip']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                descerror   = 'Algun elemento de la transacción esta faltando'
                result.append({'response':'Algun elemento de la transacción esta faltando'})  
                message = json.dumps(result)
                app_state.jsonenv = message                     
                rc = 400 
            else:                
                ip = dataJSON["ip"]                
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400


          
        if estado :  
            resultado = db.query(DBLOGWAF).filter_by(Ip_Origen=ip).all()
            if resultado:
                for wafs in resultado:
                    result.append({ 'id':wafs.id,
                                'status':wafs.status,
                                'timestar':str(wafs.timestar),
                                'timeend':str(wafs.timeend),
                                'Ip_Origen':wafs.Ip_Origen,
                                'Servicio':wafs.Servicio,
                                'respcod':wafs.respcod,
                                'duration':str(wafs.duration),
                                'DatosIn':wafs.DatosIn,
                                'DatosOut':wafs.DatosOut   
                                })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No hay datos a listar'})    
                message = json.dumps(result)
                app_state.jsonenv = message                   
                rc = 400                 

        return result , rc
    
    def stats(self,datos, user, db):
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
            param_keys = ['fecini','fecfin']
            if not all (key in dataJSON for key in param_keys):
                estado = False
                result.append({'response':'Algun elemento de la transacción esta faltando'}) 
                message = json.dumps(result)
                app_state.jsonenv = message                      
                rc = 400 
            else:                
                fecini = dataJSON["fecini"]
                fecfin = dataJSON["fecfin"]                          
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message  
            rc = 400

        if estado:
            validations = {
                'fecini': ('date', 10),
                'fecfin': ('date', 10)
            }
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key

                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc   
            
        
        # Consulta única a la base de datos
        try:
            # ✅ OPTIMIZACIÓN: Usar la conexión existente en lugar de crear nuevo engine
            query_text = text("""
                SELECT "Servicio", "timestar", "timeend", "respcod" 
                FROM public."DBLOGENTRY"
                WHERE date("timestar") >= :fecini AND date("timestar") <= :fecfin
            """)
            
            # Usar la conexión de la sesión existente
            df_log = pd.read_sql_query(
                query_text, 
                db.connection(),  # ← ¡Usar la conexión existente!
                params={'fecini': fecini, 'fecfin': fecfin}
            )
            
            if df_log.empty:
                result_data = {
                    "stats1": [], "stats2": [], "stats3": [], "stats4": [], 
                    "stats5": {"conteo": 0}
                }
                message = json.dumps(result_data)
                encripresult, estado = encrypt_message(password, message)
                return encripresult, 201

            # Definir funciones de cálculo para cada estadística
            def calculate_stats1(df):
                conteo_servicios = df['Servicio'].value_counts().reset_index()
                conteo_servicios.columns = ['Servicio', 'Conteo']
                return conteo_servicios.to_dict(orient='records')
            
            def calculate_stats2(df):
                conteo_servicios = df.groupby('Servicio').size().reset_index(name='count')
                return conteo_servicios.to_dict(orient='records')
                
            def calculate_stats3(df):
                df_filtered = df[df['respcod'].astype(str) == '500']
                if df_filtered.empty:
                    return []
                estadistico = df_filtered['Servicio'].value_counts().reset_index()
                estadistico.columns = ['Servicio', 'Count']
                return estadistico.to_dict(orient="records")
                
            def calculate_stats4(df):
                df['TiempoEjecucion'] = (df['timeend'] - df['timestar']).dt.total_seconds()
                df_grouped = df.groupby('Servicio')['TiempoEjecucion'].mean().reset_index()
                df_grouped = df_grouped.rename(columns={'TiempoEjecucion': 'TiempoPromedio'})
                return df_grouped.to_dict(orient='records')

            def calculate_stats5(df):
                servicios_excluir = ['/stats1', '/stats2', '/stats3', '/stats4', '/stats5']
                conteo = df[~df['Servicio'].isin(servicios_excluir)].shape[0]
                return {"conteo": conteo}
            
            # Procesar las estadísticas en paralelo
            tasks = {
                'stats1': calculate_stats1,
                'stats2': calculate_stats2,
                'stats3': calculate_stats3,
                'stats4': calculate_stats4,
                'stats5': calculate_stats5,
            }

            result_data = {}
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_stat = {executor.submit(func, df_log): name for name, func in tasks.items()}
                for future in concurrent.futures.as_completed(future_to_stat):
                    stat_name = future_to_stat[future]
                    try:
                        data = future.result()
                        result_data[stat_name] = data
                    except Exception as exc:
                        print(f'{stat_name} generó una excepción: {exc}')
                        result_data[stat_name] = {'error': str(exc)}

            # Crear el JSON final
            message = json.dumps(result_data, ensure_ascii=False)
            app_state.jsonenv = message
            encripresult, estado = encrypt_message(password, message)
            rc = 201

            return encripresult, rc
        except Exception as e:
            logger.error(f"Error en stats: {e}")
            return [{'response': 'Error interno del servidor'}], 500
        
    def listlogs(self,datos, user, db):
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
            result.append({'response':'informacion incorrecta'}) 
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
            logs = (
                db.query(DBLOGENTRY)
                .filter(
                    func.date(DBLOGENTRY.timestar).between(fecha_inicio, fecha_fin),
                    ~DBLOGENTRY.Servicio.in_(['/selCiaTel', '/listlogs'])  # Excluir estos servicios
                )
                .order_by(DBLOGENTRY.id.desc())
                .all()
            )

            if logs:
                for listlogs in logs:
                    #print ('ok')
                    result.append({ 
                        'timestar':str(listlogs.timestar),
                        'timeend':str(listlogs.timeend), 
                        'log_level':listlogs.log_level,
                        'respcod:':listlogs.respcod,
                        'nombre':listlogs.nombre,
                        'Ip_Origen':listlogs.Ip_Origen,
                        'Servicio':listlogs.Servicio,
                        'Metodo':listlogs.Metodo,
                        'DatosIn':listlogs.DatosIn,
                        'DatosOut':listlogs.DatosOut                                                                                                       
                            })
                rc = 201 
                #print('result :', result) 
                message = json.dumps(result) 
                app_state.jsonenv = 'message'
                encripresult , estado = encrypt_message(password, message)
                #print('encripresult :', encripresult)
                result = encripresult
            else:
                result.append({'response':'No existen datos a listar'})   
                message = json.dumps(result)
                app_state.jsonenv = message                
                rc = 400                    

        return result , rc 