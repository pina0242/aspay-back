from app.core.models import DBSERVNIV , DBLOGWAF , DBTCORP , DBLOGENTRY , User, DBTRNAUT, Folio , DBENTIDAD , DBRELPERS ,\
                        DBDGENPERS , DBUSETRAN ,DBALERTS , DBCOSTTRAN
from sqlalchemy import  desc ,  or_  ,  func
import pandas as pd
import time
import random
import logging
from app.core.state import app_state
from sqlalchemy.exc import IntegrityError
import concurrent.futures
from datetime import datetime , timedelta
import json
import re
import requests

import xml.etree.ElementTree as ET

from app.core.config import settings
from app.core.auth import create_access_token
from app.core.cypher import encrypt_message , decrypt_message

from app.core.database import SessionLocal
logger = logging.getLogger(__name__)

def obtener_fecha_actual():
    return datetime.now() 

def valoath (email , db):
    result = []
    password = settings.PASSCYPH
    
    user = db.query(User).where(User.email==email,User.tipo=='A').first()
    if user:
        print('si esta el registro')
        additional_claims = {
                    'user_id': user.id, 
                    'role': user.nivel,
                    'clvcol':user.clvcol,
                    'entidad': user.entidad        
                }
        access_token = create_access_token(
            identity=user.name,
            additional_claims=additional_claims
                )            
        rc = 201
        respOTP = 'Bearer ' + access_token
        #print ('token :',respOTP)
        result.append({'response':respOTP
                                })        
         
        print('result :', result) 
        message = json.dumps(result)
        app_state.jsonenv = message
        app_state.entidad = user.entidad 
        #print('message :', message) 
        encripresult , estado = encrypt_message(password, message)
        print('encripresult :', encripresult)
        result = encripresult
        #    result = response
    else:
        result.append({'response':'usuario no autorizado'
                                })
        rc = 400   
    return result , rc 

def get_security_rule(ip_origen,servicio):
    """Obtiene la regla de seguridad SECURULE p"""
    print ('ip_origen de waf:', ip_origen)
    db = SessionLocal()
    entidad = app_state.entidad
    try:
        security_param = db.query(DBTCORP).order_by(DBTCORP.id.desc()).filter(
            DBTCORP.llave == 'SECURULE',
            DBTCORP.entidad == entidad,
            DBTCORP.status == 'A'
        ).first()

        if security_param:
            param1 = security_param.datos[0:3]
            param2 = security_param.datos[3:6] 
            param3 = security_param.datos[6:9] 
            param4 = security_param.datos[9:12] 
            param5 = security_param.datos[12:15] 
            #print('param :',param1,param2,param3,param4)
            TKN = param1
            num_records = int(param2)
            max_time_diff_seconds = int(param3)
            num_conects       = int(param4)
            num_fails         = int(param5)
            
            resultados, tiempo = waf(ip_origen,TKN,db,num_records,max_time_diff_seconds,num_conects,num_fails)
            
            print(f"\nTiempo total de ejecución: {tiempo:.2f} segundos")
            # Recuperar el DataFrame y hacer un print para cada registro
            #print('resultados: ', resultados)
            if not resultados.empty:
                print("\nDetalle de cada registro del DataFrame combinado:")
                for index, row in resultados.iterrows():
                    #print(f"Registro {index + 1}:")
                    status = 'R'
                    for col, value in row.items():
                        if col == 'timestar':
                            timestar = value
                        if col == 'timeend':
                            timeend = value
                        if col == 'Ip_Origen':
                            Ip_Origen = value
                        if col == 'Servicio':
                            Servicio = value
                        if col == 'Metodo':
                            Metodo = value
                        if col == 'duration':
                            if isinstance(value, (int, float, complex)):
                                duration = value
                            else:
                                duration = 0
                        if col == 'respcod':
                            respcod = value
                        if col == 'DatosOut':
                            DatosOut = str(value)
                        DatosIn = ''
                        print(f"   {col}: {value}")
                    regWAF = DBLOGWAF(entidad, status ,timestar,timeend,Ip_Origen,Servicio,Metodo,respcod,duration,DatosIn,DatosOut)
                    db.add(regWAF)
                        
                    print("-" * 30)
            else:
                print("El DataFrame combinado está vacío. No se encontraron registros que cumplan las condiciones.")
            
        return security_param
    except Exception as e:
        print(f"Error obteniendo SECURULE: {e}")
        return None
    finally:
        db.close()
def f0(df_log_entry, num_records, max_time_diff_seconds):
    """
    Retrieves the latest num_records for each IP, then identifies
    consecutive entries with a time difference less than max_time_diff_seconds,
    and returns a DataFrame with relevant log details.
    """
    if df_log_entry.empty:
        return pd.DataFrame()

    df = df_log_entry.copy()
    # Ensure timestamps are in datetime format
    df['timestar'] = pd.to_datetime(df['timestar'])
    df['timeend'] = pd.to_datetime(df['timeend'])

    # Sort by IP_Origen and then by timestar for correct consecutive processing
    df_sorted = df.sort_values(by=['Ip_Origen', 'timestar']).reset_index(drop=True)

    # Group by Ip_Origen and get the latest num_records for each group
    #def get_latest_n(group):
    #    return group.tail(num_records)

    # CORRECCIÓN: Usar include_groups=False
    #df_latest_per_ip = df_sorted.groupby('Ip_Origen', group_keys=False).apply(get_latest_n).reset_index(drop=True)
    df_latest_per_ip = df_sorted.groupby('Ip_Origen').tail(num_records).reset_index(drop=True)

    # Calculate the time difference between consecutive entries for each IP_Origen
    df_latest_per_ip['prev_timestar'] = df_latest_per_ip.groupby('Ip_Origen')['timestar'].shift(1)

    # Calculate duration only for records where a previous timestamp exists
    df_latest_per_ip['duration'] = (
        df_latest_per_ip['timestar'] - df_latest_per_ip['prev_timestar']
    ).dt.total_seconds()

    # Filter for entries where the duration is less than max_time_diff_seconds
    filtered_df = df_latest_per_ip[
        (df_latest_per_ip['duration'] < max_time_diff_seconds) &
        (df_latest_per_ip['prev_timestar'].notna())
    ].copy()

    # Select and return the required columns
    final_df = filtered_df[[
        'timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'duration'
    ]].sort_values(by='timestar', ascending=False).reset_index(drop=True)
    
    return final_df



def f1(df_log_entry, num_records, num_conects):
    """
    Identifies IPs with more than num_conects entries in their latest num_records
    where respcod = '401' and "DatosOut" like '%Authorization Header%'.
    """
    if df_log_entry.empty:
        return pd.DataFrame()

    df = df_log_entry.copy()
    # Filter directly on the DataFrame based on the conditions
    filtered_condition_df = df[
        (df['respcod'] == '401') &
        (df['DatosOut'].str.contains('Authorization Header', na=False))
    ].copy()
   
    if filtered_condition_df.empty:
        return pd.DataFrame()

    # Ensure timestamps are in datetime format
    filtered_condition_df['timestar'] = pd.to_datetime(filtered_condition_df['timestar'])
    filtered_condition_df['timeend'] = pd.to_datetime(filtered_condition_df['timeend'])

    # Sort by IP_Origen and then by timestar for correct consecutive processing
    df_sorted = filtered_condition_df.sort_values(by=['Ip_Origen', 'timestar']).reset_index(drop=True)

    # Group by Ip_Origen and get the latest num_records for each group
    #def get_latest_n(group):
    #    return group.tail(num_records)

    # CORRECCIÓN: Usar include_groups=False
    #df_latest_per_ip = df_sorted.groupby('Ip_Origen', group_keys=False).apply(get_latest_n).reset_index(drop=True)
    df_latest_per_ip = df_sorted.groupby('Ip_Origen').tail(num_records).reset_index(drop=True)

    # Count connections per IP within the latest num_records
    connection_counts = df_latest_per_ip.groupby('Ip_Origen').size().reset_index(name='connection_count')

    # Identify IPs with more than num_conects connections
    flagged_ips = connection_counts[connection_counts['connection_count'] > num_conects]['Ip_Origen']

    # Filter the original DataFrame to include only records from flagged IPs
    final_df = df_latest_per_ip[df_latest_per_ip['Ip_Origen'].isin(flagged_ips)].copy()

    # Select and return the required columns
    final_df = final_df[[
        'timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'respcod', 'DatosOut'
    ]].sort_values(by='timestar', ascending=False).reset_index(drop=True)
    
    return final_df

def f2(df_log_entry, num_fails):
    """
    Identifies IPs with failed transactions (respcod = '400'
    and "DatosOut" containing "Algun elemento de la transacción esta faltando%")
    exceeding num_fails.
    """
    # 1. Filter for specific conditions to get all failed transactions
    failed_transactions = df_log_entry[
        (df_log_entry['respcod'] == '400') &
        (df_log_entry['DatosOut'].str.contains('Algun elemento de la transacción esta faltando', na=False))
    ].copy()
    
    if failed_transactions.empty:
        return pd.DataFrame(columns=['timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'respcod', 'DatosOut'])

    # Ensure 'timestar' is datetime format
    failed_transactions['timestar'] = pd.to_datetime(failed_transactions['timestar'])
    
    # Calculate 'timeend' using 'tiempo' if available
    if 'tiempo' in failed_transactions.columns and pd.api.types.is_numeric_dtype(failed_transactions['tiempo']):
        failed_transactions['timeend'] = failed_transactions['timestar'] + pd.to_timedelta(failed_transactions['tiempo'], unit='s')
    else:
        failed_transactions['timeend'] = failed_transactions['timestar']

    # 2. Group by IP and count ALL failed occurrences
    fail_counts_per_ip = failed_transactions.groupby('Ip_Origen').size().reset_index(name='connection_fails')

    # 3. Identify IPs where total failures exceed num_fails
    flagged_ips = fail_counts_per_ip[fail_counts_per_ip['connection_fails'] > num_fails]['Ip_Origen'].tolist()

    if not flagged_ips:
        return pd.DataFrame(columns=['timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'respcod', 'DatosOut'])

    # 4. Filter 'failed_transactions' to include only records from flagged IPs
    final_df = failed_transactions[
        failed_transactions['Ip_Origen'].isin(flagged_ips)
    ].copy()

    # 5. Select and return the required columns
    final_df = final_df[[
        'timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'respcod', 'DatosOut'
    ]].sort_values(by='timestar', ascending=False).reset_index(drop=True)
    
    return final_df

funciones = {
    '0': f0,
    '1': f1,
    '2': f2,
}


def waf(Ip_Origen, token, session, num_records, max_time_diff_seconds, num_conects, num_fails):
    # Paso 1: Realizar una única consulta a la base de datos
    query_results = session.query(
        DBLOGENTRY.timestar,
        DBLOGENTRY.timeend,
        DBLOGENTRY.Ip_Origen,
        DBLOGENTRY.Servicio,
        DBLOGENTRY.Metodo,
        DBLOGENTRY.respcod,
        DBLOGENTRY.DatosOut
    ).order_by(desc(DBLOGENTRY.timestar)).where(DBLOGENTRY.entidad==app_state.entidad,DBLOGENTRY.Ip_Origen==Ip_Origen).limit(num_records).all()

    # Obtener la IP del primer registro (si existe)
    ipreg = None
    for regs in query_results:
        ipreg = regs.Ip_Origen
        break  # Solo necesitamos el primero

    # Verificar si ya está registrado
    verifReg = None
    if ipreg:
        verifReg = session.query(DBLOGWAF).where(DBLOGWAF.entidad==app_state.entidad,DBLOGWAF.status=='R', DBLOGWAF.Ip_Origen==ipreg).first()  
    
    if verifReg:
        print('Ya está registrado', ipreg) 
        final_combined_df = pd.DataFrame()
        tiempo_total = 0
        return final_combined_df, tiempo_total

    # Si no hay registros o no hay verifReg, procesar normalmente
    if not query_results:
        return pd.DataFrame(), 0

    # Convertir los resultados a DataFrame
    df_log_entry = pd.DataFrame(query_results, columns=[
        'timestar', 'timeend', 'Ip_Origen', 'Servicio', 'Metodo', 'respcod', 'DatosOut'
    ])

    # Preparar tareas
    tareas_con_args = []
    for digito in token:
        if digito in funciones:
            func_to_execute = funciones[digito]
            if func_to_execute == f0:
                tareas_con_args.append((func_to_execute, df_log_entry, num_records, max_time_diff_seconds))
            elif func_to_execute == f1:
                tareas_con_args.append((func_to_execute, df_log_entry, num_records, num_conects))
            elif func_to_execute == f2:
                tareas_con_args.append((func_to_execute, df_log_entry, num_fails))

    # Ejecutar tareas en paralelo
    inicio = time.time()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futuros = []
        for tarea_info in tareas_con_args:
            func_obj = tarea_info[0]
            args = tarea_info[1:]
            futuros.append(executor.submit(func_obj, *args))

        resultados = [futuro.result() for futuro in concurrent.futures.as_completed(futuros)]

    fin = time.time()
    tiempo_total = fin - inicio
    
    # CORRECCIÓN: Filtrar DataFrames no vacíos antes de concatenar
    all_dataframes = []
    
    for res in resultados:
        if isinstance(res, pd.DataFrame) and not res.empty:
            # Verificar que el DataFrame tenga al menos una fila y columnas válidas
            all_dataframes.append(res)

    # Concatenar solo si hay DataFrames válidos
    if all_dataframes:
        # Asegurar que todos los DataFrames tengan las mismas columnas
        all_columns = set()
        for df in all_dataframes:
            all_columns.update(df.columns)
        
        # Reindexar todos los DataFrames para tener las mismas columnas
        standardized_dfs = []
        for df in all_dataframes:
            # Agregar columnas faltantes con valores NaN
            missing_cols = all_columns - set(df.columns)
            for col in missing_cols:
                df[col] = None
            # Reordenar columnas
            df = df[list(all_columns)]
            standardized_dfs.append(df)
        
        final_combined_df = pd.concat(standardized_dfs, ignore_index=True)
    else:
        # Crear DataFrame vacío con estructura esperada
        final_combined_df = pd.DataFrame()

    return final_combined_df, tiempo_total


def log_response_info(request,start_time,ip_origen,rc):
        db = SessionLocal()
        try:
            #head = request.headers    
            #log_level = logging.getLogger().getEffectiveLevel()
            #log_level_name = logging.getLevelName(log_level)
            log_level_name = ''
            timestar = start_time
            #print('timestar :',timestar)
            timeend = obtener_fecha_actual()
            funcion = ''
            nombre  = app_state.user_id
            Ip_Origen = ip_origen
            entidad = app_state.entidad
            Servicio = request.url.path
            
            #print('Servicio :', Servicio)
            Metodo = request.method
            #print (' Metodo :', Metodo, app_state.jsonrec)
            if Metodo == 'POST' or Metodo == 'GET':                    
            #    DatosIn = g.jsonrec
            #    if len(g.jsonenv) > 0:
            #        DatosOut = g.jsonenv
            #    else:
            #        DatosOut = response.get_json() if response.is_json else response.get_data(as_text=True)
                current_jsonenv = app_state.jsonenv
                current_jsonrec = app_state.jsonrec
                DatosIn=str(current_jsonrec)
                DatosOut=str(current_jsonenv) 
                respcod = rc
            #    DatosIn = DatosIn[0] if isinstance(DatosIn, tuple) else DatosIn
            #    print('DatosIn:', DatosIn)
                #print('Longitud de DatosIn:', len(DatosIn)) 
            #    DatosOut = DatosOut[0] if isinstance(DatosOut, tuple) else DatosOut                    
                
            log_entry = DBLOGENTRY(entidad,timestar,timeend,log_level_name,funcion,respcod,nombre,Ip_Origen,Servicio,Metodo,DatosIn,DatosOut)
            db.add(log_entry)  
            db.commit()

            print ('entre a registrar log')
            #print ('request:',request )
            
        except Exception as e:
            print ('e',e)
            descerror = 'Ocurrió un error: ' 
            print (descerror )
            continua = False   
        finally:
            db.close()                                                
        
        
def valrole(claims,Servicio,Ip_Origen ,datos,session):
    user_id = claims['user_id']
    role = claims['role']   
    user = claims['clvcol']
    entidad = claims['entidad']
    msg  = ''
    print('Servicio :', Servicio , 'user_id :', user_id  ,' role :', role ,' user:',user, 'entidad: ',entidad) 
    app_state.user_id = user
    app_state.entidad = entidad
    nivtran = session.query(DBSERVNIV).order_by(DBSERVNIV.id.desc()).where(DBSERVNIV.entidad==entidad,
                                                                           DBSERVNIV.Servicio==Servicio,
                                                                           DBSERVNIV.nivel==role,
                                                                           DBSERVNIV.status=='A').first() 

    if nivtran:
        acces = True , user , msg
        verifReg =  session.query(DBLOGWAF).where(DBLOGWAF.entidad==entidad,DBLOGWAF.status=='B', DBLOGWAF.Ip_Origen==Ip_Origen).first()  
        if verifReg:
            print ('Warning IP' , Ip_Origen) 
            msg = 'IP en cuarentena'
            acces = False , user , msg
        else:
#i) Indicador de Protección/Autorización
#•	Nivel 1: Acceso libre dentro de permisos
#•	Nivel 2: Requiere autorización de supervisor inmediato
#•	Nivel 3: Requiere autorización de Administrador Cliente
#•	Nivel 4: Requiere autorización de Usuario Master
#•	Nivel 5: Bloqueado por política de seguridad
#ii) Indicador de Costo/Transacción
#•	Tipo A: Servicio sin costo (básico)
#•	Tipo B: Servicio con costo fijo por uso
#•	Tipo C: Servicio con costo variable por volumen
#•	Tipo D: Servicio con costo por complejidad
#•	Tipo E: Servicio premium (costo especial)
#iii) Indicador de Monitoreo/Alerta
#•	Nivel 0: Sin monitoreo especial
#•	Nivel 1: Log básico de accesos
#•	Nivel 2: Alertas por uso inusual
#•	Nivel 3: Grabación completa de sesión
#•	Nivel 4: Doble factor en cada uso
#iv) Indicador de Exclusividad
#•	Normal: Accesible según nivel y permiso
#•	Restringido: Solo para usuarios específicos
#•	Exclusivo Master: Únicamente para Nivel 0
#•	Temporal: Restricción por ventana horaria

            if nivtran.indauth != '1':
                auth ,msg =  valauth(Servicio,datos,user_id,session)
                if not auth:
                    acces = False,user,msg

            if nivtran.indcost in ('A', 'B', 'C', 'D'):
                valcost(Servicio,user,nivtran.indcost,session)

            if nivtran.indmon in ('0', '1', '2', '3'):
                valmon(Servicio,user,nivtran.indmon,session)

            if nivtran.indexc != 'N':
                auth ,msg =  valexc(nivtran.indexc)
                if not auth:
                    acces = False,user,msg


    else:
        msg = 'Usuario inválido.'
        acces = False , user , msg
    return acces

def valauth(Servicio,datos,user_id,session):
    
    if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
        logger.error("PASSCYPH no configurado en .env")
        acces = False , 'error en ciph'
    else:
        password = settings.PASSCYPH        
        message = datos
        message = str(message).replace('"','')
        decriptMsg , estado = decrypt_message(password, message)
        entidad = app_state.entidad
        iduser = user_id
        serv = Servicio
        datosin = decriptMsg
        print ('iduser :', iduser , 'serv :',serv , 'datosin :', datosin )
        try:
            resAuth = session.query(DBTRNAUT).where(
                DBTRNAUT.entidad == entidad,
                DBTRNAUT.iduser == iduser,
                DBTRNAUT.Servicio == serv,
                DBTRNAUT.status != 'H',
                DBTRNAUT.DatosIn == datosin
            ).first()
            
            if resAuth:
                if resAuth.status == 'P':
                    mensaje = 'autorización existente!!! '  + str(resAuth.folauth)
                    acces = False , mensaje
                else:
                    if resAuth.status == 'R':
                        print ("aut resuelta:", resAuth.folauth)
                        hoy = obtener_fecha_actual()
                        resAuth.status      = 'H'
                        resAuth.fecha_auth  = hoy 
                        session.add(resAuth) 
                        session.commit()
                        mensaje = 'Autorización resuelta'

                        acces = True , mensaje  
            else:
                numero =genfol04(session)
                folauth = iduser + numero 
                status = 'P'
                DatosIn  = datosin
                emailauth = ''
                fecha_auth = '0001-01-01'
                usuario_alt = ' '
                regAuth = DBTRNAUT(entidad, iduser, folauth ,Servicio ,status ,DatosIn,emailauth,fecha_auth,usuario_alt)
                session.add(regAuth) 
                session.commit()
                mensaje = 'Require autorización !!!' + str(folauth) 
                acces = False , mensaje                 
        except Exception as e:
            print('Error:', e)
            mensaje = 'error en la consulta'
            acces = False , mensaje      
    
    return acces

def valexc(indexc):  

    if indexc == 'E':  
        print ('transaccion Exclusiva')
        mensaje = 'Transaccion Exclusiva !!!'
        acces = False , mensaje    
    else:
        mensaje = 'Permitida !!!'
        acces = True , mensaje 

         
    return acces

def valcost(Servicio,user_id,indcost,session):
    entidad = app_state.entidad
    num_id  = user_id
    
    try:
        print ('entra a validar costos')
        regUse = DBUSETRAN(entidad, Servicio,indcost, num_id )
        session.add(regUse) 
        session.commit()
         
         

    except Exception as e:
        print('Error:', e)
        mensaje = 'error en la consulta'
        print ('ocurrio un error')

def valmon(Servicio,user_id,indmon,session):
    entidad = app_state.entidad
    num_id  = user_id
    
    try:
        print ('entra a validar monitoreo')
        regMon = DBALERTS(entidad, Servicio,indmon, num_id )
        session.add(regMon) 
        session.commit()
         
         

    except Exception as e:
        print('Error:', e)
        mensaje = 'error en la consulta'
        print ('ocurrio un error')

def genfol04(session): 
    max_attempts = 100  # Limita los intentos para evitar bucles infinitos en casos extremos
    for _ in range(max_attempts):
        clave = random.randint(1000, 9999)
        try:
            # Crea una instancia de Folio con el número de transacción
            regFol = Folio(numtranc=clave) # Asumiendo que Folio acepta numtranc en el constructor
            session.add(regFol)
            session.commit()  # Intenta guardar el folio inmediatamente
            return clave
        except IntegrityError:
            # Si hay un error de integridad (clave duplicada), se revierte y se intenta de nuevo
            session.rollback()
            # Continúa el bucle para generar una nueva clave
        except Exception as e:
            # Cualquier otro error inesperado
            session.rollback()
            raise Exception(f"Ocurrió un error inesperado al generar el folio: {e}")

    raise Exception

def genfol06(session): 
    max_attempts = 100  # Limita los intentos para evitar bucles infinitos en casos extremos
    for _ in range(max_attempts):
        clave = random.randint(100000, 999999)
        try:
            # Crea una instancia de Folio con el número de transacción
            regFol = Folio(numtranc=clave) # Asumiendo que Folio acepta numtranc en el constructor
            session.add(regFol)
            session.commit()  # Intenta guardar el folio inmediatamente
            return clave
        except IntegrityError:
            # Si hay un error de integridad (clave duplicada), se revierte y se intenta de nuevo
            session.rollback()
            # Continúa el bucle para generar una nueva clave
        except Exception as e:
            # Cualquier otro error inesperado
            session.rollback()
            raise Exception(f"Ocurrió un error inesperado al generar el folio: {e}")

    raise Exception


def valcampo(accion, dato, long):
    #print ('va a validar dato :', dato)
    if accion == 'date':
        formatos = ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', 'YYYY-MM-DD HH:mm:ss']  # Añadir otros formatos según sea necesario
        for formato in formatos:
            try:
                datetime.strptime(dato, formato)
                return True
            except ValueError:
                continue
        return False
    if accion == 'long':
        long_val = len(dato)
        return long_val <= long
    if accion == 'longEQ':
        long_val = len(dato)
        return long_val == long
    if accion == 'num':
        if int(dato) >= 0:
            return True
        else:
            return False
    if accion == 'hh:mm':
        if len(dato) != 5 or dato[2] != ':':
            return False
        try:
            horas, minutos = map(int, dato.split(':'))
            return 0 <= horas <= 23 and 0 <= minutos <= 59
        except ValueError:
            return False
    if accion == 'float':
        try:
            float(dato)
            return True
        except ValueError:
            print('no es numerico')
            return False

def listcorp(db, llave: str, clave: str) -> list[str]:
    ordered_unique_headers: list[str] = []
    resultado = db.query(DBTCORP.datos).order_by(DBTCORP.id.asc()).where(
        DBTCORP.entidad==app_state.entidad,
        DBTCORP.llave==llave,
        DBTCORP.clave==clave,
        DBTCORP.status=='A'
    ).all()
    
    seen_headers: set[str] = set() 
    if resultado:
        for row in resultado:
            header_raw = row[0]
            cleaned_h = header_raw.strip().lower()
            if cleaned_h not in seen_headers:
                ordered_unique_headers.append(cleaned_h)
                seen_headers.add(cleaned_h)
    return ordered_unique_headers


def tipos_id_se_validan(session):

    tipos_id = session.query(DBTCORP).where(
    or_(DBTCORP.llave == 'TIPOIDES',
        DBTCORP.llave == 'TIPOIDPT'
       )).order_by(DBTCORP.id.desc()).all()
    
    # Obtener los tipos de ID que se puede validar su estructura por país
    #print('tipos_id: ',tipos_id)

    cadena_tid_es = []
    cadena_tid_pt = []
    
    if tipos_id:
        cadena_tid_es = [registro.clave for registro in tipos_id if registro.clave and registro.llave == 'TIPOIDES']
        cadena_tid_pt = [registro.clave for registro in tipos_id if registro.clave and registro.llave == 'TIPOIDPT']
    else:
        cadena_tid_es = []
        cadena_tid_pt = []
    #print('cadena tides:',cadena_tides)
    return cadena_tid_es, cadena_tid_pt

def validar_id_persona(identificacion):
    # """
    # Valida identificaciones oficiales de España y Portugal
    
    # Args:
    #     identificacion (str): Número de identificación a validar
    #     pais (str): 'ES' para España, 'PT' para Portugal, 'auto' para detección automática
    
    # Returns:
    #     dict: Diccionario con información sobre la validación
    # """
    
    # Limpiar y normalizar la entrada
    
    id_limpio = identificacion.upper().replace(' ', '').replace('-', '').replace('.', '').strip()
    #print('id_limpio:',id_limpio)
    if not id_limpio:
        return {
            'valido': False,
            'pais': 'Desconocido',
            'tipo': 'Desconocido',
            'error': 'Identificación vacía'
        }    
    # Determinar el país si es automático
    #if pais == 'auto':
    pais = detectar_pais_identificacion(id_limpio)
    
    # Validar según el país
    if pais == 'ES':
        return validar_identificacion_espanola(id_limpio)
    elif pais == 'PT':
        return validar_identificacion_portuguesa(id_limpio)
    else:
        return {
            'valido': False,
            'pais': 'Desconocido',
            'tipo': 'Desconocido',
            'error': 'País no soportado o formato no reconocido'
        }

def detectar_pais_identificacion(identificacion):
    """
    Detecta automáticamente el país basado en el formato de identificación
    """
    # Patrones España
    patron_es_dni = r'^[0-9]{8}[A-Z]$'
    patron_es_nie = r'^[XYZ][0-9]{7}[A-Z]$'
    patron_es_cif = r'^[ABCDEFGHJKLMNPQRSUVW][0-9]{7}[0-9A-J]$'
    
    # Patrones Portugal
    patron_pt_cc = r'^[0-9]{8,9}[0-9]$'  # Cartão de Cidadão
    patron_pt_nif = r'^[0-9]{9}$'  # NIF portugués
    patron_pt_bi = r'^[0-9]{7,8}[0-9]$'  # Bilhete de Identidade antiguo
    
    if (re.match(patron_es_dni, identificacion) or 
        re.match(patron_es_nie, identificacion) or 
        re.match(patron_es_cif, identificacion)):
        return 'ES'
    elif (re.match(patron_pt_cc, identificacion) or 
          re.match(patron_pt_nif, identificacion) or 
          re.match(patron_pt_bi, identificacion)):
        return 'PT'
    
    return 'Desconocido'

def validar_identificacion_espanola(identificacion):
    """
    Valida identificaciones españolas (DNI, NIE, CIF, NIF)
    """
    tipo = determinar_tipo_identificacion_es(identificacion)
    #print ('tipo identificado',tipo)
    
    if tipo == 'DNI':
        valido = validar_dni_es(identificacion)
    elif tipo == 'NIE':
        valido = validar_nie_es(identificacion)
    elif tipo == 'CIF':
        valido = validar_cif_es(identificacion)
    elif tipo == 'NIF':
        valido = validar_nif_es(identificacion)
    else:
        return {
            'valido': False,
            'pais': 'España',
            'tipo': 'Desconocido',
            'error': 'Formato español no reconocido'
        }
    
    return {
        'valido': valido,
        'pais': 'España',
        'tipo': tipo,
        'identificacion': identificacion,
        'formateado': formatear_identificacion_es(identificacion, tipo)
    }

def determinar_tipo_identificacion_es(identificacion):
    """
    Determina el tipo de identificación española
    """
    patron_dni = r'^[0-9]{8}[A-Z]$'
    patron_nie = r'^[XYZ][0-9]{7}[A-Z]$'
    patron_cif = r'^[ABCDEFGHJKLMNPQRSUVW][0-9]{7}[0-9A-J]$'
    
    if re.match(patron_dni, identificacion):
        return 'DNI'
    elif re.match(patron_nie, identificacion):
        return 'NIE'
    elif re.match(patron_cif, identificacion):
        return 'CIF'
    else:
        return 'Desconocido'  # No más conversiones automáticas a CIF

def validar_dni_es(dni):
    #print('entro validar dni esp')
    #print(f'DNI recibido: "{dni}"')  # Debug: ver qué llega exactamente
    if len(dni) != 9:
        return False
    numero = dni[:8]
    letra = dni[8].upper() 
    letras_dni = 'TRWAGMYFPDXBNJZSQVHLCKE'
    print('letra',letra)
    try:
        numero_int = int(numero)
        print(f'Número entero para cálculo: {numero_int}') 
        indice = numero_int % 23 
       
        print ('indice',indice)
        print ('letras indice',letras_dni[indice])
        return letras_dni[indice] == letra
    except ValueError:
        return False

def validar_nie_es(nie):
    #print(f'NIE recibido: "{nie}"')  # Debug: ver qué llega exactamente
    if len(nie) != 9:
        return False
    letra_inicial = nie[0].upper()
    if letra_inicial == 'X':
        numero = '0' + nie[1:8]
    elif letra_inicial == 'Y':
        numero = '1' + nie[1:8]
    elif letra_inicial == 'Z':
        numero = '2' + nie[1:8]
    else:
        return False
    letra_final = nie[8].upper()
    letras_dni = 'TRWAGMYFPDXBNJZSQVHLCKE'
    try:
        print(f'Número entero para cálculo: {numero}') 
        indice = int(numero) % 23
        print ('indice',indice)
        print ('letras indice',letras_dni[indice])
        return letras_dni[indice] == letra_final
    except ValueError:
        return False

def validar_cif_es(cif):
    """
    Valida CIF español según el algoritmo oficial
    """
    if len(cif) != 9:
        return False
    
    letra_inicial = cif[0]
    numero = cif[1:8]
    digito_control = cif[8]
    
    # Letras válidas para CIF
    letras_validas = 'ABCDEFGHJKLMNPQRSUVW'
    if letra_inicial not in letras_validas:
        return False
    
    # Validar que los 7 caracteres del medio sean números
    if not numero.isdigit():
        return False
    
    # Validar que el dígito de control sea número o letra A-J
    if digito_control not in '0123456789ABCDEFGHIJ':
        return False
    
    # Algoritmo oficial de validación
    suma_pares = 0
    suma_impares = 0
    
    for i, digito in enumerate(numero):
        valor = int(digito)
        if (i + 1) % 2 == 0:  # Posiciones pares (2ª, 4ª, 6ª)
            suma_pares += valor
        else:  # Posiciones impares (1ª, 3ª, 5ª, 7ª)
            doble = valor * 2
            suma_impares += doble // 10 + doble % 10
    
    total = suma_pares + suma_impares
    digito_calculado = (10 - (total % 10)) % 10
    #print('digito calculado: ',digito_calculado)
    #print('digito control: ',digito_control)
    
    # Validar dígito de control
    if digito_control in 'ABCDEFGHIJ':
        letras_control = 'JABCDEFGHI'
        digito_calculado_letra = letras_control[digito_calculado]
        return digito_control == digito_calculado_letra
    else:
        return str(digito_calculado) == digito_control

def validar_nif_es(nif):
    #return validar_dni_es(nif)
    """
    Valida NIF español - ahora solo para casos que no son DNI/NIE/CIF
    """
    # Si tiene formato de DNI o NIE, usar esas validaciones
    if re.match(r'^[0-9]{8}[A-Z]$', nif):
        return validar_dni_es(nif)
    elif re.match(r'^[XYZ][0-9]{7}[A-Z]$', nif):
        return validar_nie_es(nif)
    else:
        # Para otros tipos de NIF, validación más flexible
        return len(nif) >= 8 and len(nif) <= 10

def formatear_identificacion_es(identificacion, tipo):
    if tipo in ['DNI', 'NIF']:
        return f"{identificacion[:8]}-{identificacion[8:]}"
    elif tipo == 'NIE':
        return f"{identificacion[:1]}-{identificacion[1:8]}-{identificacion[8:]}"
    elif tipo == 'CIF':
        return f"{identificacion[:1]}-{identificacion[1:8]}-{identificacion[8:]}"
    else:
        return identificacion

# FUNCIONES PORTUGAL
def determinar_tipo_identificacion_pt(identificacion):
    """
    Determina el tipo de identificación portuguesa
    """
    if re.match(r'^[0-9]{8}[0-9]$', identificacion) and len(identificacion) == 9:
        return 'CC'  # Cartão de Cidadão (nuevo)
    elif re.match(r'^[0-9]{9}$', identificacion):
        return 'NIF'  # Número de Identificação Fiscal
    elif re.match(r'^[0-9]{7,8}[0-9]$', identificacion) and len(identificacion) in [8, 9]:
        return 'BI'  # Bilhete de Identidade (antiguo)
    else:
        return 'Desconocido'

def validar_cc_pt(cc):
    """
    Valida Cartão de Cidadão portugués (algoritmo MOD 11-2)
    """
    if len(cc) != 9:
        return False
    
    # Los primeros 8 dígitos
    numero = cc[:8]
    digito_control = int(cc[8])
    
    # Calcular dígito de control
    suma = 0
    for i, digito in enumerate(numero):
        peso = 9 - i
        suma += int(digito) * peso
    
    resto = suma % 11
    digito_calculado = 11 - resto if resto != 0 else 0
    
    # Si el dígito calculado es 10, se convierte en 0
    if digito_calculado == 10:
        digito_calculado = 0
    
    print('digito_calculado:',digito_calculado)
    return digito_calculado == digito_control

def validar_nif_pt(nif):
    """
    Valida NIF portugués (Número de Identificação Fiscal)
    """
    if len(nif) != 9:
        return False
    
    # El primer dígito debe ser 1, 2, 5, 6, 8 o 9
    primer_digito = int(nif[0])
    if primer_digito not in [1, 2, 5, 6, 8, 9]:
        return False
    
    # Algoritmo de validación MOD 11
    suma = 0
    for i in range(8):
        digito = int(nif[i])
        peso = 9 - i
        suma += digito * peso
    
    resto = suma % 11
    digito_control_calculado = 11 - resto if resto > 1 else 0
    
    return digito_control_calculado == int(nif[8])

def validar_bi_pt(bi):
    """
    Valida Bilhete de Identidade portugués (antiguo)
    """
    length = len(bi)
    if length not in [8, 9]:
        return False
    
    # Para BI, la validación es más simple (solo formato)
    # Los primeros length-1 dígitos son el número, el último es dígito de control
    try:
        numero = int(bi[:-1])
        digito_control = int(bi[-1])
        return True  # Para BI antiguo, la validación es menos estricta
    except ValueError:
        return False

def validar_identificacion_portuguesa(identificacion):
    """
    Valida identificaciones portuguesas (CC, NIF, BI)
    """
    tipo = determinar_tipo_identificacion_pt(identificacion)
    
    if tipo == 'CC':
        valido = validar_cc_pt(identificacion)
    elif tipo == 'NIF':
        valido = validar_nif_pt(identificacion)
    elif tipo == 'BI':
        valido = validar_bi_pt(identificacion)
    else:
        return {
            'valido': False,
            'pais': 'Portugal',
            'tipo': 'Desconocido',
            'error': 'Formato portugués no reconocido'
        }
    
    return {
        'valido': valido,
        'pais': 'Portugal',
        'tipo': tipo,
        'identificacion': identificacion,
        'formateado': formatear_identificacion_pt(identificacion, tipo)
    }

def formatear_identificacion_pt(identificacion, tipo):
    """
    Formatea identificaciones portuguesas
    """
    if tipo == 'CC':
        return f"{identificacion[:8]} {identificacion[8:]}"
    elif tipo == 'NIF':
        return f"{identificacion[:3]} {identificacion[3:6]} {identificacion[6:]}"
    elif tipo == 'BI':
        return identificacion
    else:
        return identificacion

def es_mayor_de_edad(fecha_nacimiento_str):
    
    try:
        # Convertir la cadena de texto a un objeto de fecha
        fecha_nacimiento = datetime.strptime(fecha_nacimiento_str, '%Y-%m-%d').date()
    except ValueError:
        # Maneja el caso de que el formato de fecha sea incorrecto
        print("Error: El formato de fecha no es válido. Debe ser 'AAAA-MM-DD'.")
        return False
        
    # Obtener la fecha actual
    fecha_actual = datetime.now()

    # Calcular la edad en años. Se resta 1 si la fecha de cumpleaños de este año
    # aún no ha llegado.
    edad = fecha_actual.year - fecha_nacimiento.year
    #print('resultado edad',edad)
    if (fecha_actual.month, fecha_actual.day) < (fecha_nacimiento.month, fecha_nacimiento.day):
        edad -= 1

    # Devolver True si la edad es mayor o igual a 18
    return edad >= 18

def generar_fecha_juliana():
    # Obtener la fecha actual en formato de fecha juliana
    fecha_actual = datetime.now().strftime('%Y%j')
    return fecha_actual

def creaNum10(session): 
    result = []
    # Generar la fecha juliana
    fecha_juliana = generar_fecha_juliana()
    fecha_juliana = fecha_juliana[2:7]
    #print('fecha_juliana',fecha_juliana)
    # Genera un número aleatorio de 9 dígitos
    numero =genfol04(session)
    #print('numero', numero)
    numero_c = fecha_juliana + str(numero) 
    #print('numero', numero_c)
    # Calcula el dígito verificador usando suma ponderada
    suma_ponderada = sum(int(numero_c[i]) * (i + 1) for i in range(len(numero_c)))
    #print('suma_ponderada :',suma_ponderada)
    digito_verificador = suma_ponderada % 10  
    # Asegura que el dígito verificador no sea cero
    if digito_verificador == 0:
        digito_verificador = 1
    # Combina el número de cuenta y el dígito verificador
    numero_c =  numero_c + str(digito_verificador)       
    return numero_c

def busca_activ_econ(giro,session):
    clave = giro
    llave_secu = 'ACTECON' + giro[0]
    acteconp = 'NO EXISTE ACTIV ECON'
    actecons = 'GIRO INEXISTENTE'
 
    actecon_princ = session.query(DBTCORP).order_by(DBTCORP.id.desc()).where(DBTCORP.llave=='ACTECON',DBTCORP.clave==llave_secu).first() 

    if actecon_princ:
        if actecon_princ.datos != '':
            acteconp = actecon_princ.datos
            actecon_secu = session.query(DBTCORP).order_by(DBTCORP.id.desc()).where(DBTCORP.llave==llave_secu,
                                                                       DBTCORP.clave==clave).first() 
            if actecon_secu:
                actecons = actecon_secu.datos
        else:
            acteconp = 'NO EXISTE ACTIV ECON'
            actecons = 'GIRO INEXISTENTE'
    else:
        acteconp = 'NO EXISTE ACTIV ECON'
        actecons = 'GIRO INEXISTENTE'
    
    return acteconp,actecons


def obtener_entidad_unica(db):
    intentos = 0
    max_intentos = 50  
    
    while intentos < max_intentos:
        entidad = f"{random.randint(0, 9999):04d}"
        
        if not db.query(DBENTIDAD).filter(DBENTIDAD.entidad == entidad).first():
            return entidad
        
        intentos += 1
    
    for i in range(1000, 10000):
        entidad = str(i)
        if not db.query(DBENTIDAD).filter(DBENTIDAD.entidad == entidad).first():
            return entidad

def validar_dato_obligatorio(valor, tipo_dato):
    # Verifica si un campo tiene un valor válido basado en su tipo de dato.

    if valor is None:
        return False

    if tipo_dato == 'string':
        # Para strings, se verifica que no sea una cadena vacía o solo espacios en blanco.
        return isinstance(valor, str) and bool(valor.strip())
    
    elif tipo_dato == 'int':
        # Para enteros, se verifica que no sea nulo. Un 0 es un valor válido.
        return isinstance(valor, int)
    
    elif tipo_dato == 'float':
        # Para flotantes, se verifica que no sea nulo. Un 0.0 es un valor válido.
        return isinstance(valor, float)
        
    elif tipo_dato == 'bool':
        # Para booleanos, se verifica que sea un booleano (True o False).
        return isinstance(valor, bool)

    # Si el tipo de dato no es reconocido, se asume que no es válido.
    return False

def busca_cp(pais,cod_postal,session):

    clave_pais = busca_clave_pais(pais,session)          
    if clave_pais == 'NO EXISTE':    
       return({'response': 'Código de pais inexistente'})
    else:   
        try:
        # Opción 1: API de Geonames (gratuita)
        # username = 'tu_usuario_geonames'  # Necesitas registrarte en geonames.org
        # url = f"http://api.geonames.org/postalCodeSearchJSON?postalcode={cod_postal}&country={pais}&username={username}"
            
        # Opción 2: API de Zippopotam (gratuita)
            url = f"http://api.zippopotam.us/{clave_pais}/{cod_postal}"
            
        # Opción 3: Si tienes una API específica para la región Ibérica
        # url = f"https://tu-api-iberica.com/validar-cp?cod_postal={cod_postal}&pais={pais}"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
               api_data = response.json()
               #print('api_data: ',api_data)
                
        # Procesamiento según la API utilizada
               if 'places' in api_data:  # Para Zippopotam
                    if api_data['places']:
                       lugar = api_data['places'][0]
                       ciudad = lugar.get('place name', 'Desconocido')
                       longitud = lugar.get('longitude', 'Desconocido')
                       latitud = lugar.get('latitude', 'Desconocido')
                        
                       return({
                            'cod_postal': cod_postal,
                            'ciudad': ciudad,
                            'pais': clave_pais,
                            'longitud' : longitud,
                            'latitud' : latitud
                    #'api_response': api_data  # Opcional: incluir respuesta completa
                          })
                
                    else:
                        return({'response': 'Código postal no encontrado'})
                
                # Para Geonames (descomentar si usas esta API)
                # elif 'postalCodes' in api_data:
                #     if api_data['postalCodes']:
                #         cp_info = api_data['postalCodes'][0]
                #         result.append({
                #             'cod_postal': cp_info.get('postalCode', cod_postal),
                #             'ciudad': cp_info.get('placeName', 'Desconocido'),
                #             'pais': cp_info.get('countryCode', pais)
                #         })
                #         rc = 201
                #     else:
                #         result.append({'response': 'Código postal no encontrado'})
                #         estado = False
                #         rc = 404
                
               else:
                    return({'response': 'Formato de API no reconocido'})

            else:
                return({'response': f'Error en la API externa: {response.status_code}'})
                
        except requests.exceptions.Timeout:
            return({'response': 'Timeout al conectar con la API'})
        except requests.exceptions.ConnectionError:
            return({'response': 'Error de conexión con la API'})
        except requests.exceptions.RequestException as e:
            return({'response': f'Error en la solicitud: {str(e)}'})

def busca_clave_pais(pais,session):
    datos=pais
    #print('pais_entrada:',pais)
    clave_p = session.query(DBTCORP).order_by(DBTCORP.id.desc()).where(DBTCORP.llave=='PAISUE',
                                                                       DBTCORP.datos==datos).first() 
    #print('clave_pais: ',clave_p.clave)
    if clave_p:
        clave_pais = clave_p.clave
    else:
        clave_pais = 'NO EXISTE'
    
    return clave_pais

def cargar_df_relaciones(session):
    """Carga DataFrame UNA sola vez"""
    try:
        # Cargar relaciones activas
        query_relaciones = session.query(DBRELPERS.num_id_princ,
                                         DBRELPERS.num_id_relac,
                                         DBRELPERS.tipo_relac,
                                         DBRELPERS.nivel_relac,
                                         DBRELPERS.porcentaje_partic
                                        ).order_by(desc(DBRELPERS.id)).where(
                                                        DBRELPERS.estatus == 'A').all() 
         
        df_relaciones = pd.DataFrame(query_relaciones, columns=[
        'num_id_princ', 'num_id_relac', 'tipo_relac', 'nivel_relac', 'porcentaje_partic'])
                
        return df_relaciones
        
    except Exception as e:
        print(f"Error cargando DataFrame relaciones: {e}")
        return pd.DataFrame()

def cargar_df_personas(session):
    """Carga DataFrame UNA sola vez"""
    try:        
        # Cargar personas activas
        query_personas = session.query(DBDGENPERS.num_id,
                                       DBDGENPERS.tipo_per,
                                       DBDGENPERS.entidad
                                       ).order_by(desc(DBDGENPERS.id)).where(
                                                       DBDGENPERS.estatus == 'A').all() 
        df_personas = pd.DataFrame(query_personas, columns=['num_id', 'tipo_per','entidad'])
        
        return df_personas
        
    except Exception as e:
        print(f"Error cargando DataFrame personas: {e}")
        return pd.DataFrame()

def obtener_info_persona(df_personas, num_id):
    """Usa el DataFrame ya cargado - SIN acceso a BD"""
    if df_personas.empty:
        return None
        
    persona = df_personas[df_personas['num_id'] == num_id]
    if not persona.empty:
        return {
            'num_id': persona.iloc[0]['num_id'],
            'tipo_per': persona.iloc[0]['tipo_per'],
            'entidad': persona.iloc[0]['entidad']
        }
    return None

def busca_tipos_rel(session):

    tipos_rel = session.query(DBTCORP).where(
    or_(DBTCORP.llave == 'TIPORELF',
        DBTCORP.llave == 'TIPORELM'
       )).order_by(DBTCORP.id.desc()).all()
    #print('tipos_rel: ',tipos_rel)
    cadena_trelf = []
    cadena_trelm = []
    # if tipos_rel:
    #    for registro in tipos_rel:
    #        if registro.clave:
    #           if registro.llave == 'TIPORELF':
    #              cadena_trelf.append(registro.clave)
    #           elif registro.llave == 'TIPORELM':
    #                cadena_trelm.append(registro.clave)

    if tipos_rel:
        cadena_trelf = [registro.clave for registro in tipos_rel if registro.clave and registro.llave == 'TIPORELF']
        cadena_trelm = [registro.clave for registro in tipos_rel if registro.clave and registro.llave == 'TIPORELM']
    else:
        cadena_trelf = []
        cadena_trelm = []
    #print('cadena trelf:',cadena_trelf)
    return cadena_trelf, cadena_trelm

def verificar_relacion_existente(df_relaciones, num_id_princ, num_id_relac, tipo_relac):
    """Usa el DataFrame ya cargado - SIN acceso a BD"""
    if df_relaciones.empty:
        return False
        
    existe = ((df_relaciones['num_id_princ'] == num_id_princ) & 
             (df_relaciones['num_id_relac'] == num_id_relac) & 
             (df_relaciones['tipo_relac'] == tipo_relac)).any()
    
    return existe

def calcular_suma_porcentajes(df_relaciones, num_id_princ, tipo_relac, num_id_relac, funcion):
    """Usa el DataFrame ya cargado - SIN acceso a BD"""
    if df_relaciones.empty:
        return 0

    if funcion == 'R':    
        filtro = (df_relaciones['num_id_princ'] == num_id_princ) & (df_relaciones['tipo_relac'] == tipo_relac)
    else:
        filtro = ((df_relaciones['num_id_princ'] == num_id_princ) & 
                  (df_relaciones['tipo_relac'] == tipo_relac) &
                  (df_relaciones['num_id_relac'] != num_id_relac)) 
    
    relaciones_filtradas = df_relaciones[filtro]
    
    if relaciones_filtradas.empty:
        return 0
        
    suma = relaciones_filtradas['porcentaje_partic'].sum()
    return suma if not pd.isna(suma) else 0


def valtipdat(tipov,datosv):
    valid=False
    enmascar=''
    match tipov:
        #IBAN
        case '001':      
           numiban=datosv[:36] 
           valid=validate_iban(numiban)
           enmascar=enmascarar_cuenta(numiban) 
        #PAN
        case '002':
            numpan=datosv[:16] 
            valid=valNumTarjeta(numpan)
            enmascar=enmascarar_cuenta(numpan) 

        #NUMCTA
        case '003':
             numcta=datosv[:22] 
             enmascar=enmascarar_cuenta(numcta) 
             if not valcampo('num',numcta,22):
                 valid=False
             else:
                valid=True
        case _:
            valid=False
    return valid,enmascar

def validate_iban(iban: str, validate_country_specific: bool = True) -> bool:
    """
    Valida un IBAN según el estándar ISO 13616, con validación adicional
    del CCC para cuentas españolas (código país 'ES').

    Args:
        iban (str): IBAN a validar
        validate_country_specific (bool): Habilita validación específica por país

    Returns:
        bool: True si el IBAN es válido, False en caso contrario
    """
    # Limpieza del IBAN: eliminar espacios y convertir a mayúsculas
    iban_clean = iban.replace(' ', '').replace('\t', '').upper()

    # Validación básica de caracteres
    if not iban_clean.isalnum() or len(iban_clean) < 4:
        return False

    # Extraer código de país y verificar
    country_code = iban_clean[:2]
    if not country_code.isalpha():
        return False

    # Longitudes específicas por país (fuente: SWIFT IBAN Registry 2023)
    country_lengths = {
        'ES': 24, 'DE': 22, 'FR': 27, 'IT': 27, 'GB': 22, 'CH': 21,
        'PT': 25, 'NL': 18, 'BE': 16, 'AT': 20, 'DK': 18, 'FI': 18,
        'SE': 24, 'NO': 15, 'PL': 28, 'CZ': 24, 'SK': 24, 'HU': 28
    }

    # Validar longitud según país
    if country_code in country_lengths and len(iban_clean) != country_lengths[country_code]:
        return False

    # Reordenar: Mover los 4 primeros caracteres al final
    rearranged = iban_clean[4:] + iban_clean[:4]

    # Convertir letras a números (A=10, B=11, ... Z=35)
    digits_str = ''
    for char in rearranged:
        if char.isdigit():
            digits_str += char
        elif 'A' <= char <= 'Z':
            digits_str += str(ord(char) - ord('A') + 10)
        else:
            return False  # Carácter inválido
    # Validación módulo 97
    remainder = 0
    for digit in digits_str:
        remainder = (remainder * 10 + int(digit)) % 97
    if remainder != 1:
        return False

    # Validación específica para España (CCC)
    if validate_country_specific and country_code == 'ES' and len(iban_clean) == 24:
        ccc = iban_clean[4:]  # Obtener los 20 dígitos después de 'ESXX'

        if not ccc.isdigit():
            return False

        # Partes del CCC
        bank_code = ccc[:4]       # Código bancario
        branch_code = ccc[4:8]    # Código de sucursal
        check_digits = ccc[8:10]  # Dígitos de control
        account_number = ccc[10:] # Número de cuenta

        # Validación primer dígito de control (entidad + sucursal)
        weights1 = [1, 2, 4, 8, 5, 10, 9, 7]
        total1 = sum(int(bank_code[i]) * weights1[i] for i in range(4))
        total1 += sum(int(branch_code[j]) * weights1[j+4] for j in range(4))
        expected_digit1 = 11 - (total1 % 11)
        expected_digit1 = 1 if expected_digit1 == 10 else 0 if expected_digit1 == 11 else expected_digit1

        # Validación segundo dígito de control (número de cuenta)
        weights2 = [1, 2, 4, 8, 5, 10, 9, 7, 3, 6]
        total2 = sum(int(account_number[k]) * weights2[k] for k in range(10))
        expected_digit2 = 11 - (total2 % 11)
        expected_digit2 = 1 if expected_digit2 == 10 else 0 if expected_digit2 == 11 else expected_digit2

        # Comparar con los dígitos reales
        if int(check_digits[0]) != expected_digit1 or int(check_digits[1]) != expected_digit2:
            return False

    return True

def enmascarar_cuenta(numero_cuenta: str, caracteres_visibles: int = 4) -> str:
    
    if not isinstance(numero_cuenta, str):
        # Opcional: convertir a str si viene como int/float
        numero_cuenta = str(numero_cuenta)
        
    if len(numero_cuenta) <= caracteres_visibles:
        return numero_cuenta # Si es muy corto, muestra todo
    
    # Calcular la parte a enmascarar
    parte_enmascarada = '*' * (len(numero_cuenta) - caracteres_visibles)
    
    # Obtener los últimos caracteres visibles
    parte_visible = numero_cuenta[-caracteres_visibles:]
    
    return parte_enmascarada + parte_visible

def valNumTarjeta(numtarjeta):
    estado = False
    cadena = numtarjeta[:15]
    
    try:
        valcadena = int(cadena)
    except ValueError:
        return False
    except TypeError:
        return False
    
    try:
        valdig=int(numtarjeta[15])
    except ValueError:
        return False
    except TypeError:
        return False

    dvo = int(numtarjeta[15])
    pesos = [2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2]
    suma = 0
    for i in range(15):
        producto = int(cadena[i]) * pesos[i]
        if producto > 9:
            producto -= 9
        suma += producto
    dvc = 10 - (suma % 10)
    if dvc == 10:
        dvc = 0
    if dvo == dvc:
        estado = True      
    
    return estado

def busca_moneda_local(pais,session):
 
    print('pais_entrada:',pais)
    moneda_loc = session.query(DBTCORP).order_by(DBTCORP.id.desc()).where(
                                                 DBTCORP.llave == 'PAISMONL',
                                                func.substr(DBTCORP.datos, 1, 2) == pais).first()
    #print('moneda: ',moneda.datos)
    if moneda_loc:
        moneda = moneda_loc.clave
    else:
        moneda = 'NO EXISTE'
    
    return moneda

def busca_tipo_cambio(moneda, session):
    """
    Usando el XML oficial del Banco Central Europeo
    """
    try:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        response = requests.get(url)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        # Namespace del XML
        ns = {'ns': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}
        #print('estoy en tipo cambio, respuesta: ', response)
        
        # Buscar la moneda en el XML
        for cube in root.findall('.//ns:Cube[@currency]', ns):
            if cube.get('currency') == moneda.upper():
                return float(cube.get('rate'))
        
        # Si no encuentra la moneda, retorna 0
        print(f"Moneda {moneda} no encontrada, retornando 0")
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 0
    

def generar_skyc(id_per, state_per, fecha_nac, act_econ, session):

    # """
    # Genera una simulación KYC para una persona, orientada a la comunidad europea.
    # Args:
    #     id_per (str): Id de la persona.
    #     state_per   : Pais de residencia de la persona.
    #     fecha_nac (str): Fecha de nacimiento en formato 'YYYY-MM-DD'.
    # Returns:
    #     dict: Un diccionario con el resultado de la simulación KYC.
    # """

    resultado_kyc = {

        "persona": {
            "id_persona": id_per,
            "fecha_nacimiento": fecha_nac,
            "edad": None
        },

        "evaluacion_riesgo": {
            "riesgo_geografico": "B",
            "riesgo_edad": "B",
            "riesgo_act_econ": "B",
            "riesgo_total": "B"
        },

        "historial_crediticio": {
            "puntuacion_fico_simulada": 0,
            "categoria_fico": "B"
        },

         "transacciones_simuladas": [],
          "patrones_detectados": {
              "transacciones_frecuentes": 0,
              "repeticion_montos": 0,
              "multiples_beneficiarios_sospechosos": 0,
              "transacciones_alto_valor": 0,
              "paises_alto_riesgo_afectados": 0,
              "estructuracion_detectada": 0
          },

        "transacciones_sospechosas_marcadas": 0,
        "aprobacion_kyc": "False",
        "razon_aprobacion_rechazo": []

    }

    # --- 1. Evalúa su riesgo geográfico y de edad ---
    # Recuperación de países de la UE desde DBTCORP
    try:
        paises_ue_query = session.query(DBTCORP).filter_by(llave='PAISUE', status='A').all()
        paises_ue = [p.datos for p in paises_ue_query]
        
        # Recuperación de países de alto riesgo desde DBTCORP
        paises_alto_riesgo_ue_query = session.query(DBTCORP).filter_by(llave='PAISRSGO', status='A').all()
        paises_alto_riesgo_ue = [p.datos for p in paises_alto_riesgo_ue_query]

        # Recuperación de actividades econ de alto riesgo desde DBTCORP
        actecon_alto_riesgo_ue_query = session.query(DBTCORP).filter_by(llave='ACTECORZ', status='A').all()
        actecon_alto_riesgo_ue = [a.clave for a in actecon_alto_riesgo_ue_query]
        
        if not paises_ue:
            # Fallback o manejo de error si no se encuentran países de la UE
            paises_ue = ["España", "Alemania", "Francia", "Italia", "Portugal", "Bélgica", "Países Bajos"]
            print("Advertencia: No se encontraron países de la UE en la base de datos, usando lista predeterminada.")
        if not paises_alto_riesgo_ue:
            # Fallback o manejo de error si no se encuentran países de alto riesgo
            paises_alto_riesgo_ue = [
                "Corea del Norte", "Irán", "Siria", "Yemen", "República Democrática del Congo",
                "Haití", "Marruecos", "Filipinas", "Panamá", "Sudáfrica", "Nigeria", "Kenia",
                "Angola", "Mozambique", "Myanmar", "Tanzania", "Colombia"
            ]
            print("Advertencia: No se encontraron países de alto riesgo en la base de datos, usando lista predeterminada.")
        if not actecon_alto_riesgo_ue:
            # Fallback o manejo de error si no se encuentran países de alto riesgo
            actecon_alto_riesgo_ue = [
                "C32.11", "L64.31","L64.19","N69.2", "M68.32", "M68.31",
                "M68.2", "M68.11", "S92", "G46.82", "G46.12", "C24.41", "C24.45"            ]
            print("Advertencia: No se encontraron activ econ de alto riesgo en la base de datos, usando lista predeterminada.")

    except Exception as e:
        print(f"Error al cargar países desde la base de datos: {e}")
        # En caso de error de DB, usar listas predefinidas para que la función no falle
        paises_ue = ["España", "Alemania", "Francia", "Italia", "Portugal", "Bélgica", "Países Bajos"]
        paises_alto_riesgo_ue = [
            "Corea del Norte", "Irán", "Siria", "Yemen", "República Democrática del Congo",
            "Haití", "Marruecos", "Filipinas", "Panamá", "Sudáfrica", "Nigeria", "Kenia",
            "Angola", "Mozambique", "Myanmar", "Tanzania", "Colombia"
        ]
        resultado_kyc["razon_aprobacion_rechazo"].append(f"Error al cargar catálogos de países: {e}")


    # El riesgo geográfico se activará por los países de destino de las transacciones.
    # Verificar el país de residencia (state_per)
    if state_per not in paises_ue:
        resultado_kyc["evaluacion_riesgo"]["riesgo_geografico"] = "Medio"
        #resultado_kyc["razon_aprobacion_rechazo"].append(f"País de residencia '{state_per}' no es un país de la UE. Riesgo geográfico inicial elevado a Medio.")
    else:
        resultado_kyc["evaluacion_riesgo"]["riesgo_geografico"] = "Bajo"


    # Evaluación de edad
    try:
        fecha_nacim = fecha_nac.date()
        fn = fecha_nacim
        hoy = datetime.now()
        edad = hoy.year - fn.year - ((hoy.month, hoy.day) < (fn.month, fn.day))
        
        resultado_kyc["persona"]["edad"] = edad

        if edad < 18 or edad > 75: # Menores de edad o muy mayores pueden tener un perfil de riesgo diferente
            resultado_kyc["evaluacion_riesgo"]["riesgo_edad"] = "Medio"
        elif 20 <= edad <= 65:
            resultado_kyc["evaluacion_riesgo"]["riesgo_edad"] = "Bajo"
        else:
            resultado_kyc["evaluacion_riesgo"]["riesgo_edad"] = "Medio" # Edades límite

    except ValueError:
        resultado_kyc["evaluacion_riesgo"]["riesgo_edad"] = "Indef"
        resultado_kyc["razon_aprobacion_rechazo"].append("Error: Formato de fecha de nacimiento inválido.")

    # Verificar la actividad económica (act_econ)
    if act_econ not in actecon_alto_riesgo_ue:
        resultado_kyc["evaluacion_riesgo"]["riesgo_act_econ"] = "Medio"
    else:
        resultado_kyc["evaluacion_riesgo"]["riesgo_act_econ"] = "Bajo"
    
    #print('riesgo act econ', resultado_kyc["evaluacion_riesgo"]["riesgo_act_econ"])
    # Ajuste del riesgo total inicial (antes de las transacciones)
    if  resultado_kyc["evaluacion_riesgo"]["riesgo_edad"] == "Medio" or \
        resultado_kyc["evaluacion_riesgo"]["riesgo_geografico"] == "Medio" or \
        resultado_kyc["evaluacion_riesgo"]["riesgo_act_econ"] == "Medio":
        resultado_kyc["evaluacion_riesgo"]["riesgo_total"] = "Medio"

    # --- 2. 50 Transacciones simuladas ---
    beneficiarios_comunes = ["Proveedor X", "Servicios Y", "Inversiones Z"]
    beneficiarios_inusuales = ["Empresa Offshore A", "Fondo de Inversión B", "Cash & Go Inc."] # Para simular múltiples beneficiarios sospechosos
    start_date = datetime.now() - timedelta(days=90) # Transacciones de los últimos 90 días
    montos_estructuracion = [random.randint(900, 999) for _ in range(5)] # 5 montos para simular estructuración

    for i in range(50):
        # Fecha y hora (simulando transacciones frecuentes)
        fecha_transaccion = start_date + timedelta(seconds=random.randint(0, 90*24*3600))
        if i % 5 == 0 and i > 0: # Simular transacciones frecuentes (intervalos < 1 hora)
            # CONVERTIMOS LA CADENA DE FECHA DE NUEVO A OBJETO DATETIME
            fecha_transaccion_anterior_str = resultado_kyc["transacciones_simuladas"][-1]["fecha"]
            fecha_transaccion_anterior_dt = datetime.fromisoformat(fecha_transaccion_anterior_str) # <-- CORRECCIÓN AQUÍ
            fecha_transaccion = fecha_transaccion_anterior_dt + timedelta(minutes=random.randint(1, 59))
            resultado_kyc["patrones_detectados"]["transacciones_frecuentes"] += 1

        # Monto
        monto = round(random.uniform(10, 10000), 2)
        if i % 7 == 0: # Simular repetición de montos
            monto = random.choice([100.00, 250.00, 500.00, 1000.00])
            resultado_kyc["patrones_detectados"]["repeticion_montos"] += 1

        if i % 10 == 0: # Simular transacciones de alto valor
            monto = round(random.uniform(5500, 20000), 2)
            resultado_kyc["patrones_detectados"]["transacciones_alto_valor"] += 1

        if i % 15 == 0 and len(montos_estructuracion) > 0: # Simular estructuración
            monto = montos_estructuracion.pop(0)
            resultado_kyc["patrones_detectados"]["estructuracion_detectada"] += 1

        # Beneficiario y País de destino
        beneficiario = random.choice(beneficiarios_comunes)
        pais_destino = random.choice(paises_ue)
 

        if random.random() < 0.15: # 15% de probabilidad de transacciones a países de alto riesgo
            pais_destino = random.choice(paises_alto_riesgo_ue)
            resultado_kyc["patrones_detectados"]["paises_alto_riesgo_afectados"] += 1
            resultado_kyc["evaluacion_riesgo"]["riesgo_geografico"] = "Alto" # Elevar riesgo si hay transacciones a estos países
            resultado_kyc["evaluacion_riesgo"]["riesgo_total"] = "Alto"

        if random.random() < 0.10: # 10% de probabilidad de usar beneficiarios inusuales
            beneficiario = random.choice(beneficiarios_inusuales)
            resultado_kyc["patrones_detectados"]["multiples_beneficiarios_sospechosos"] += 1

            if resultado_kyc["evaluacion_riesgo"]["riesgo_total"] != "Alto":
                 resultado_kyc["evaluacion_riesgo"]["riesgo_total"] = "Medio" # Subir a medio si hay beneficiarios sospechosos

        transaccion = {
            "fecha": fecha_transaccion.isoformat(),
            "monto": monto,
            "beneficiario": beneficiario,
            "pais_destino": pais_destino,
            "sospechosa": False # Se marcará después
        }

        resultado_kyc["transacciones_simuladas"].append(transaccion)

    # --- 3. Asigna una puntuación FICO simulada y determina su categoría ---
    fico_score = random.randint(300, 850)
    resultado_kyc["historial_crediticio"]["puntuacion_fico_simulada"] = fico_score

    if 300 <= fico_score <= 579:
        resultado_kyc["historial_crediticio"]["categoria_fico"] = "Baja"
    elif 580 <= fico_score <= 669:
        resultado_kyc["historial_crediticio"]["categoria_fico"] = "Regular"
    elif 670 <= fico_score <= 739:
        resultado_kyc["historial_crediticio"]["categoria_fico"] = "Buena"
    elif 740 <= fico_score <= 799:
        resultado_kyc["historial_crediticio"]["categoria_fico"] = "Muy Buena"
    else: # 800-850
        resultado_kyc["historial_crediticio"]["categoria_fico"] = "Excelente"

    # --- 4. Simula transacciones y marca las sospechosas (> $3000) ---

    for transaccion in resultado_kyc["transacciones_simuladas"]:
        if transaccion["monto"] > 3000:
            transaccion["sospechosa"] = True
            resultado_kyc["transacciones_sospechosas_marcadas"] += 1

    # --- 5. Decide si aprueba el KYC ---
    aprobado = True
    razones = []
 
    # Condición 1: El riesgo no es Alto
    if resultado_kyc["evaluacion_riesgo"]["riesgo_total"] == "Alto":
        aprobado = False
        razones.append("Riesgo total de cliente clasificado como Alto.")

    # Condición 2: La puntuación crediticia es Alta o Excelente
    if resultado_kyc["historial_crediticio"]["categoria_fico"] not in ["Muy Buena", "Excelente"]:
        aprobado = False
        razones.append(f"Puntuación crediticia ('{resultado_kyc['historial_crediticio']['categoria_fico']}') no es Muy Buena o Excelente.")

    # Condición 3: No hay transacciones sospechosas
    if resultado_kyc["transacciones_sospechosas_marcadas"] > 0:
        aprobado = False
        razones.append(f"Se detectaron {resultado_kyc['transacciones_sospechosas_marcadas']} transacciones sospechosas.")

    # Condición 4 (Hueco detectado): Si se detecta estructuración, siempre rechazar.
    # La estructuración es un fuerte indicador de lavado de dinero.
    if resultado_kyc["patrones_detectados"]["estructuracion_detectada"] > 0:
        aprobado = False
        razones.append("Se detectó estructuración de transacciones, lo cual es una señal de alerta grave.")

    # Si pasa todas las condiciones, se aprueba
    if aprobado:
        resultado_kyc["aprobacion_kyc"] = "True"
        resultado_kyc["razon_aprobacion_rechazo"].append("Cliente cumple los criterios de aprobación KYC.")
    else:
        resultado_kyc["aprobacion_kyc"] = "False"
        if not razones: # Si no se añadió ninguna razón, algo salió mal
             razones.append("Decisión de aprobación/rechazo no pudo ser determinada claramente.")

        resultado_kyc["razon_aprobacion_rechazo"] = razones
 

    return resultado_kyc

def generar_iban_interno(codigo_banco='9999', sucursal='0001', numero_cuenta=None):

    # 1. Validar/Generar Código de Banco
    if not re.match(r'^\d{4}$', codigo_banco):
        raise ValueError("El código de banco debe tener 4 dígitos")
    
    # 2. Validar/Generar Sucursal
    if not re.match(r'^\d{4}$', sucursal):
        raise ValueError("La sucursal debe tener 4 dígitos")

    # 3. Generar Número de Cuenta (si no viene informado)
    if numero_cuenta is None:
        account_number = f"{random.randint(0, 9999999999):010d}"
    else:
        if not re.match(r'^\d{10}$', numero_cuenta):
            raise ValueError("El número de cuenta debe tener 10 dígitos")
        account_number = numero_cuenta

    # 4. CALCULO DE DÍGITOS DE CONTROL (CCC) - USANDO TUS PESOS DE VALIDACIÓN
    # Pesos para primer dígito (Entidad + Sucursal)
    weights1 = [1, 2, 4, 8, 5, 10, 9, 7]
    total1 = sum(int(codigo_banco[i]) * weights1[i] for i in range(4))
    total1 += sum(int(sucursal[j]) * weights1[j+4] for j in range(4))
    
    dc1 = 11 - (total1 % 11)
    dc1 = 1 if dc1 == 10 else 0 if dc1 == 11 else dc1

    # Pesos para segundo dígito (Número de cuenta)
    weights2 = [1, 2, 4, 8, 5, 10, 9, 7, 3, 6]
    total2 = sum(int(account_number[k]) * weights2[k] for k in range(10))
    
    dc2 = 11 - (total2 % 11)
    dc2 = 1 if dc2 == 10 else 0 if dc2 == 11 else dc2

    # 5. Formar el BBAN (20 caracteres)
    # Estructura: Banco(4) + Sucursal(4) + DC(2) + Cuenta(10)
    check_digits_ccc = f"{dc1}{dc2}"
    bban = f"{codigo_banco}{sucursal}{check_digits_ccc}{account_number}"

    # 6. CALCULO DE DÍGITOS DE CONTROL IBAN (Módulo 97)
    # España es 'ES' -> E=14, S=28. Formato: BBAN + 1428 + 00
    pre_iban = bban + "142800"
    resto = int(pre_iban) % 97
    dc_iban = 98 - resto
    dc_iban_str = f"{dc_iban:02d}"

    # 7. IBAN Completo (24 caracteres)
    iban_final = f"ES{dc_iban_str}{bban}"
    enmascar=enmascarar_cuenta(iban_final)
   
    # Formatear para mostrar (en bloques de 4 caracteres)
    iban_formateado = ' '.join([iban_final[i:i+4] for i in range(0, len(iban_final), 4)])
   
    return iban_final, enmascar


def cargar_costos(entidad,session):
    try:
        # Cargar costos de la entidad activos
        query_costos = session.query(DBCOSTTRAN.indcost,
                                     DBCOSTTRAN.num_txs_libres,
                                     DBCOSTTRAN.costo_tx
                                        ).order_by(desc(DBCOSTTRAN.id)).where(
                                                        DBCOSTTRAN.entidad == entidad, DBCOSTTRAN.estatus == 'A').all() 
        
        if not query_costos:  # Verifica si la consulta devolvió resultados
            print(f"No se encontraron costos para la entidad {entidad}")
            return pd.DataFrame()
         
        df_costos = pd.DataFrame(query_costos, columns=[
        'indcost', 'num_txs_libres', 'costo_tx'])
                
        return df_costos
        
    except Exception as e:
        print(f"Error cargando DataFrame costos: {e}")
        return pd.DataFrame()
    
def calcula_costo_txs(costos_df, indcost_entrada, cantidad):
    
    # Buscar el indcost en el DataFrame
    filtro = costos_df[costos_df['indcost'] == indcost_entrada]
    
    if filtro.empty:
        print('Indcost de Tx no encontrado. Costo = 0',indcost_entrada)
        return 0.0
    
    fila = filtro.iloc[0]
    num_txs_libres = fila['num_txs_libres']
    costo_tx = fila['costo_tx']
    
    # Aplicar lógica según indcost
    if indcost_entrada in ['B', 'D']:
        calculo = float(cantidad) * float(costo_tx)
        return calculo
    
    elif indcost_entrada == 'C':
        if float(cantidad) > float(num_txs_libres):
            excedente = float(cantidad) - float(num_txs_libres)
            calculo = excedente * float(costo_tx)
            return calculo
        return 0.0
    
    else:
        print(f"Indcost {indcost_entrada} no reconocido. Costo = 0")
        return 0.0