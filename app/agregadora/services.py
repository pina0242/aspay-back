
from app.core.models import DBMOVTOS, DBCTAPERS, DBSALDOS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, valtipdat, obtener_fecha_actual, busca_moneda_local, busca_tipo_cambio
from app.core.interfaz import consulta_sdomov,login_al_banco, consulta_movagre
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from app.core.database import get_db, SessionLocal
from sqlalchemy import  func
from datetime import timedelta, datetime, date
from sqlalchemy.orm import Session,sessionmaker
import logging
import json 
import time
from concurrent.futures import ThreadPoolExecutor

from contextlib import contextmanager
from collections import defaultdict
#prueba GIT
logger = logging.getLogger(__name__)

class AgrService:
    
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

    

    
    def selctaagr(self, datos , user, db):
        result = []
        resumen = defaultdict(float)

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

        if estado:
            dataJSON = json.loads(decriptMsg)
            entidad = dataJSON.get("entidad")

            if not entidad:
                estado = False
                result.append({'response': 'Debe proporcionar entidad.'})
                rc = 400
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:

                                                
            ctas_entries = db.query(DBCTAPERS).order_by(DBCTAPERS.id.desc()).where(DBCTAPERS.entidad == entidad,DBCTAPERS.estatus=='A',DBCTAPERS.entban != '9999').all()
            ctas_entriesAS = db.query(DBCTAPERS).order_by(DBCTAPERS.id.desc()).where(DBCTAPERS.entidad == entidad,DBCTAPERS.estatus=='A',DBCTAPERS.entban == '9999').all() 

            def f0(ctas_entries):

                datos_decrypted, decrypt_estado = decrypt_message(password, ctas_entries.datos)
                cuenta = datos_decrypted

                saldoeasy = self.recsaldos(cuenta, ctas_entries.alias, db)


                saldo = saldoeasy["resp"][0].get("saldo", 0) if saldoeasy["resp"] else 0

                time.sleep(2)
                return saldo



            with ThreadPoolExecutor(max_workers=10) as executor:
                resultados = list(executor.map(f0, ctas_entries))
                print('resultados',resultados)
        

            # inicio = time.time()


            # fin = time.time()
            # tiempo_total = fin - inicio

            # print(f"\nTiempo total de ejecución: {tiempo_total:.2f} segundos")
            # print(f"Se ejecutaron {len(ctas_entries)} funciones en paralelo, cada una tarda 10 segundos")
            # print(f"En serie hubiera tardado {len(ctas_entries)*10} segundos, pero en paralelo solo {tiempo_total:.2f} segundos")



            if ctas_entries:
                cont = 0
                for entry in ctas_entries:


                    # Desencriptar el campo 'datos' después de recuperarlo de la DB
                    datos_decrypted, decrypt_estado = decrypt_message(password, entry.datos)
                    if not decrypt_estado:
                        datos_decrypted = "Error al desencriptar datos" # Manejar error de desencriptación
                    # print('entry moneda:', entry.moneda)

                    tipo_cambio = 0
                    if entry.moneda != 'EUR':
                        print('voy a tipo cambio por: ', entry.moneda)
                        tipo_cambio = busca_tipo_cambio(entry.moneda,db)
                     
                    if entry.entban == '9999':
                        print('entro ASPAY',entry.tknper)
                        ctas_saldos =  db.query(DBSALDOS).where( DBSALDOS.tkncli==entry.tknper,).first()
                        saldo = ctas_saldos.saldo
                    else:
                        # print('entro EASY RENT',entry.datos)
                        saldo = resultados[cont]
                        # cuenta = datos_decrypted
                        # saldoeasy = self.recsaldos(cuenta, entry.alias, db)

                        # # Siempre seguro: si no hay saldo, devuelve 0
                        # saldo = saldoeasy["resp"][0].get("saldo", 0) if saldoeasy["resp"] else 0

                    categoria = entry.categoria
                    importe = saldo
                    resumen[categoria] += importe

                    result.append({
                        'id': entry.id,
                        'entidad':entry.entidad,
                        'tknper': entry.tknper,
                        'pais': entry.pais,
                        'moneda': entry.moneda,
                        'tipo_cambio': tipo_cambio,
                        'entban': entry.entban,
                        'tipo': entry.tipo,
                        'alias': entry.alias,
                        'datos': datos_decrypted, # Devolver datos desencriptados
                        'indoper': entry.indoper,
                        'estatus': entry.estatus,
                        'categoria':entry.categoria,
                        'fecha_alta': entry.fecha_alta.strftime("%Y-%m-%d"),
                        'usuario_alta': entry.usuario_alta,
                        'saldo': saldo,
                        'resumen': resumen

                        })

                    cont += 1
            if ctas_entriesAS:

                for entryAS in ctas_entriesAS:

                    datos_decrypted, decrypt_estado = decrypt_message(password, entryAS.datos)
                    if not decrypt_estado:
                        datos_decrypted = "Error al desencriptar datos" # Manejar error de desencriptación


                    tipo_cambio = 0
                    if entryAS.moneda != 'EUR':
                        print('voy a tipo cambio por aspay: ', entryAS.moneda)
                        tipo_cambio = busca_tipo_cambio(entryAS.moneda,db)
                     

                    ctas_saldos =  db.query(DBSALDOS).where( DBSALDOS.tkncli==entryAS.tknper,).first()
                    if ctas_saldos:
                        saldoAS = ctas_saldos.saldo


                        categoria = entryAS.categoria
                        importe = saldoAS
                        resumen[categoria] += importe

                        result.append({
                            'id': entryAS.id,
                            'entidad':entryAS.entidad,
                            'tknper': entryAS.tknper,
                            'pais': entryAS.pais,
                            'moneda': entryAS.moneda,
                            'tipo_cambio': tipo_cambio,
                            'entban': entryAS.entban,
                            'tipo': entryAS.tipo,
                            'alias': entryAS.alias,
                            'datos': datos_decrypted, # Devolver datos desencriptados
                            'indoper': entryAS.indoper,
                            'estatus': entryAS.estatus,
                            'categoria':entryAS.categoria,
                            'fecha_alta': entryAS.fecha_alta.strftime("%Y-%m-%d"),
                            'usuario_alta': entryAS.usuario_alta,
                            'saldo': saldoAS,
                            'resumen': resumen

                            })



                print(dict(resumen))
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                print('llega vacio???',ctas_entriesAS)
                result.append({'response': 'No existen datos a listar con los criterios proporcionados.'})
                rc = 400
        return result, rc 

    def recsaldos(self, tknper, alias, db):

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
                datos = consulta_sdomov(
                    s_c,
                    euser,
                    eIp_Origen,
                    etimestar,
                    headertoken,
                    datosin=tknper,
                    alias=alias
                )

            # Validar que resp y resp1 sean listas de dicts
            resulta = datos.get("resp", [])


            if isinstance(resulta, str):
                try:
                    resulta = json.loads(resulta)
                except Exception:
                    resulta = []


            rc = datos.get("rc", 0)
     
            return {"resp": resulta, "rc": rc,}

        except Exception as e:
            logger.error(f"Error en recsaldos: {e}")
            return {"resp": [],  "rc": 0}
        

    def recmovagre(self, datos , user, db):

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

        if estado:
            dataJSON = json.loads(decriptMsg)
            tknper = dataJSON.get("tknper")
            alias = dataJSON.get("alias")
            entidad = dataJSON.get("entidad")

        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400
        if estado:
            if entidad == '9999':                         
                movtos = db.query(DBMOVTOS).where( DBMOVTOS.tkncli==tknper).all()

                if movtos:
                    for selmovto in movtos:

                        result.append({ 
                            'id':selmovto.id,
                            'entidad':selmovto.entidad, 
                            'tkncli':selmovto.tkncli,
                            'alias':selmovto.alias,
                            'signo':selmovto.signo,
                            'importe':selmovto.importe,
                            'concepto':selmovto.concepto,
                            'fecha_movto':selmovto.fecha_movto.strftime("%Y-%m-%d")                                                                                                   
                                })
 

        
                movimientos_list = {
                    "movimientos": result
                }
                rc = 201 
                message = json.dumps(movimientos_list) 
                app_state.jsonenv = message
                encripresult , estado = encrypt_message(password, message)
                result = encripresult

     
            else:
                
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
                            datosin=tknper,
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
                        app_state.jsonenv = message

                        encripresult , estado = encrypt_message(password, message)
                        result = encripresult
                        rc = 201   

                    else:
                        result.append({'response': 'No existen datos a listar con los criterios proporcionados.'})
                        rc = 400
                

                except Exception as e:
                    rc = 400
                    logger.error(f"Error en recmovagre: {e}")
                    return {"resp": [],  "rc": 400}        
        return result, rc 