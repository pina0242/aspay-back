
from app.core.models import DBDGENPERS, DBCTAPERS, DBTCORP
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, valtipdat, obtener_fecha_actual, busca_moneda_local, busca_tipo_cambio, obten_desc_indoper
from app.core.config import settings
from app.core.state import app_state
from app.core.auth import create_access_token
from app.core.security import hash_password, verify_password
from sqlalchemy import  func
from datetime import timedelta, datetime, date
import logging
import json 
logger = logging.getLogger(__name__)

class CtaService:
    
    def __init__(self, db_session=None):
        self.db = db_session
    
    def regcta(self, datos , user, db):
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

        if estado:
            dataJSON = json.loads(decriptMsg)
            param_keys = ['tknper','pais','moneda','entban','tipo', 'alias', 'datos','indoper','categoria']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                rc = 400
            else:
                tknper  = dataJSON["tknper"]
                pais    = dataJSON["pais"]
                moneda  = dataJSON["moneda"]
                entban  = dataJSON["entban"]
                tipoc   = dataJSON["tipo"]
                alias   = dataJSON["alias"]
                datos_ctas_raw = dataJSON["datos"] # Raw data from decrypted payload
                indoper   = dataJSON["indoper"]
                categoria = dataJSON["categoria"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            validations = {
                'tknper': ('long', 36),
                'pais': ('long', 2),
                'moneda': ('long', 3),
                'entban': ('long', 4),
                'tipo': ('long', 3),
                'alias': ('long', 10),
                'indoper': ('long', 2),
                'categoria':('long',20)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
            # if not valcampo('long', tknper, 36):
            #     result.append({'response': 'tknper invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', pais, 2):
            #     result.append({'response': 'pais invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', moneda, 3):
            #     result.append({'response': 'moneda invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', entidad, 4):
            #     result.append({'response': 'entidad invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', tipo, 3):
            #     result.append({'response': 'tipo invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', alias, 10):
            #     result.append({'response': 'alias invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', datos_ctas_raw, 5000): # Assuming max length for Text field for the raw data
            #     result.append({'response': 'datos invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            valtd, enmascarv =  valtipdat(tipoc,datos_ctas_raw)
            if not valtd:
                result.append({'response': 'tipo de cuenta invalido.'})
                estado = False
                rc = 400
                return result, rc
        if estado:
            if pais=='ES':
                valent = db.query(DBTCORP).where(DBTCORP.llave=='ENTBAN',DBTCORP.clave==entban,DBTCORP.status=='A').first() 
                if not valent:
                    result.append({'response': 'entban no existe en catálogo'})
                    estado = False
                    rc = 400
                    return result, rc
                 
                if tipoc == '001':
                    # Extraemos los caracteres de la posición 5 a la 8
                    iban_clean = datos_ctas_raw.replace(' ', '').replace('\t', '').upper()
                    banco_en_iban = iban_clean[4:8]
            
                    if banco_en_iban != entban:
                        result.append({'response': 'El código de banco en el IBAN no coincide con entban informado'})
                        estado = False
                        rc = 400
                        return result, rc
                                        
        if estado:                         
            per  = db.query(DBDGENPERS).order_by(DBDGENPERS.id.desc()).where(DBDGENPERS.tknper==tknper, DBDGENPERS.estatus=='A').first() 
            if per:
                tknper  = per.tknper
                entidad = per.entidad
            else:
                result.append({'response':'No existe persona'}) 
                estado = False
                rc = 400
                return result , rc  
            
        if estado:
            # valalias=db.query(DBCTAPERS).where(DBCTAPERS.tknper==tknper,DBCTAPERS.alias==alias,DBCTAPERS.indoper==indoper,DBCTAPERS.estatus=='A').first() 
            # if valalias:
            #     result.append({'response': 'Ya existe alias para el cliente.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # Buscar TODOS los registros con el mismo token, estatus 'A', mismo indoper y mismo tipo
            cuentas_existentes = db.query(DBCTAPERS).where(
                DBCTAPERS.tknper == tknper,
                DBCTAPERS.indoper == indoper,
                DBCTAPERS.tipo == tipoc,
                DBCTAPERS.estatus == 'A'
            ).all()
        
            if cuentas_existentes:
                cuenta_duplicada = False
                #cuenta_existente_info = None
                
                # Para cada registro encontrado, desencriptar y comparar con la cuenta informada
                for cuenta in cuentas_existentes:
                    try:
                        # Desencriptar el campo datos de la cuenta existente
                        datos_desencriptados, desenc_estado = decrypt_message(password, cuenta.datos)
                        
                        if desenc_estado:
                            # Normalizar ambas cuentas para comparación (eliminar espacios, tabs, convertir a mayúsculas)
                            cuenta_existente_limpia = datos_desencriptados.replace(' ', '').replace('\t', '').upper()
                            cuenta_nueva_limpia = datos_ctas_raw.replace(' ', '').replace('\t', '').upper()
                            
                            # Comparar las cuentas
                            if cuenta_existente_limpia == cuenta_nueva_limpia:
                                cuenta_duplicada = True
                                # cuenta_existente_info = {
                                #     'alias': cuenta.alias,
                                #     'entban': cuenta.entban,
                                #     'cuenta': datos_desencriptados
                                # }
                                break  # Salir del bucle si encontramos un duplicado
                    
                    except Exception as e:
                        print(f'Error al desencriptar cuenta existente: {e}')
                        # Continuar con la siguiente cuenta si hay error en una
                
                if cuenta_duplicada:
                    mensaje_error = f'La cuenta ya existe para este cliente.'
                    # if cuenta_existente_info:
                    #     # Solo mostrar alias si la cuenta está duplicada
                    #     mensaje_error += f' Alias existente: {cuenta_existente_info["alias"]}'
                    
                    result.append({'response': mensaje_error})
                    estado = False
                    rc = 400
                    return result, rc
        #No debe haber más de una cuenta operativa
        # if estado:
        #     if indoper == 'CO':
        #         valindoper = db.query(DBCTAPERS).where(
        #                             DBCTAPERS.tknper == tknper,
        #                             #DBCTAPERS.alias == alias,
        #                             DBCTAPERS.indoper == 'CO',
        #                             DBCTAPERS.estatus == 'A'
        #                             ).first()
            
        #         if valindoper:
        #             result.append({'response': 'Ya existe otra cuenta operativa para este cliente.'})
        #             estado = False
        #             rc = 400
        #             return result, rc
                
        if estado:
            if moneda != 'EUR':
                moneda_pais = busca_moneda_local(pais,db)
                print('moneda pais, moneda entrada: ',moneda_pais,moneda)

                if moneda_pais != moneda:
                    result.append({'response': 'Moneda incorrecta'})
                    estado = False
                    rc = 400
                    return result, rc

        if estado:
            # Encriptar el campo 'datos' antes de almacenar en la DB
            datos_ctas_encrypted, encrypt_estado = encrypt_message(password, datos_ctas_raw)
            if not encrypt_estado:
                result.append({'response': 'Error al encriptar el campo datos.'})
                rc = 500 # Internal Server Error
                return result, rc

            #with self.session_scope() as session:
            # fecha_actual = obtener_fecha_actual()
            

            usuario_alta =  user
            usuario_mod  = ' '
            fecha_mod    = datetime.min
            indoper      = indoper
            estatus      = 'A'
            enmascar     = enmascarv
            tipo         = tipoc
            categoria    = categoria

            try:
                regCta = DBCTAPERS(
                    entidad = entidad,
                    tknper=tknper,
                    pais=pais,
                    moneda=moneda,
                    entban=entban,
                    tipo=tipo,
                    alias=alias,
                    datos=datos_ctas_encrypted, # Almacenar datos encriptados
                    indoper = indoper,
                    estatus=estatus,
                    enmascar=enmascar,
                    categoria=categoria,
                    usuario_alta=usuario_alta,
                    fecha_mod=fecha_mod,
                    usuario_mod=usuario_mod
                )
                db.add(regCta)
                db.commit()
                result.append({'response': 'Exito', 'id': regCta.id})
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Error BD'})               
        return result, rc
    
    def selctas(self, datos , user, db):
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
            tknper = dataJSON.get("tknper")

            if not tknper:
                estado = False
                result.append({'response': 'Debe proporcionar tknper.'})
                rc = 400
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            # with self.session_scope() as session:
                                                
            ctas_entries = db.query(DBCTAPERS).order_by(DBCTAPERS.id.desc()).where(DBCTAPERS.tknper == tknper,DBCTAPERS.estatus=='A').all() 

            if ctas_entries:
                for entry in ctas_entries:
                    # Desencriptar el campo 'datos' después de recuperarlo de la DB
                    datos_decrypted, decrypt_estado = decrypt_message(password, entry.datos)
                    if not decrypt_estado:
                        datos_decrypted = "Error al desencriptar datos" # Manejar error de desencriptación
                    print('entry moneda:', entry.moneda)
                    tipo_cambio = 0
                    if entry.moneda != 'EUR':
                        print('voy a tipo cambio por: ', entry.moneda)
                        tipo_cambio = busca_tipo_cambio(entry.moneda,db)

                    desc_indoper = obten_desc_indoper (entry.indoper,db)
                    fecha_alta_str = entry.fecha_alta.isoformat() if isinstance(entry.fecha_alta, (date, datetime)) else str(entry.fecha_alta)
                    fecha_mod_str = entry.fecha_mod.isoformat() if isinstance(entry.fecha_mod, (date, datetime)) else str(entry.fecha_mod)
                    
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
                        'indoper': entry.indoper + ' - ' + desc_indoper,
                        #'estatus': entry.estatus,
                        #'enmascar':entry.enmascar,
                        'categoria':entry.categoria,
                        'fecha_alta': fecha_alta_str,
                        'usuario_alta': entry.usuario_alta,
                        'fecha_mod': fecha_mod_str,
                        'usuario_mod': entry.usuario_mod
                        })
                rc = 201
                message = json.dumps(result)
                app_state.jsonenv = message
                encripresult, estado = encrypt_message(password, message)
                result = encripresult
            else:
                result.append({'response': 'No existen datos a listar con los criterios proporcionados.'})
                rc = 400
        return result, rc 
    
    def updcta(self, datos , user, db):
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
            param_keys = ['id','tknper','pais','moneda','entban','tipo','alias','datos','indoper','categoria']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                rc = 400
            else:
                id             = dataJSON["id"]
                tknper         = dataJSON["tknper"]
                pais           = dataJSON["pais"]
                moneda         = dataJSON["moneda"]
                entban         = dataJSON["entban"]
                tipoc          = dataJSON["tipo"]
                alias          = dataJSON["alias"]
                datos_ctas_raw = dataJSON["datos"] # Raw data from decrypted payload
                indoper        = dataJSON["indoper"]
                categoria      = dataJSON["categoria"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            validations = {
                'id': ('float', 10),
                'tknper': ('long', 36),
                'pais': ('long', 2),
                'moneda': ('long', 3),
                'entban': ('long', 4),
                'tipo': ('long', 3),
                'alias': ('long', 10),
                'indoper': ('long', 2),
                'categoria':('long',20)
            } 
            for key, (tipo, limite) in validations.items():
                    if not valcampo(tipo, dataJSON[key], limite):
                        response_key = key
                        result = [{'response': response_key}]
                        rc = 400
                        message = json.dumps(result)
                        app_state.jsonenv = message
                        return result, rc
                    
            # if not valcampo('float',id,10):
            #     result.append({'response':'id'})
            #     estado = False
            #     rc = 400
            #     print('result :', result)
            #     return result , rc
            # if not valcampo('long', tknper, 36):
            #     result.append({'response': 'tknper invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', pais, 2):
            #     result.append({'response': 'pais invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', moneda, 3):
            #     result.append({'response': 'moneda invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', entidad, 4):
            #     result.append({'response': 'entidad invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', tipo, 3):
            #     result.append({'response': 'tipo invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', alias, 10):
            #     result.append({'response': 'alias invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            # if not valcampo('long', datos_ctas_raw, 5000):
            #     result.append({'response': 'datos invalido.'})
            #     estado = False
            #     rc = 400
            #     return result, rc
            valtd, enmascarv =  valtipdat(tipoc,datos_ctas_raw)
            if not valtd:
                result.append({'response': 'tipo de cuenta invalido.'})
                estado = False
                rc = 400
                return result, rc
        if estado:
            if pais=='ES':
                valent = db.query(DBTCORP).where(DBTCORP.llave=='ENTBAN',DBTCORP.clave==entban,DBTCORP.status=='A').first() 
                if not valent:
                    result.append({'response': 'entidad no existe en catálogo'})
                    estado = False
                    rc = 400
                    return result, rc
                
                if tipoc == '001':
                    # Extraemos los caracteres de la posición 5 a la 8
                    iban_clean = datos_ctas_raw.replace(' ', '').replace('\t', '').upper()
                    banco_en_iban = iban_clean[4:8]
            
                    if banco_en_iban != entban:
                        result.append({'response': 'El código de banco en el IBAN no coincide con entban informado'})
                        estado = False
                        rc = 400
                        return result, rc
                    
        if estado:
            # Buscar TODOS los registros con el mismo token, estatus 'A', mismo indoper y mismo tipo
            cuentas_existentes = db.query(DBCTAPERS).where(
                                            DBCTAPERS.tknper == tknper,
                                            DBCTAPERS.indoper == indoper,
                                            DBCTAPERS.tipo == tipoc,
                                            DBCTAPERS.estatus == 'A',
                                            DBCTAPERS.id != id  # Excluir el registro actual que se está actualizando
                                             ).all() 
            
            if cuentas_existentes:
                cuenta_duplicada = False
                #cuenta_existente_info = None
                
                # Para cada registro encontrado, desencriptar y comparar con la cuenta informada
                for cuenta in cuentas_existentes:
                    try:
                        # Desencriptar el campo datos de la cuenta existente
                        datos_desencriptados, desenc_estado = decrypt_message(password, cuenta.datos)
                        
                        if desenc_estado:
                            # Normalizar ambas cuentas para comparación (eliminar espacios, tabs, convertir a mayúsculas)
                            cuenta_existente_limpia = datos_desencriptados.replace(' ', '').replace('\t', '').upper()
                            cuenta_nueva_limpia = datos_ctas_raw.replace(' ', '').replace('\t', '').upper()
                            
                            # Comparar las cuentas
                            if cuenta_existente_limpia == cuenta_nueva_limpia:
                                cuenta_duplicada = True
                                # cuenta_existente_info = {
                                #     'id': cuenta.id,
                                #     'alias': cuenta.alias,
                                #     'entban': cuenta.entban,
                                #     'cuenta': datos_desencriptados
                                # }
                                break  # Salir del bucle si encontramos un duplicado
                    
                    except Exception as e:
                        print(f'Error al desencriptar cuenta existente: {e}')
                        # Continuar con la siguiente cuenta si hay error en una
                
                if cuenta_duplicada:
                    mensaje_error = f'La cuenta ya existe para este cliente.'
                    # if cuenta_existente_info:
                    #     # Solo mostrar alias si la cuenta está duplicada
                    #     mensaje_error += f' Alias existente: {cuenta_existente_info["alias"]}'
                    
                    result.append({'response': mensaje_error})
                    estado = False
                    rc = 400
                    return result, rc
            
        # if estado:
        #     if indoper == 'CO':
        #         valindoper = db.query(DBCTAPERS).where(
        #                             DBCTAPERS.tknper == tknper,
        #                             #DBCTAPERS.alias == alias,
        #                             DBCTAPERS.indoper == 'CO',
        #                             DBCTAPERS.estatus == 'A',
        #                             DBCTAPERS.id != id  # Excluir el registro actual
        #                             ).first()
            
        #         if valindoper:
        #             result.append({'response': 'Ya existe otra cuenta operativa para este cliente.'})
        #             estado = False
        #             rc = 400
        #             return result, rc
                
        if estado:
            if moneda != 'EUR':
                moneda_pais = busca_moneda_local(pais,db)

                if moneda_pais != moneda:
                    result.append({'response': 'Moneda incorrecta'})
                    estado = False
                    rc = 400
                    return result, rc

        if estado:
            # Encriptar el campo 'datos' antes de actualizar en la DB
            datos_ctas_encrypted, encrypt_estado = encrypt_message(password, datos_ctas_raw)
            if not encrypt_estado:
                result.append({'response': 'Error al encriptar el campo datos.'})
                rc = 500
                return result, rc

            # with self.session_scope() as session:
            ctas_ent = db.query(DBCTAPERS).where(DBCTAPERS.id==id,DBCTAPERS.estatus=='A').first() 
            if ctas_ent:
                try:
                # 1. Crear copia histórica (estatus 'M')
                    campos_originales = {}
                    for col in ctas_ent.__table__.columns:
                        if col.name not in ['id', 'fecha_alta']:
                            campos_originales[col.name] = getattr(ctas_ent, col.name)
                
                    campos_originales['estatus'] = 'M'  # eStatus histórico
                    campos_originales['usuario_alta'] = user  # Usuario que crea el histórico

                    print('campos_originales :', campos_originales)
                
                    cta_historico = DBCTAPERS(**campos_originales)
                    db.add(cta_historico)

                # 2. ACTUALIZAR registro original (mantener estatus 'A')
                    ctas_ent.pais = pais
                    ctas_ent.moneda = moneda
                    ctas_ent.entban = entban
                    ctas_ent.tipo = tipoc
                    ctas_ent.alias = alias
                    ctas_ent.datos = datos_ctas_encrypted # Actualizar con datos encriptados
                    ctas_ent.indoper = indoper
                    ctas_ent.enmascar= enmascarv
                    ctas_ent.categoria= categoria
                    ctas_ent.fecha_mod = obtener_fecha_actual()
                    ctas_ent.usuario_mod = user
                    db.add(ctas_ent)
                    db.commit()
                    result.append({'response': 'Exito'})
                    rc = 201
                    message = json.dumps(result)
                    app_state.jsonenv = message
                    encripresult, estado = encrypt_message(password, message)
                    result = encripresult
                except Exception as e:
                    print ('e',e)
                    result.append({'response':'Error DB'}) 
                    estado = False
                    rc = 400   
            else:
                result.append({'response': 'registro inexistente'})
                estado = False
                rc = 400
        return result, rc   
    
    def delcta(self, datos , user, db):
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
            param_keys = ['id']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                rc = 400
            else:
                id = dataJSON["id"]
        else:
            estado = False
            result = 'informacion incorrecta'
            rc = 400

        if estado:
            if not valcampo('float',id,1):
                result.append({'response':'id'})
                estado = False
                rc = 400
                print('result :', result)
                return result , rc

        if estado:
            # with self.session_scope() as session:
            ctas_ent = db.query(DBCTAPERS).where(DBCTAPERS.id==id).first() 
            if  ctas_ent:
                ctas_ent.estatus = 'B'
                ctas_ent.fecha_mod = obtener_fecha_actual()
                ctas_ent.usuario_mod = user
                db.add(ctas_ent)
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
        return result, rc
    