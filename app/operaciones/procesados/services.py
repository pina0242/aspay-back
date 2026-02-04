from app.core.models import MasivoBatch, MasivoItem, MasivoIngestSummary, MasivoIngestLog
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.entidad.services import EntService
from app.core.config import settings
from app.core.state import app_state
from sqlalchemy import func , and_
import logging
import json 
import os, time, json
logger = logging.getLogger(__name__)




class OperServiceProcesados:

    def __init__(self, db_session=None):
        self.db = db_session

    def listproc(self,datos,db):
        result = []
        rc = 400
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
            print(dataJSON)
            param_keys = ['entidad','fecha_ini','fecha_fin']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                message = json.dumps(result)
                app_state.jsonenv = message 
                rc = 400
            else:
                entidad= dataJSON.get("entidad")
                fecha_ini= dataJSON.get("fecha_ini")
                fecha_fin = dataJSON.get("fecha_fin")
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
        print('entre psc')
        if estado:
            try:
                archivos = db.query(MasivoIngestSummary).order_by(MasivoIngestSummary.id.desc()).where(and_(
                                    func.date(MasivoIngestSummary.finished_at).between(fecha_ini, fecha_fin),
                                    MasivoIngestSummary.entidad == entidad,
                                    MasivoIngestSummary.status == 'Procesado'
                                )).all()

                if archivos:
                    for selarch in archivos:
                        result.append({ 
                            'id':selarch.id,
                            'entidad':selarch.entidad,
                            'filename':selarch.filename, 
                            'total':selarch.total,
                            'invalid':selarch.invalid,
                            'duplicates_in_file':selarch.duplicates_in_file,
                            'inserted':selarch.inserted,
                            'skipped_existing':selarch.skipped_existing                                                                                                     
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
                                        
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Oooopppss'})  
                message = json.dumps(result)
                app_state.jsonenv = message    
        return result, rc

    def listpdet(self,datos,db):
        result = []
        rc = 400
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
            print(dataJSON)
            param_keys = ['id','entidad','filename']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                message = json.dumps(result)
                app_state.jsonenv = message 
                rc = 400
            else:
                id = dataJSON.get("id")
                entidad = dataJSON.get("entidad")
                filename = dataJSON.get("filename")
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
                'id': ('num', 1),
                'entidad': ('long', 8), 
                'filename': ('long', 255)
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
            try:

                q_logs = (db.query(MasivoIngestLog.batch_id)
                  .where(MasivoIngestLog.entidad == entidad,
                         MasivoIngestLog.ingest_id == id,
                         MasivoIngestLog.filename == filename,
                         MasivoIngestLog.batch_id.isnot(None))
                  .distinct())
                batch_ids = [r[0] for r in q_logs.all()]

                if not batch_ids:
                    result.append({'response': 'No hay registros asociados al archivo.'})
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    return result, 400
                
                # 3) Obtener clientes involucrados en esos batch_ids con items PENDING
                detallep = db.query(MasivoItem).order_by(MasivoItem.id.desc()).where(
                                MasivoItem.entidad == entidad,
                                MasivoItem.batch_id.in_(batch_ids),
                                MasivoItem.filename == filename
                            ).all()

                if detallep:
                    for seldetap in detallep:
                        result.append({ 
                            'cliente':seldetap.cliente,
                            'aliasori':seldetap.aliasori,
                            'aliasdes':seldetap.aliasdes,
                            'concepto':seldetap.concepto,
                            'importe':seldetap.importe,
                            'fecha_ejec':str(seldetap.fecha_ejec)[0:10],
                            'status':seldetap.status,
                            'error':seldetap.error
                                })
                    rc = 201 
                    message = json.dumps(result) 
                    app_state.jsonenv = message
                    encripresult , estado = encrypt_message(password, message)
                    result = encripresult                                
                                        
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Oooopppss'}) 
                message = json.dumps(result)
                app_state.jsonenv = message     
        return result, rc



