#from fastapi import Depends
from app.core.models import MasivoBatch, MasivoItem, MasivoIngestSummary, MasivoIngestLog, DBOPERDIA, DBSALDOS, DBMOVTOS, DBCTAPERS
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from app.core.database import get_db, SessionLocal #, SessionManager
from app.core.interfaz import login_al_banco, consulta_saldo, registra_movto
from app.entidad.services import EntService
from sqlalchemy import func, and_, case
from sqlalchemy.orm import Session, sessionmaker
from contextlib import contextmanager
import logging
import json 
import os, time, json
import concurrent.futures
logger = logging.getLogger(__name__)
FINAL_DIR   = os.environ.get("MASIVO_OK_DIR",   "./data/final")
BATCH_ACTIVE = 'ACTIVE'
BATCH_PAUSED = 'PAUSED'
OK_DIR   = os.environ.get("MASIVO_OK_DIR",   "./data/archive")
STATUS_PENDING    = 'PENDIENTE'
STATUS_SUCCESS    = 'EXITO'
STATUS_FAILED     = 'FALLADO'
STATUS_CANCELLED  = 'CANCELLED'
STATUS_RETURNED   = 'RETURNED'
STATUS_VOIDED     = 'VOIDED'
STATUS_PROCESSING = 'PROCESSING'
REQUIRED_HEADERS = [
    'accion','cliente','tkncliori','aliasori','tipoori','ordenante',
    'tknclides','aliasdes','tipodes','beneficiario',
    'concepto','importe','fecha_ejecucion'
]
MAX_ERROR_LEN = int(os.environ.get("MASIVO_ERROR_MAXLEN", "0"))
MASIVO_IN_DIR = os.environ.get('MASIVO_IN_DIR', './data/incoming')
UPLOAD_CHUNK = int(os.environ.get('UPLOAD_CHUNK', 1024 * 1024))  # 1MB por chunk
os.makedirs(MASIVO_IN_DIR, exist_ok=True)
_MASIVO_CHUNK_SIZE = int(os.environ.get("MASIVO_CHUNK_SIZE", "25"))
chunk_size=_MASIVO_CHUNK_SIZE
MASIVO_WORKERS_PER_CLIENT = 6
MASIVO_CLIENTS_PARALLEL  = 6
MASIVO_PARALLEL_PER_CHUNK = int(os.environ.get("MASIVO_PARALLEL_PER_CHUNK", "6"))



class OperServicePendientes:

    def __init__(self, db_session: Session = None, session_factory: sessionmaker = None):
        # Session del request (NO se usa en hilos)
        self.db = db_session
        # Fábrica de sesiones a usar en hilos y en ciclos internos
        self.session_factory = session_factory or SessionLocal

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

    def listsing(self,datos,db):
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
                entidad = dataJSON.get("entidad")
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
            validations = {'entidad': ('long', 8), 
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
            
        if estado:
            try:
                archivos = db.query(MasivoIngestSummary).order_by(MasivoIngestSummary.id.desc()).where(and_(
                                    func.date(MasivoIngestSummary.finished_at).between(fecha_ini, fecha_fin),
                                    MasivoIngestSummary.entidad==entidad, MasivoIngestSummary.status == 'Pendiente'
                                )).all()

                if archivos:
                    for selarch in archivos:
                        q_logs = (db.query(MasivoIngestLog.batch_id)
                                  .where(MasivoIngestLog.ingest_id == selarch.id,  
                                  MasivoIngestLog.entidad == entidad,      
                                  MasivoIngestLog.filename == selarch.filename,
                                  MasivoIngestLog.batch_id.isnot(None))
                                  .distinct())
                        batch_ids = [r[0] for r in q_logs.all()]

                        if not batch_ids:
                            result.append({ 
                                'id':selarch.id,
                                'filename':selarch.filename, 
                                'total':selarch.total,
                                'invalid':selarch.invalid,
                                'duplicates_in_file':selarch.duplicates_in_file,
                                'inserted':selarch.inserted,
                                'pending': 0,
                                'skipped_existing':selarch.skipped_existing                                                                                                     
                                    })
                            continue
                

                        q_pendiente = db.query(func.count(MasivoItem.id)).where(
                            MasivoItem.batch_id.in_(batch_ids),
                            MasivoItem.entidad == entidad,
                            MasivoItem.filename == selarch.filename,
                            MasivoItem.status == STATUS_PENDING
                            ).scalar()



                        result.append({ 
                            'id':selarch.id,
                            'entidad':selarch.entidad,
                            'filename':selarch.filename, 
                            'total':selarch.total,
                            'invalid':selarch.invalid,
                            'duplicates_in_file':selarch.duplicates_in_file,
                            'inserted':selarch.inserted,
                            'pending': q_pendiente,
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

    def listsdet(self,datos,db):
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
                detalle = db.query(MasivoIngestLog).order_by(MasivoIngestLog.id.desc()).where(
                    MasivoIngestLog.entidad==entidad, MasivoIngestLog.ingest_id==id,MasivoIngestLog.filename==filename).all() 

                if detalle:
                    for seldeta in detalle:
                        result.append({ 
                            'cliente':seldeta.cliente,
                            'line_no':seldeta.line_no, 
                            'code':seldeta.code,
                            'message':seldeta.message                                                                                                     
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

    def delsing(self,datos,db):
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
            param_keys = ['ingest_id','entidad','filename']
            if not all(key in dataJSON for key in param_keys):
                estado = False
                result.append({'response': 'Algun elemento de la transacción esta faltando.'})
                message = json.dumps(result)
                app_state.jsonenv = message 
                rc = 400
            else:
                id = dataJSON.get("ingest_id")
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
                'ingest_id': ('num', 1),  
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
                # 3) Localiza el summary y valida estado 'Pendiente'
                summary = (
                    db.query(MasivoIngestSummary)
                    .where(MasivoIngestSummary.entidad == entidad,
                        MasivoIngestSummary.id == id,
                        MasivoIngestSummary.filename == filename)
                    .first()
                )
                # 4) Determinar LOS batch_ids ÚNICAMENTE de esta ingesta
                q_logs = (db.query(MasivoIngestLog.batch_id)
                        .where(MasivoIngestLog.ingest_id == id,
                                MasivoIngestLog.entidad == entidad,
                                MasivoIngestLog.batch_id.isnot(None),
                                MasivoIngestLog.filename == filename)
                        .distinct())
                batch_ids = [r[0] for r in q_logs.all()]

                # 5) Pausar SOLO esos batches (evita colisiones con workers)
                if batch_ids:
                    db.query(MasivoBatch)\
                        .where(MasivoBatch.entidad==entidad,MasivoBatch.id.in_(batch_ids),)\
                        .update({MasivoBatch.status: "PAUSED"}, synchronize_session=False)

                # 6) Verificaciones de colisión SOLO sobre esos batches
                processing_cnt = 0
                non_pending_cnt = 0
                if batch_ids:
                    processing_cnt = (db.query(func.count(MasivoItem.id))
                                    .where(MasivoItem.entidad==entidad,MasivoItem.batch_id.in_(batch_ids),
                                           MasivoItem.filename == filename,
                                            MasivoItem.status == "PROCESSING")
                                    .scalar() or 0)
                    non_pending_cnt = (db.query(func.count(MasivoItem.id))
                                    .where(MasivoItem.entidad==entidad,MasivoItem.batch_id.in_(batch_ids),
                                           MasivoItem.filename == filename,
                                            MasivoItem.status != "PENDIENTE")
                                    .scalar() or 0)

                if processing_cnt > 0:
                    result.append({'response': 'Conflicto: hay items en PROCESSING. Intenta más tarde.'})
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    rc = 400
                    message = json.dumps(result, ensure_ascii=False)
                    app_state.jsonenv = message
                    encripresult, _ = encrypt_message(password, message)
                    return encripresult, rc

                if non_pending_cnt > 0:
                    result.append({'response': 'Conflicto: existen items ya procesados/alterados (no PENDING).'})
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    rc = 400
                    message = json.dumps(result, ensure_ascii=False)
                    app_state.jsonenv = message
                    encripresult, _ = encrypt_message(password, message)
                    return encripresult, rc

                # 7) Borrados en orden seguro (acotados a ESTA ingesta)
                deleted = {"items": 0, "logs": 0, "batches": 0, "summary": 0}

                # 7.1) Items
                if batch_ids:
                    deleted["items"] = (db.query(MasivoItem)
                                        .where(MasivoItem.entidad==entidad,MasivoItem.batch_id.in_(batch_ids),
                                               MasivoItem.filename == filename)
                                        .delete(synchronize_session=False))

                # 7.2) Logs (primero por ingest_id; adicionalmente por batch_id por robustez)
                deleted["logs"] += (db.query(MasivoIngestLog)
                                    .where(MasivoIngestLog.entidad==entidad,MasivoIngestLog.ingest_id == id,
                                            MasivoIngestLog.filename == filename)
                                    .delete(synchronize_session=False))
                if batch_ids:
                    deleted["logs"] += (db.query(MasivoIngestLog)
                                        .where(MasivoIngestLog.entidad==entidad,MasivoIngestLog.batch_id.in_(batch_ids))
                                        .delete(synchronize_session=False))

                # 7.3) Batches
                if batch_ids:
                    deleted["batches"] = (db.query(MasivoBatch)
                                        .where(MasivoBatch.entidad==entidad,MasivoBatch.id.in_(batch_ids))
                                        .delete(synchronize_session=False))

                # 7.4) Summary
                db.delete(summary)
                deleted["summary"] = 1
                db.commit()
                # 8) Respuesta OK (encriptada)
                rc = 201
                result.append({'response': 'Exito', 'archivo': filename,'deleted': deleted})
                message = json.dumps(result, ensure_ascii=False)
                app_state.jsonenv = message
                encripresult, _ = encrypt_message(password, message)
                return encripresult, rc                                               
            except Exception as e:
                print ('Error :', e)
                rc = 400  
                result.append({'response':'Oooopppss'})    
                message = json.dumps(result)
                app_state.jsonenv = message  
        return result, rc

#ejectarch
    def _update_batch_stats(self, db, entidad1, batch_id: int):
        """Recalcula contadores processed/success/failed del batch."""
        entidad1=entidad1
        proc = db.query(func.count(MasivoItem.id)).filter(
            MasivoItem.entidad == entidad1,
            MasivoItem.batch_id == batch_id, MasivoItem.status != "PENDIENTE"
        ).scalar() or 0
        ok = db.query(func.count(MasivoItem.id)).filter(
            MasivoItem.entidad == entidad1,
            MasivoItem.batch_id == batch_id, MasivoItem.status == "EXITO"
        ).scalar() or 0
        nok = db.query(func.count(MasivoItem.id)).filter(
            MasivoItem.entidad == entidad1,
            MasivoItem.batch_id == batch_id, MasivoItem.status == "FALLADO"
        ).scalar() or 0
        db.query(MasivoBatch).filter(MasivoBatch.entidad == entidad1,MasivoBatch.id == batch_id).update({
            MasivoBatch.processed: int(proc),
            MasivoBatch.successful: int(ok),
            MasivoBatch.failed: int(nok)
        }, synchronize_session=False)

    def _claim_pending_chunk(self, db, entidad, cliente: str, filename: str, limit: int, batch_ids):        
        COBRO_ACTIONS = ('2','4','5')
        q = (
            db.query(MasivoItem)
            .join(MasivoBatch, MasivoBatch.id == MasivoItem.batch_id)
            .filter(
                MasivoItem.entidad == entidad,
                MasivoItem.cliente == cliente,
                MasivoItem.filename == filename,
                MasivoItem.status == STATUS_PENDING,
                MasivoItem.batch_id.in_(batch_ids),
                func.coalesce(MasivoBatch.status, BATCH_ACTIVE) == BATCH_ACTIVE
            )
            .order_by(
                case(
                    (MasivoItem.accion.in_(COBRO_ACTIONS), 0),
                    else_=1
                ).asc(),
                MasivoItem.importe.asc().nullslast(),
                MasivoItem.id.asc()
            )
            .with_for_update(skip_locked=True)
        )

        items = q.limit(int(limit)).all()
        if not items:
            return []

        now = obtener_fecha_actual()
        for it in items:
            it.status = STATUS_PROCESSING
            it.updated_at = now

        db.flush()
        return items
    
    # ---- Worker por cliente ----
    def _process_cliente(self, db, entidad, cliente: str, filename: str, 
                        chunk_size: int, batch_ids, ftkncli, dt, ftipo, falias):
        import concurrent.futures, threading
        results = {"cliente": cliente, "ok": 0, "nok": 0, "batches_touched": set()}

        def _to_int_accion(v):
            try: return int(str(v).strip())
            except: return 4

        def _worker_loop():
            while True:
                with self.session_scope_ctx() as s_claim:
                    items = self._claim_pending_chunk(s_claim, entidad, cliente, filename, chunk_size, batch_ids)
                    if not items:
                        return
                    work = []
                    entidad1=entidad
                    for it in items:
                        work.append({
                            "id": int(it.id), "batch_id": int(it.batch_id),
                            "importe": float(it.importe or 0.0),
                            "accion": _to_int_accion(it.accion),
                            "tkncliori": it.tkncliori, "aliasori": it.aliasori, "tipoori": it.tipoori,
                            "tknclides": it.tknclides, "aliasdes": it.aliasdes, "tipodes": it.tipodes,
                            "beneficiario": it.beneficiario,"ordenante": it.ordenante,
                            "concepto": it.concepto, "fecha_ejec": str(it.fecha_ejec)[:10],
                            })
            
                def _do_one(entidad,row):
                    
                    try:
                        with self.session_scope_ctx() as s_tx:
                            acc = row["accion"]
                            nombre_benef = row["beneficiario"]
                            nombre_mandate = row["ordenante"]
                            fecha_ejec = row["fecha_ejec"]
                            importe = row["importe"]
                            concepto = row["concepto"]
                            entidad = entidad

                            if acc == 2:
                                self._apply_saldo_and_movto_atomic(
                                    s_tx, entidad, ftkncli=ftkncli, dt=dt, ftipo=ftipo, falias=falias,
                                    concepto=concepto, importe=importe, toper="COBRO"
                                )
                                self._add_operdia_atomic(
                                    s_tx, entidad, tipo_oper="COBRO", fecha_ejec=fecha_ejec, importe=importe,
                                    concepto=concepto, nombre_benef=nombre_benef, nombre_mandate=nombre_mandate
                                )
                                return {"id": row["id"], "ok": True}

                            if acc == 3:
                                self._apply_saldo_and_movto_atomic(
                                    s_tx, entidad, ftkncli=ftkncli, dt=dt, ftipo=ftipo, falias=falias,
                                    concepto=concepto, importe=importe, toper="ABONO"
                                )
                                self._add_operdia_atomic(
                                    s_tx, entidad, tipo_oper="ABONO", fecha_ejec=fecha_ejec, importe=importe,
                                    concepto=concepto, nombre_benef=nombre_benef, nombre_mandate=nombre_mandate
                                )
                                return {"id": row["id"], "ok": True}

                            if acc in (4, 5):
                                self._apply_saldo_and_movto_atomic(
                                    s_tx, entidad, ftkncli=ftkncli, dt=dt, ftipo=ftipo, falias=falias,
                                    concepto=concepto, importe=importe, toper="COBRO"
                                )
                                self._apply_saldo_and_movto_atomic(
                                    s_tx, entidad, ftkncli=ftkncli, dt=dt, ftipo=ftipo, falias=falias,
                                    concepto=concepto, importe=importe, toper="ABONO"
                                )
                                self._add_operdia_atomic(
                                    s_tx, entidad, tipo_oper="COBRO", fecha_ejec=fecha_ejec, importe=importe,
                                    concepto=concepto, nombre_benef=nombre_benef, nombre_mandate=nombre_mandate
                                )
                                self._add_operdia_atomic(
                                    s_tx, entidad, tipo_oper="ABONO", fecha_ejec=fecha_ejec, importe=importe,
                                    concepto=concepto, nombre_benef=nombre_benef, nombre_mandate=nombre_mandate
                                )
                                return {"id": row["id"], "ok": True}
                            raise ValueError("Accion invalida")

                    except Exception as ex:
                        return {"id": row["id"], "ok": False, "err": f"{ex}"}


                inner_workers = max(1, min(MASIVO_PARALLEL_PER_CHUNK, len(work)))
                ok_res, ko_res = [], []
                with concurrent.futures.ThreadPoolExecutor(max_workers=inner_workers) as pool:
                    futs = [pool.submit(_do_one, entidad, r) for r in work]
                    for f in concurrent.futures.as_completed(futs):
                        res = f.result()
                        (ok_res if res.get("ok") else ko_res).append(res) 


                now = obtener_fecha_actual()
                entidad1=entidad
                with self.session_scope_ctx() as s_upd:

                    touched = set(int(w["batch_id"]) for w in work)
                    if ok_res:
                        ids_ok = [r["id"] for r in ok_res]; by_id = {r["id"]: r for r in ok_res}
                        for iid in ids_ok:
                            it = s_upd.get(MasivoItem, iid)
                            if not it: continue
                            data = by_id[iid]
                            it.status = "PROCESADO"; it.error = None
                            it.attempts = (it.attempts or 0) + 1
                            it.processed_at = now; it.updated_at = now
                        results["ok"] += len(ids_ok)

                    if ko_res:
                        ids_ko = [r["id"] for r in ko_res]; by_idk = {r["id"]: r for r in ko_res}
                        for iid in ids_ko:
                            it = s_upd.get(MasivoItem, iid)
                            if not it: continue
                            it.status = "FALLADO"; it.error = (by_idk[iid].get("err") or "Fallo")[:512]
                            it.attempts = (it.attempts or 0) + 1; it.updated_at = now
                        results["nok"] += len(ids_ko)

                    for bid in touched: self._update_batch_stats(s_upd, entidad1,bid,)
                    results["batches_touched"].update(touched)
                    

        with concurrent.futures.ThreadPoolExecutor(max_workers=MASIVO_WORKERS_PER_CLIENT) as ex:
            futures = [ex.submit(_worker_loop) for _ in range(MASIVO_WORKERS_PER_CLIENT)]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    fut.result() 
                except Exception as e:
                    logger.error(f"Thread fatal error: {e}")
        results["batches_touched"] = list(results["batches_touched"])
        return results

    def _set_ingest_processed(self, sfinal, entidad, ingest_id: int):
        entidad = entidad
        print(entidad, entidad)
        s = sfinal.query(MasivoIngestSummary).filter(MasivoIngestSummary.entidad==entidad,MasivoIngestSummary.id == int(ingest_id)).first()
        print('s: ',s)
        if s:
            try:
                s.status = "Procesado"
            except Exception:
                pass
            s.finished_at = obtener_fecha_actual()
    
    def _apply_saldo_and_movto_atomic(self, sesion: Session, entidad, *,  ftkncli: str, dt:str, ftipo: str, falias: str,
                                    concepto: str, importe: float, toper: str):
        imp = float(importe or 0.0)
        op = (toper or "").strip().upper()
        if op not in ("COBRO", "ABONO"):
            raise ValueError("toper inválido")
        ok = self.cons_saldox(sesion, entidad, ftkncli, dt, ftipo, falias, imp, op)
        if not ok:
            raise ValueError("Saldo insuficiente o error al ajustar saldo")
        signo = "H" if op == "COBRO" else "D"
        movto = DBMOVTOS(
            entidad,
            ftkncli,
            dt,
            falias,
            signo,
            obtener_fecha_actual(),
            concepto,
            imp,
            'fake',
            'A',
            obtener_fecha_actual(),
            str(app_state.user_id),
            '0001-01-01',
            ' '
        )
        sesion.add(movto)
        sesion.flush()


    def _add_operdia_atomic(self, sesion: Session, entidad,  *, tipo_oper: str, fecha_ejec: str, importe: float,
                            concepto: str, nombre_benef: str, nombre_mandate: str):
        
        entidad1 = entidad
        num_transacc = 'fake'
        stat = 'Pendiente'
        cod_rechazo = ''
        num_intentos = 0
        usuario_alta = app_state.user_id
        fecha_mod = '0001-01-01'
        usuario_mod = ''
        id_acreedor = 'ID ACREEDOR'
        id_mandate = 'ID MANDATE'
        fecha_mandate = '2001-01-01'
        clve_mandate = 'RCUR'
        if str(tipo_oper).upper() == "COBRO":
            iban_benef, bic_benef = 'ES3513690890071354041177', 'BBVAESMM'
            iban_mand, bic_mand = 'GB90YTLG03175153632324', 'CGDIESMM'
        else:  
            iban_benef, bic_benef = 'GB90YTLG03175153632324', 'CGDIESMM'
            iban_mand, bic_mand = 'ES3513690890071354041177', 'BBVAESMM'
        regdia = DBOPERDIA(
            entidad1, num_transacc, tipo_oper, fecha_ejec, float(importe or 0.0),
            concepto, stat,
            iban_benef, bic_benef, nombre_benef, id_acreedor,
            id_mandate, iban_mand, bic_mand, nombre_mandate, fecha_mandate,
            clve_mandate, cod_rechazo, num_intentos, usuario_alta, fecha_mod, usuario_mod
        )
        sesion.add(regdia)
        sesion.flush()


    def cons_saldo(self, db, entidad, tkncli: str, datos: str, tipo: str, alias: str):
        csaldo = db.query(DBSALDOS).where(
                DBSALDOS.entidad == entidad,
                DBSALDOS.tkncli == tkncli,
                DBSALDOS.datos == datos,
                DBSALDOS.tipo == tipo,
                DBSALDOS.alias == alias).first()
        if csaldo:
            return csaldo.saldo
        else: 
            return 0.0
    
    def cons_saldox(self, db, entidad, tkncli: str,  datos: str, tipo: str, alias: str, importe: float, toper: str):
       
        importe = float(importe or 0.0)
        op = (toper or "").strip().upper()
        with db.no_autoflush:
            reg_act = (
                db.query(DBSALDOS)
                .where(
                    DBSALDOS.entidad == entidad,
                    DBSALDOS.tkncli == tkncli,
                    DBSALDOS.datos == datos,
                    DBSALDOS.tipo == tipo,
                    DBSALDOS.alias == alias
                )
                .with_for_update()
                .first()
            )
            if not reg_act:
                
                reg_act = DBSALDOS(
                    entidad, tkncli, datos, tipo, alias,
                    0.0, obtener_fecha_actual(),
                    obtener_fecha_actual(), str(app_state.user_id),
                    obtener_fecha_actual(), str(app_state.user_id)
                )
                db.add(reg_act)
                db.flush() 
            saldo_before = float(reg_act.saldo or 0.0)
            if op == "COBRO":
                saldo_after = saldo_before + importe
            elif op == "ABONO":
                saldo_after = saldo_before - importe
                if saldo_after < 0:
                    return False
            else:
                return False
            reg_act.saldo = float(saldo_after)
            reg_act.fecha_mod = obtener_fecha_actual()
            reg_act.usuario_mod = str(app_state.user_id)

        return True

    def ejecarch(self,datos,db,ip_origen,time_start):
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

        if not estado:
            result.append({'response': 'informacion incorrecta'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, rc

        try:
            dataJSON = json.loads(decriptMsg)
        except Exception:
            result.append({'response': 'JSON inválido'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, rc
        
        # Validación mínima
        if not all(k in dataJSON for k in ("id", "entidad", "filename")):
            result.append({'response': 'Algun elemento de la transacción esta faltando'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, rc

        ingest_id = dataJSON.get("id")
        entidad = dataJSON.get("entidad")
        filename  = dataJSON.get("filename")

        if not valcampo('num', ingest_id, 1) or not valcampo('long', entidad, 8) or not valcampo('long', filename, 255):
            result.append({'response': 'Parámetros inválidos'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, rc

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


        # 1) localizar el summary
        summary = db.query(MasivoIngestSummary).filter(
            MasivoIngestSummary.entidad == entidad,
            MasivoIngestSummary.id == int(ingest_id),
            MasivoIngestSummary.filename == filename
        ).first()

        if not summary:
            result.append({'response': 'registro inexistente (summary).'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, rc

        # No ejecutar si ya está Procesado
        if hasattr(summary, "status"):
            st = (summary.status or "").strip().lower()
            if st == "procesado":
                result.append({'response': 'No permitido: el archivo ya fue Procesado.'})
                message = json.dumps(result)
                app_state.jsonenv = message 
                return result, 400

        # 2) Determinar batch_ids de ESTA ingesta (vía logs)
        q_logs = (db.query(MasivoIngestLog.batch_id)
                  .where(MasivoIngestLog.entidad == entidad,
                         MasivoIngestLog.ingest_id == ingest_id,
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
        clientes = [r[0] for r in
                    db.query(MasivoItem.cliente).where(
                        MasivoItem.entidad == entidad,
                        MasivoItem.batch_id.in_(batch_ids),
                        MasivoItem.status == STATUS_PENDING
                    ).distinct().all()]

        if not clientes:
            result.append({'response': 'No hay registros pendientes asociados al archivo.'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, 400
        print('5')

        # 4) sumatoria de cargos y abonos
        items_q = (
            db.query(MasivoItem.accion, MasivoItem.importe)
            .where(
                MasivoItem.entidad == entidad,
                MasivoItem.batch_id.in_(batch_ids),
                MasivoItem.status == STATUS_PENDING
            )
        )

        suma_cobros = 0.0  
        suma_abonos = 0.0  

        for acc, imp in items_q.all():
            accion = int(acc or 0)
            importe = float(imp or 0.0)

            if accion in (2, 4, 5):
                suma_cobros += importe

            if accion in (3, 4, 5):
                suma_abonos += importe

        # 5) saldo actual (cuenta concentradora)
        ctaprop = db.query(DBCTAPERS).filter(
                        DBCTAPERS.entidad == entidad,
                        DBCTAPERS.entban == '9999'
                        ).first()
        if not ctaprop:
            result.append({'response': 'No hay cuenta propia asociada.'})
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, 400
        
        tk = ctaprop.tknper
        dt = ctaprop.datos
        tp = ctaprop.tipo
        al = ctaprop.alias
        registro_saldo = self.cons_saldo(db, entidad, tk, dt, tp, al) 
        saldo_actual = float(registro_saldo)

        saldo_proyectado = saldo_actual + suma_cobros - suma_abonos
        if saldo_proyectado < 0:
            print('No hay saldo suficiente para procesar archivo.')
            result.append({
                'response': 'No hay saldo suficiente para procesar archivo.',
                'saldo_actual': saldo_actual,
                'suma_cobros': suma_cobros,
                'suma_abonos': suma_abonos
            })
            message = json.dumps(result)
            app_state.jsonenv = message
            return result, 400
            
        # 6) Procesamiento concurrente por cliente
        aggregate = {"clientes": [], "tot_ok": 0, "tot_nok": 0}
        def _runner_cliente(db,cli):
            return self._process_cliente(db, entidad, cli, filename, chunk_size, batch_ids,tk,dt,tp,al)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MASIVO_CLIENTS_PARALLEL) as ex:
            futs = [ex.submit(_runner_cliente, db, cli) for cli in clientes]

            for fut in concurrent.futures.as_completed(futs):
                r = fut.result()
                aggregate["clientes"].append(r)
                aggregate["tot_ok"] += r.get("ok", 0)
                aggregate["tot_nok"] += r.get("nok", 0)
        print('5.2')
        # 7) Marcar summary como Procesado (si todo terminó)
        with self.session_scope_ctx() as sfinal:
            self._set_ingest_processed(sfinal, entidad, int(ingest_id))

        # 8) Respuesta OK (encriptada como el resto)
        rc = 201
        result.append({
            "response": "Exito",
            "archivo": filename,
            "id": int(ingest_id),
            "Total ok": aggregate["tot_ok"],
            "Total Nok": aggregate["tot_nok"],
        })
        message = json.dumps(result, ensure_ascii=False)
        app_state.jsonenv = message
        encripresult, _ = encrypt_message(password, message)
        return encripresult, rc


    def selproej(self,datos,db):
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
            param_keys = ['id','entidad','filename','inserted']
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
                inserted = dataJSON.get("inserted")
        else:
            estado = False
            result = 'informacion incorrecta'
            message = json.dumps(result)
            app_state.jsonenv = message 
            rc = 400

        if estado:
            validations = {
            'id': ('num', 1),
            'entidad': ('long', 8), 
            'filename': ('long', 255),
            'inserted': ('num', 1)
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
                q_total = inserted

                q_logs = (db.query(MasivoIngestLog.batch_id)
                  .where(MasivoIngestLog.entidad==entidad,
                         MasivoIngestLog.ingest_id == id,
                         MasivoIngestLog.filename == filename,
                         MasivoIngestLog.batch_id.isnot(None))
                  .distinct())
                batch_ids = [r[0] for r in q_logs.all()]

                if not batch_ids:
                    result.append({'response': 'No hay registros asociados al archivo.'})
                    message = json.dumps(result)
                    app_state.jsonenv = message 
                    print('no baches ids')
                    return result, 400
                
                # 3) Obtener conteo de los batch_ids con items diferente a PENDING
                q_avance = db.query(func.count(MasivoItem.id)).where(
                    MasivoItem.entidad==entidad,
                    MasivoItem.batch_id.in_(batch_ids),
                    MasivoItem.filename == filename,
                    MasivoItem.status != STATUS_PENDING
                    ).scalar()
                result.append({ 
                        'id': id,
                        'filename':filename,
                        'total':q_total,
                        'realizados':q_avance
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
