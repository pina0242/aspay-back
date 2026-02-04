
from app.core.models import MasivoBatch, MasivoItem, MasivoIngestSummary, MasivoIngestLog, DBLAYOUT
from app.core.cypher import encrypt_message , decrypt_message
from app.core.cai import valcampo, obtener_fecha_actual
from app.core.config import settings
from app.core.state import app_state
from app.entidad.services import EntService
from sqlalchemy import func ,select , select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import timedelta
import logging
import json 
import os, time, json, shutil
import uuid
from werkzeug.utils import secure_filename
logger = logging.getLogger(__name__)
MAX_ERROR_LEN = int(os.environ.get("MASIVO_ERROR_MAXLEN", "0"))
BATCH_ACTIVE = 'ACTIVE'
BATCH_PAUSED = 'PAUSED'
OK_DIR   = os.environ.get("MASIVO_OK_DIR",   "./data/archive")
# Estados y banderas
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

class OperServiceFiles:

    
    
    def __init__(self, db_session=None):
        self.db = db_session

    # --- NUEVO: Asegura carpeta de carga ---
    def ensure_upload_folder(self, folder: str) -> None:
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"No se pudo crear/verificar la carpeta de carga: {folder}") from e

    # --- NUEVO: Normaliza nombre y fuerza extensión .csv ---
    def normalize_csv_filename(self, filename: str) -> str:
        safe_name = secure_filename(filename or "archivo.csv")
        name, _ = os.path.splitext(safe_name)
        # fuerza .csv para evitar que el runner detecte antes de tiempo
        return f"{name}.csv"
    
    def write_csv_atomic(self, upload_folder: str, filename: str, content: str, chunk_size: int = 1024 * 1024) -> str:
        """
        Escribe primero a un archivo temporal (oculto/part), fuerza fsync y luego
        hace os.replace al .csv final PARA EVITAR que masivos_runner lo detecte incompleto.
        Retorna el nombre final (csv).
        """
        self.ensure_upload_folder(upload_folder)

        final_filename = self.normalize_csv_filename(filename)
        final_path = os.path.join(upload_folder, final_filename)

        # Archivo temporal oculto con sufijo único para que el runner no lo vea.
        tmp_filename = f".{final_filename}.{uuid.uuid4().hex}.part"
        tmp_path = os.path.join(upload_folder, tmp_filename)

        # Escritura en chunks (texto). Si tu 'content' viniera en base64, decódificalo aquí antes.
        try:
            with open(tmp_path, 'w', encoding='utf-8', newline='') as f:
                for i in range(0, len(content), chunk_size):
                    f.write(content[i:i + chunk_size])
                f.flush()
                os.fsync(f.fileno())

            # Renombre atómico al .csv final (no dispara el runner hasta aquí).
            os.replace(tmp_path, final_path)
        finally:
            # Si algo falló, intentamos limpiar el temporal
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        return final_filename

    # --- Logs de ingesta ---
    def create_ingest_summary(self, db, entidad, filename: str) -> int:
        s = MasivoIngestSummary(
        entidad=entidad,
        filename=filename,
        status="Pendiente",
        started_at=obtener_fecha_actual(),
        created_at=obtener_fecha_actual()
        )
        db.add(s)
        db.flush()
        return int(s.id)


    def _ymd(self,v) -> str:
        # Normaliza a AAAA-MM-DD
        from datetime import datetime, date
        if isinstance(v, datetime):
            return v.date().isoformat()
        if isinstance(v, date):
            return v.isoformat()
        return str(v)[:10]

    def init_schema(self, db):
        # Asegura que las tablas existan con el engine ya configurado
        engine_or_conn = db.get_bind()
        try:
            engine = engine_or_conn.engine
        except AttributeError:
            engine = engine_or_conn
        db.metadata.create_all(bind=engine)

    def _sha256(self,s: str) -> str:
        import hashlib
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _lower_keys(self,d: dict) -> dict:
        return { (k.lower() if isinstance(k, str) else k): v for k, v in d.items() }

    def _mk_idem(self,entidad, row: dict) -> str:
        """
        Llave de idempotencia: combina campos clave + importe normalizado + fecha de ejecución YYYY-MM-DD
        """
        parts = [
            entidad, row['accion'],row['cliente'], row['tkncliori'], row['aliasori'], row['tipoori'],
            row['tknclides'], row['aliasdes'], row['tipodes'],
            row['concepto'], str(row['importe']).strip(),str(row['fecha_ejecucion']).strip()[:10]
        ]
        return self._sha256("|".join(p.strip() for p in parts))
    
    def _mk_idemval(self,entidad, row: dict) -> str:
        
        parts = [
            entidad, row['accion'],row['cliente'], row['tkncliori'], row['aliasori'], row['tipoori'],
            row['tknclides'], row['aliasdes'], row['tipodes'],
            row['concepto'], str(row['importe']).strip()
        ]
        return self._sha256("|".join(p.strip() for p in parts))
    
    def _busca_layout(self,db,entidad, llave: str, clave: str) -> list[str]:
        print('entre layout')
        ordered_unique_headers: list[str] = []
        estatus='A'
        resultado = db.query(DBLAYOUT.datos).order_by(DBLAYOUT.id.asc()).where(
            DBLAYOUT.entidad==entidad,
            DBLAYOUT.llave==llave,
            DBLAYOUT.clave==clave,
            DBLAYOUT.status==estatus
        ).all()
        
        seen_headers: set[str] = set() 
        if resultado:
            for row in resultado:
                header_raw = row[0]
                cleaned_h = header_raw.strip().lower()
                if cleaned_h not in seen_headers:
                    ordered_unique_headers.append(cleaned_h)
                    seen_headers.add(cleaned_h)
        print('saliendo layout')
        return ordered_unique_headers
    
   # --- Lector CSV tolerante (solo exige REQUIRED_HEADERS; ignora extra/orden)
    def _read_csv_path(self, db, entidad, path: str, filename: str) -> list[dict]:
        import csv, os
        out: list[dict] = []
        has_header = False
        start_line_no = 1
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            dr = csv.DictReader(f)
            if not dr.fieldnames:
                return out
            headers_lower = [h.strip().lower() for h in dr.fieldnames]
            missing = [req for req in REQUIRED_HEADERS if req not in headers_lower]
            if missing:
                headers_lower = self._busca_layout(db, entidad, 'MASIHEAD', filename)
                if not headers_lower:
                    raise ValueError(f"No se encontraron encabezados requeridos para el archivo {filename}.")    
                else:   
                    first_row_values = dr.fieldnames 
                    picked_first = {}
                    for i, value in enumerate(first_row_values):
                        if i < len(headers_lower):
                            key = headers_lower[i] 
                            picked_first[key] = str(value).strip()
                    out.append(picked_first)
                    dr2 = csv.reader(f, delimiter=',') 
                    for row in dr2:
                        picked = {}
                        for i, value in enumerate(row):
                            if i < len(headers_lower):
                                key = headers_lower[i] 
                                picked[key] = str(value).strip()
                        out.append(picked)
            else:
                has_header = True
                start_line_no = 2
                for row in dr:
                   raw = self._lower_keys(row)
                   picked = { k: str(raw.get(k, "")).strip() for k in REQUIRED_HEADERS }
                   out.append(picked)
        return out, start_line_no

    def add_ingest_log(self, db, entidad, ingest_id: int, *, filename: str,
                       code: str, message: str, cliente: str | None = None,
                       batch_id: int | None = None, line_no: int | None = None,
                       idem: str | None = None, detail: dict | None = None):
        entry = MasivoIngestLog(
            entidad=entidad,ingest_id=ingest_id, batch_id=batch_id, cliente=cliente, filename=filename,
            line_no=line_no, idempotency_key=idem, code=code, message=message or "",
            detail=json.dumps(detail, ensure_ascii=False) if detail is not None else None,
            created_at=obtener_fecha_actual()
        )
        db.add(entry)

    def _validate_row(self,row: dict, line_no: int) -> tuple[bool, str]:
        req = ['accion','cliente','tkncliori','aliasori','tipoori','ordenante','tknclides','aliasdes','tipodes','beneficiario','concepto','importe','fecha_ejecucion']
        missing = [k for k in req if not str(row.get(k, '')).strip()]
        if missing:
            return False, f"L{line_no}: faltan campos: {', '.join(missing)}"

        # Reglas de longitud/forma 
        if not valcampo('long', str(row['accion']).strip(), 1): return False, f"L{line_no}: accion excede longitud máxima (1)"
        if int(row['accion']) < 1 and int(row['accion']) > 5: return False, f"L{line_no}: accion no valida"
        if not valcampo('long',row.get('cliente',''),8): return False, f"L{line_no}: cliente debe ser de 8 caracteres"
        if not valcampo('long',row.get('tkncliori',''), 36): return False, f"L{line_no}: Cliente ordenante invalido"
        if not valcampo('long',row.get('aliasori',''), 10): return False, f"L{line_no}: Alias ordenante invalido"
        if not valcampo('long',row.get('tipoori',''), 3):  return False, f"L{line_no}: Tipo ordenante invalido"
        if not valcampo('long',row.get('ordenante',''),60): return False, f"L{line_no}: ordenante invalido"
        if not valcampo('long',row.get('tknclides',''), 36): return False, f"L{line_no}: Cliente benficiario invalido"
        if not valcampo('long',row.get('aliasdes',''), 10): return False, f"L{line_no}: Alias benficiario invalido"
        if not valcampo('long',row.get('tipodes',''), 3):  return False, f"L{line_no}: Tipo beneficiario invalido"
        if not valcampo('long',row.get('beneficiario',''),60): return False, f"L{line_no}: beneficiario invalido"
        if not valcampo('long',row.get('concepto',''),30):  return False, f"L{line_no}: Concepto invalido"
        if not valcampo('date',row.get('fecha_ejecucion',''),10): return False, f"L{line_no}: fecha_ejecucion inválida (AAAA-MM-DD)"
        if not valcampo('float',row.get('importe',''),1):  return False, f"L{line_no}: importe inválido"

        return True, ""

   # --- Batches ---
    def create_batch(self, db, entidad, cliente: str, filename: str, total: int) -> int:
        b = MasivoBatch(
            entidad=entidad,cliente=cliente, filename=filename, total=int(total or 0),
            processed=0, successful=0, failed=0, status=BATCH_ACTIVE,
            created_at=obtener_fecha_actual()
        )
        db.add(b)
        db.flush()
        return int(b.id)

    # --- Staging/insert con idempotencia ---
    def _stage_items(self, db, entidad, filename: str, rows: list[dict],
                     reintentar: bool, ingest_id: int, start_line_no: int):
        by_cliente: dict[str, list] = {}
        duplicates_in_file = 0
        invalids: list[str] = []
        seen = set()
        print('psc:',start_line_no)
        # 1) Valida y detecta duplicados dentro del archivo
        for i, r in enumerate(rows, start=start_line_no):  # asumiendo encabezados en línea 1
            ok, err = self._validate_row(r, i)
            if not ok:
                invalids.append(err)
                cliente_orig = str(r.get('cliente', '')).strip()
                cliente_slice = cliente_orig[:8]
                r['cliente'] = cliente_slice
                self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="INVALIDO",
                                         message=err, cliente=r.get('cliente').strip()[:8], line_no=i, detail={"row": r}) 
                continue
            idem = self._mk_idem(entidad, r)
            idemval = self._mk_idemval(entidad, r)
            if idem in seen:
                duplicates_in_file += 1
                self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="DUP_ARCHIVO",
                                         message=f"Duplicado en archivo (L{i})",
                                         cliente=r['cliente'], line_no=i, idem=idem, detail={"row": r})
                continue
            seen.add(idem)
            by_cliente.setdefault(r['cliente'], []).append((i, r, idem))

        summary: dict[str, dict[str,int]] = {}
        now = obtener_fecha_actual()

        # 2) Inserción por cliente (batch) + idempotencia
        for cliente, items in by_cliente.items():
            batch_id = self.create_batch(db, entidad, cliente, filename, total=len(items))
            inserted = skipped = reinstated = 0

            for line_no, r, idem in items:
                stmt = (
                    pg_insert(MasivoItem)
                    .values(
                        entidad=entidad, batch_id=batch_id, accion=r['accion'].strip(), cliente=cliente, filename=filename, line_no=line_no,
                        tkncliori=r['tkncliori'].strip(), aliasori=r['aliasori'].strip(), tipoori=r['tipoori'].strip(),
                        ordenante=r['ordenante'].strip(),
                        tknclides=r['tknclides'].strip(), aliasdes=r['aliasdes'].strip(), tipodes=r['tipodes'].strip(),
                        beneficiario=r['beneficiario'].strip(),
                        concepto=r['concepto'].strip(),
                        importe=float(r['importe']), fecha_ejec=self._ymd(r['fecha_ejecucion']),
                        status=STATUS_PENDING, error=None, attempts=0,
                        idempotency_key=idem, idempotency_val=idemval,created_at=now, updated_at=now
                    )
                    .on_conflict_do_nothing(index_elements=['idempotency_key'])
                )
                res = db.execute(stmt)
                if res.rowcount and res.rowcount > 0:
                    inserted += 1
                    self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="INGRESADO",
                                             message="Insertado como PENDIENTE",
                                             cliente=cliente, batch_id=batch_id, line_no=line_no,
                                             idem=idem, detail={"importe": float(r["importe"])})
                else:
                    # Ya existía; si reintentar=True, podrías revisar estado actual (opcional)
                    if reintentar:
                        row = db.execute(
                            select(MasivoItem.id, MasivoItem.status).where(MasivoItem.idempotency_key == idem)
                        ).first()
                        if row:
                            existing_id, st = int(row[0]), str(row[1] or "")
                            # Si estaba FAILED, podrías reactivar a PENDING (aquí solo contabilizamos)
                            if st.upper() == STATUS_FAILED:
                                reinstated += 1
                                db.execute(
                                    update(MasivoItem).where(MasivoItem.entidad == entidad, MasivoItem.id == existing_id).values(status=STATUS_PENDING, updated_at=now)
                                )
                                self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="REINSTATED",
                                                         message="Reactivado desde FALLADO a PENDIENTE",
                                                         cliente=cliente, batch_id=batch_id, line_no=line_no,
                                                         idem=idem)
                            else:
                                skipped += 1
                                self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="EXISTE",
                                                         message=f"Repetido en base con estado={st}",
                                                         cliente=cliente, batch_id=batch_id, line_no=line_no,
                                                         idem=idem)
                    else:
                        skipped += 1
                        self.add_ingest_log(db, entidad,ingest_id, filename=filename, code="EXISTE",
                                                 message="Repetido",
                                                 cliente=cliente, batch_id=batch_id, line_no=line_no,
                                                 idem=idem)

            summary[cliente] = {
                "batch_id": batch_id,
                "inserted": inserted,
                "skipped": skipped,
                "reinstated": reinstated
            }

        return summary, duplicates_in_file, invalids

    def finish_ingest_summary(self, db, entidad, ingest_id: int, counters: dict, raw_summary: dict):
        s = db.get(MasivoIngestSummary, ingest_id)
        if not s:
            return
        s.total = counters.get("total", 0)
        s.invalid = counters.get("invalid", 0)
        s.duplicates_in_file = counters.get("duplicates_in_file", 0)
        s.inserted = counters.get("inserted", 0)
        s.skipped_existing = counters.get("skipped_existing", 0)
        s.reinstated_failed = counters.get("reinstated_failed", 0)
        s.finished_at = obtener_fecha_actual()
        s.raw_summary = json.dumps(raw_summary, ensure_ascii=False)

    # --- Ingesta de uno o varios archivos 
    def ingestar_archivos(self, db,entidad, paths: list[str], reintentar: bool = False) -> dict:
        import os
        paths = [p for p in paths if p and os.path.isfile(p)]
        if not paths:
            raise FileNotFoundError("No se encontraron archivos válidos.")
        summary_global = {"archivos":0,"registros":0,"duplicados_en_archivo":0,"invalidos":[],"por_cliente":{}}

        
        for path in paths:
            filename = os.path.basename(path)
            rows, start_line_no  = self._read_csv_path(db, entidad, path,filename)  # tolerante a layout
            summary_global["archivos"] += 1
            summary_global["registros"] += len(rows)

            ingest_id = self.create_ingest_summary(db, entidad,  filename)

            summary_por_cliente, dup_in_file, invalids = self._stage_items(
                db, entidad, filename, rows, reintentar, ingest_id, start_line_no
            )

            inserted_total    = sum(s["inserted"]    for s in summary_por_cliente.values())
            skipped_total     = sum(s["skipped"]     for s in summary_por_cliente.values())
            reinstated_total  = sum(s["reinstated"]  for s in summary_por_cliente.values())
            counters = {
                "total": len(rows),
                "invalid": len(invalids),
                "duplicates_in_file": dup_in_file,
                "inserted": inserted_total,
                "skipped_existing": skipped_total,
                "reinstated_failed": reinstated_total
            }

            self.finish_ingest_summary(db, entidad,ingest_id, counters, {
                "por_cliente": summary_por_cliente,
                "invalidos": invalids
            })

            summary_global["duplicados_en_archivo"] += dup_in_file
            summary_global["invalidos"].extend(invalids)
            for cli, info in summary_por_cliente.items():
                pc = summary_global["por_cliente"].setdefault(cli, {"batches":[]})
                pc["batches"].append({
                    "batch_id": info["batch_id"],
                    "insertados": info["inserted"],
                    "saltados_existentes": info["skipped"],
                    "reinstalados_failed": info["reinstated"]
                })
        return summary_global

   
    def files(self,datos, db):
        result = []
        estado = True
        message = datos
        message = str(message).replace('"','')
        
        
        if not settings.PASSCYPH or settings.PASSCYPH == "default-password":
            logger.error("PASSCYPH no configurado en .env")
            return None, 500
        else:
            password = settings.PASSCYPH
            decrypted_content , estado = decrypt_message(password, message)
            app_state.jsonrec = decrypted_content

        try:
            # 3. Parsear el JSON para obtener el nombre y el contenido del archivo
            file_info = json.loads(decrypted_content)
            filename = file_info.get('filename')
            content = file_info.get('content')
            entidad = file_info.get('entidad')

            if not filename or content is None:
                result = [{'response': 'Faltan datos en el JSON desencriptado (nombre o contenido).'}]
                message = json.dumps(result)
                app_state.jsonenv = message 
                return result, 400


            entidad_usu = app_state.entidad
            valenti = EntService.selent(entidad, db)
            if valenti:
                #entidad_usu = '4000'
                if entidad_usu != '0001':
                    if entidad_usu != entidad:
                        result.append({'response':'Solo puede operar su propia entidad'})    
                        message = json.dumps(result)
                        app_state.jsonenv = message                                 
                        return result, 400 

            else:
                if entidad_usu != '0001':
                    result.append({'response':'No existe entidad'})    
                    message = json.dumps(result)
                    app_state.jsonenv = message                                 
                    return result, 400


            # 4. Ruta de guardado (se mantiene)
            upload_folder = 'data/incoming'
            entidadu= app_state.entidad
            print('entidad: ', entidad)
            print('entidadu: ', entidadu)
            
            # 5. Guardado atómico en chunks y RENOMBRE a .csv al final
            #    Esto evita que masivos_runner vea el archivo hasta que esté completo.
            saved_csv = self.write_csv_atomic(upload_folder, filename, content)

            # 6) carga de archivos
            #    Puedes habilitar reintento leyendo env MASIVO_RETRY_FAILED (1/0)
            reintentar = bool(int(os.environ.get("MASIVO_RETRY_FAILED", "0")))
            csv_path = os.path.join(upload_folder, saved_csv)

            summary = self.ingestar_archivos(db,entidad,[csv_path], reintentar=reintentar)

            # 7) despues de cargar archivo se procede a mover archivo a archivados
            self.ensure_upload_folder(OK_DIR)
            name, ext = os.path.splitext(saved_csv)
            path_arch= os.path.join(OK_DIR, f"{name}.{int(time.time())}{ext}")
            archived =  shutil.move(csv_path, path_arch)
            db.commit()
            # 8) Respuesta
            result = [{
                'response': f'Archivo "{saved_csv}" guardado correctamente.',
                'Resutados': summary
            }]
            return result, 201


        except json.JSONDecodeError:
            result = [{'response': 'Error al decodificar el JSON. El contenido no es un JSON válido.'}]
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, 400
        except Exception as e:
            # Manejo genérico por si algo raro ocurre en escritura/renombre
            result= [{'response': f'Error al guardar: {str(e)}'}]
            message = json.dumps(result)
            app_state.jsonenv = message 
            return result, 500       

