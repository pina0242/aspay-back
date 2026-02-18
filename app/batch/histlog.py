
from datetime import datetime

from app.core.database import SessionLocal, engine
from app.core.models import DB_BATCH_CONFIG, DB_BATCH_HISTORY,DBLOGENTRY_HIST,DBLOGENTRY
from datetime import date
from sqlalchemy import insert, delete, select,func




def hist_log(task_db_id=None):
    db = SessionLocal(bind=engine)
    try:
        # Llamamos a la función de archivado
        archivar_registro(db, DBLOGENTRY, DBLOGENTRY_HIST, task_db_id)
        tarea = db.query(DB_BATCH_CONFIG).filter(DB_BATCH_CONFIG.task_path == 'hist_log', DB_BATCH_CONFIG.id == task_db_id ).first()

        if tarea:

                historico = DB_BATCH_HISTORY(
                entidad=tarea.entidad, 
                nombre_proceso=tarea.nombre_proceso, 
                task_path=tarea.task_path, 
                ultima_ejecucion= datetime.now(),
                ultimo_resultado="EJECUTADO MANUALMENTE RESPALDO"
            )
        db.add(historico)
        db.commit()
    finally:
        
        db.close()

def archivar_registro(session, modelo_origen, modelo_destino, registro_id):
    try:

        tabla_orig = modelo_origen.__table__
        tabla_hist = modelo_destino.__table__


        stmt_select = select(DBLOGENTRY).filter(func.date(DBLOGENTRY.timestar) != date.today())
            

        columnas = tabla_orig.columns.keys()
        
        stmt_insert = insert(tabla_hist).from_select(columnas, stmt_select)
        session.execute(stmt_insert)

        # 3. Borrar el registro de la tabla original
        stmt_delete = delete(tabla_orig).filter(func.date(DBLOGENTRY.timestar) != date.today())
        session.execute(stmt_delete)

        # Confirmamos la transacción
        session.commit()
        print(f"Registro  movido a histórico con éxito.")

    except Exception as e:
        session.rollback()
        print(f"Error al archivar: {e}")
        raise        
