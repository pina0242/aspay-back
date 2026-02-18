import logging
from datetime import datetime

from app.core.database import SessionLocal, engine
from app.core.models import DB_BATCH_CONFIG, DB_BATCH_HISTORY,DBLOGENTRY_HIST,DBLOGENTRY
from datetime import date
from sqlalchemy import insert, delete, select,func
from app.batch.histlog import hist_log
logger = logging.getLogger(__name__)

# --- Aquí irás definiendo tus procesos batch ---

def proceso_ejemplo_notificacion():
    print(f"[{datetime.now()}] Ejecutando: Envío de reportes diarios...")
    # Aquí iría tu lógica real
    return "Reportes enviados con éxito"

def proceso_limpieza_logs():
    print(f"[{datetime.now()}] Ejecutando: Limpieza de logs temporales...")
    return "Limpieza completada"

# 1. Definimos la función física que hace el trabajo
def ejecutar_prueba_test(task_db_id=None):
    db = SessionLocal(bind=engine)
    print(f"\n!!! DISPARO DE PROCESO DETECTADO ID!!!!!!!! !!! ID: {task_db_id}")

    print("\n" + "!"*60)
    print(f"!!! DISPARO DE PROCESO DETECTADO !!!")
    print(f"Hora Sistema: {datetime.now()}")
    print("!"*60 + "\n")
    print(task_db_id)
    try:
     
        tarea = db.query(DB_BATCH_CONFIG).filter(DB_BATCH_CONFIG.task_path == 'test_debug', DB_BATCH_CONFIG.id == task_db_id ).first()

        if tarea:

                historico = DB_BATCH_HISTORY(
                entidad=tarea.entidad, 
                nombre_proceso=tarea.nombre_proceso, 
                task_path=tarea.task_path, 
                ultima_ejecucion= datetime.now(),
                ultimo_resultado="EJECUTADO MANUALMENTE PARA PRUEBA"
            )
        db.add(historico)
        


        db.commit()

    except Exception as e:
        print(f">>> ERROR AL ACTUALIZAR BD: {e}")
    finally:
        db.close()
def historico_log(task_db_id=None):
     respuesta = hist_log(task_db_id)
     print(f"\n!!! Historico log correcto!!!!!!!! !!!,{respuesta}")

AVAILABLE_TASKS = {
    "test_debug": ejecutar_prueba_test,
    "hist_log": historico_log,
    "enviar_reportes": proceso_ejemplo_notificacion,
    "limpiar_logs": proceso_limpieza_logs,
}