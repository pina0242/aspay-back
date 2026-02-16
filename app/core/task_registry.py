import logging
from datetime import datetime

from app.core.database import SessionLocal, engine
from app.core.models import DB_BATCH_CONFIG

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
def ejecutar_prueba_test():
    db = SessionLocal(bind=engine)
    print("\n" + "!"*60)
    print(f"!!! DISPARO DE PROCESO DETECTADO !!!")
    print(f"Hora Sistema: {datetime.now()}")
    print("!"*60 + "\n")
    
    try:
        # Intentamos actualizar la tabla
        tarea = db.query(DB_BATCH_CONFIG).filter(DB_BATCH_CONFIG.task_path == 'test_debug').first()
        if tarea:
            tarea.ultima_ejecucion = datetime.now()
            tarea.ultimo_resultado = "EJECUTADO MANUALMENTE PARA PRUEBA"
            db.commit()
            print(">>> BASE DE DATOS ACTUALIZADA EXITOSAMENTE")
    except Exception as e:
        print(f">>> ERROR AL ACTUALIZAR BD: {e}")
    finally:
        db.close()



# --- El Registro ---
# Este diccionario mapea el nombre que React enviará con la función de Python
AVAILABLE_TASKS = {
    "test_debug": ejecutar_prueba_test,
    "enviar_reportes": proceso_ejemplo_notificacion,
    "limpiar_logs": proceso_limpieza_logs,
}