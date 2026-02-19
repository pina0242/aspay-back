from sqlalchemy.orm import Session
from app.core.models import DB_BATCH_CONFIG
from app.core.scheduler_manager import scheduler_manager
from app.core.task_registry import AVAILABLE_TASKS
import logging

logger = logging.getLogger(__name__)

class BatchService:
    def __init__(self, db: Session):
        self.db = db

    def crear_tarea_programada(self, nombre: str, task_key: str, cron_str: str):
        # 1. Validar que la tarea existe en nuestro registro
        if task_key not in AVAILABLE_TASKS:
            raise ValueError(f"La tarea {task_key} no está registrada en el sistema.")

        # 2. Guardar en Base de Datos
        nueva_tarea = DB_BATCH_CONFIG(
            nombre_proceso=nombre,
            task_path=task_key,
            cron_expression=cron_str,
            is_active=True
        )
        self.db.add(nueva_tarea)
        self.db.commit()
        self.db.refresh(nueva_tarea)

        # 3. Programar en el Scheduler en tiempo real
        scheduler_manager.add_or_update_job(
            task_id=nueva_tarea.id,
            func=AVAILABLE_TASKS[task_key],
            cron_str=cron_str,
            name=nombre
        )
        return nueva_tarea

    def cargar_todas_las_tareas_al_inicio(self):
        """Este método se llamará en el startup de la app"""
        tareas = self.db.query(DB_BATCH_CONFIG).filter(DB_BATCH_CONFIG.is_active == True).all()
        for t in tareas:
            if t.task_path in AVAILABLE_TASKS:
                scheduler_manager.add_or_update_job(
                    task_id=t.id,
                    func=AVAILABLE_TASKS[t.task_path],
                    cron_str=t.cron_expression,
                    name=t.nombre_proceso
                )
        logger.info(f"Se cargaron {len(tareas)} tareas al Scheduler.")