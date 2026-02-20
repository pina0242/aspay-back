from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from tzlocal import get_localzone
import logging

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
        # Configuramos el scheduler con la zona horaria local del servidor
        self.scheduler = BackgroundScheduler(timezone=str(get_localzone()))
        self.jobs_map = {} # Para rastrear jobs activos por ID de la base de datos

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("--- APScheduler Iniciado ---")

    def shutdown(self):
        self.scheduler.shutdown()
        logger.info("--- APScheduler Detenido ---")

    def add_or_update_job(self, task_id, func, cron_str, name):
        """
        Registra o actualiza un trabajo en el motor de APScheduler.
        """
        job_id = f"job_{task_id}"
        
        # Si el job ya existía en el motor, lo removemos para evitar duplicados
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        try:
            trigger = CronTrigger.from_crontab(cron_str)
            self.scheduler.add_job(
                func, 
                trigger, 
                id=job_id, 
                name=name,
                replace_existing=True,
                misfire_grace_time=3600  # Tolerancia de 1 hora si el server estuvo apagado
            )
            logger.info(f"Job programado/actualizado: {name} (ID: {task_id}) con cron [{cron_str}]")
        except Exception as e:
            logger.error(f"Error al programar el job {name}: {e}")

    def reload_job(self, task_record):

        job_id = f"job_{task_record.id}"
        
        # 1. Siempre intentamos removerlo primero si existe
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            logger.info(f"Tarea {job_id} removida del motor para actualización.")

        # 2. Si la tarea está marcada como activa en la BD, la volvemos a montar
        if task_record.is_active:
            # Importación local para evitar errores circulares
            from app.core.task_registry import AVAILABLE_TASKS
            
            func = AVAILABLE_TASKS.get(task_record.task_path)
            
            if func:
                self.add_or_update_job(
                    task_id=task_record.id,
                    func=func,
                    cron_str=task_record.cron_expression,
                    name=task_record.nombre_proceso
                )
            else:
                logger.warning(f"No se pudo recargar: '{task_record.task_path}' no existe en AVAILABLE_TASKS")
        else:
            logger.info(f"Tarea {job_id} no fue recargada porque está INACTIVA.")

scheduler_manager = SchedulerManager()