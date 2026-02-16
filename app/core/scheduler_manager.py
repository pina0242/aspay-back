from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from tzlocal import get_localzone
import logging

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
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
        # Si el job ya existía, lo quitamos para actualizarlo
        job_id = f"job_{task_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        trigger = CronTrigger.from_crontab(cron_str)
        self.scheduler.add_job(
            func, 
            trigger, 
            id=job_id, 
            name=name,
            replace_existing=True
        )
        logger.info(f"Job programado: {name} con cron [{cron_str}]")

# Instancia única para toda la app
scheduler_manager = SchedulerManager()