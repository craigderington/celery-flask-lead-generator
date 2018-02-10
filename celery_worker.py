from celery import Celery
from celery.schedules import crontab
from earlauto import create_app
from earlauto.tasks import log, get_new_visitors, append_visitors


def create_celery(app):
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery


flask_app = create_app()
celery = create_celery(flask_app)


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10.0, get_new_visitors, name='EARL Get New Visitors')
    sender.add_periodic_task(15.0, append_visitors, name='EARL Append Visitors')
    sender.add_periodic_task(30.0, log.s('Celery Heartbeat'), name='Celery Heartbeat')
    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=7, minute=30, day_of_week=1),
        log.s('Monday morning log!'),
    )
