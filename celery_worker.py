from celery import Celery
from celery.schedules import crontab

from earlauto import create_app
from earlauto.tasks import log, reverse_messages, long_task


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
    # Calls reverse_messages every 10 seconds.
    sender.add_periodic_task(10.0, reverse_messages, name='Reverse database entry every 10 seconds.')

    # Calls log('Logging Stuff') every 30 seconds
    sender.add_periodic_task(30.0, log.s(('Logging Stuff')), name='Log the DEBUG output every 30 seconds.')

    sender.add_periodic_task(60.0, long_task, name='Log every 60 seconds.')

    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(hour=7, minute=30, day_of_week=1),
        log.s('Monday morning log!'),
    )
