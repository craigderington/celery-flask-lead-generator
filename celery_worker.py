from celery import Celery
from celery.schedules import crontab
from earlauto import create_app
from earlauto.tasks import log, get_new_visitors, resend_http_errors, resend_leads_to_dealer, send_daily_recap_report


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
    # period task executes every 2.5 seconds
    sender.add_periodic_task(2.5, get_new_visitors, name='EARL Get New Visitors')

    # periodic task executes every 12 hours
    sender.add_periodic_task(43200.0, resend_http_errors, name='EARL Resend HTTP Errors')
    sender.add_periodic_task(43200.0, resend_leads_to_dealer, name='EARL Resend Leads to Dealer')

    # periodic task executes every night just after mid-midnight
    sender.add_periodic_task(
        crontab(hour=0, minute=2),
        send_daily_recap_report,
        name='Send the Daily Recap Report'
    )
