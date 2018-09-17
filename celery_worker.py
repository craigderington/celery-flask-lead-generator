from celery import Celery
from celery.schedules import crontab
from earlauto import create_app
from earlauto.tasks import log, get_new_visitors, resend_http_errors, resend_leads_to_dealer, \
    get_recap_report_campaigns, get_stores_for_dashboard, update_global_dashboard, get_expired_campaigns, \
    get_campaigns_for_dashboard, admin_campaign_report


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

    # periodic task executes every 2 hours (7200)
    # periodic task executes every 4 hours (14400)
    # periodic task to execute every 6 hours (21600)
    # periodic tasks executes every 8 hours (28800)
    sender.add_periodic_task(28800.0, get_campaigns_for_dashboard, name='EARL Update Campaign Dashboards')

    # periodic task executes every 12 hours (43200)
    sender.add_periodic_task(43200.0, update_global_dashboard, name='EARL Update Global Dashboard')
    sender.add_periodic_task(43200.0, resend_http_errors, name='EARL Resend HTTP Errors')
    sender.add_periodic_task(43200.0, resend_leads_to_dealer, name='EARL Resend Leads to Dealer')

    # periodic task executes every 24 hours (86400)
    sender.add_periodic_task(86400.0, get_stores_for_dashboard, name='EARL Update Store Dashboards')

    # periodic task executes on crontab schedule
    sender.add_periodic_task(
        crontab(hour=0, minute=2),
        get_recap_report_campaigns,
        name='Get Active Campaigns and Send the Daily Recap Reports'
    )

    sender.add_periodic_task(
        crontab(hour=0, minute=5),
        admin_campaign_report,
        name='Send Admin Campaign Daily Report'
    )

    sender.add_periodic_task(
        crontab(hour=0, minute=10),
        get_expired_campaigns,
        name='Get Active Campaigns by End Date and Mark Inactive'
    )

