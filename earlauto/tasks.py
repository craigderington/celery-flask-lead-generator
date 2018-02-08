import datetime
import random
import time
import pymongo
import config
import json
from celery.signals import task_postrun
from celery.utils.log import get_task_logger

from earlauto import celery, db
from earlauto.models import Visitor, Campaign
from sqlalchemy import and_

logger = get_task_logger(__name__)


def convert_datetime_object(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}


@celery.task
def log(message):
    """Print some log messages"""
    logger.debug(message)
    logger.info(message)
    logger.warning(message)
    logger.error(message)
    logger.critical(message)


# @celery.task
def reverse_messages():
    """Reverse all messages in DB"""
    for message in Message.query.all():
        words = message.text.split()
        message_text = " ".join(reversed(words))
        reversed_message = Message(text=message_text)
        db.session.add(reversed_message)
        db.session.commit()


@task_postrun.connect
def close_session(*args, **kwargs):
    # Flask SQLAlchemy will automatically create new sessions for you from
    # a scoped session factory, given that we are maintaining the same app
    # context, this ensures tasks have a fresh session (e.g. session errors
    # won't propagate across tasks)
    db.session.remove()


@celery.task()
def get_new_visitors():
    """
    Get the list of all unprocessed new visitors from MongoDB and set up for
    processing into MySQL database
    :return: mark_complete
    """
    # connect to MongoDB
    try:

        client = pymongo.MongoClient(config.DevelopmentConfig.MONGO_SERVER, 27017)
        mongodb = client[config.DevelopmentConfig.MONGO_DB]

        # query the sent collection for new IP's
        sent_collection = mongodb.sent_collection
        data = sent_collection.find({'processed': 0}, {'_id': 1, 'ip': 1, 'agent': 1, 'send_hash': 1,
                                                       'job_number': 1, 'client_id': 1, 'sent_date': 1,
                                                       'campaign_hash': 1, 'open_hash': 1, 'send_hash': 1})

        if data:
            for item in data:
                record_id = item['_id']
                client_id = item['client_id']
                job_number = item['job_number']
                ip_addr = item['ip']
                agent = item['agent']
                sent_date = item['sent_date']
                campaign_hash = item['campaign_hash']
                open_hash = item['open_hash']
                send_hash = item['send_hash']
                raw_data = {
                    'client_id': client_id,
                    'job_number': job_number,
                    'ip_addr': ip_addr,
                    'agent': agent,
                    'sent_date': str(sent_date),
                    'campaign_hash': campaign_hash,
                    'open_hash': open_hash,
                    'send_hash': send_hash,
                    'appended': 0
                }

                raw_data = json.dumps(raw_data, default=convert_datetime_object)

                campaign = Campaign.query.filter(and_(
                    Campaign.job_number == job_number,
                    Campaign.client_id == client_id
                )).first()

                new_visitor = Visitor(
                    campaign_id=campaign.id,
                    store_id=campaign.store_id,
                    created_date=sent_date,
                    ip=ip_addr,
                    user_agent=agent,
                    job_number=job_number,
                    client_id=client_id,
                    open_hash=open_hash,
                    campaign_hash=campaign_hash,
                    send_hash=send_hash,
                    num_visits=1,
                    last_visit=sent_date,
                    raw_data=raw_data,
                    processed=False
                )
                db.session.add(new_visitor)
                db.session.commit()

                # update the processed flag in MongoDB and set to True
                sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

            return True

        else:
            # Log a message to the console
            message = "There are zero new visitors waiting to be processed..."
            logger.info(message)
            return 'No Records Found!'

    except pymongo.errors.ConnectionFailure as e:
        print('Could not connect to the Pixel Tracker MongoDB server: {}'.format(e))
