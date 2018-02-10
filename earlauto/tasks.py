import datetime
import random
import time
import pymongo
import config
import json
import GeoIP
import requests
from celery.signals import task_postrun
from celery.utils.log import get_task_logger

from earlauto import celery, db
from earlauto.models import Visitor, Campaign, AppendedVisitor, Store
from sqlalchemy import and_
from sqlalchemy import exc

logger = get_task_logger(__name__)


def convert_datetime_object(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def get_location(ip_addr):
    gi = GeoIP.open('/var/lib/geoip/GeoLiteCity.dat', GeoIP.GEOIP_INDEX_CACHE | GeoIP.GEOIP_CHECK_CACHE)
    gi_lookup = gi.record_by_addr(ip_addr)
    return gi_lookup


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


@task_postrun.connect
def close_session(*args, **kwargs):
    # Flask SQLAlchemy will automatically create new sessions for you from
    # a scoped session factory, given that we are maintaining the same app
    # context, this ensures tasks have a fresh session (e.g. session errors
    # won't propagate across tasks)
    db.session.remove()


@celery.task(queue='visitors')
def get_new_visitors():
    """
    Get the list of all unprocessed new visitors from MongoDB and set up for
    processing into MySQL database
    :return: mark_complete
    """

    # set date vars
    current_time = datetime.datetime.now()
    one_day_ago = current_time - datetime.timedelta(hours=1)

    # connect to MongoDB
    try:

        client = pymongo.MongoClient(config.DevelopmentConfig.MONGO_SERVER, 27017)
        mongodb = client[config.DevelopmentConfig.MONGO_DB]

        # query the sent collection for new IP's
        sent_collection = mongodb.sent_collection
        data = sent_collection.find({'processed': 0}, {'_id': 1, 'ip': 1, 'agent': 1, 'send_hash': 1,
                                                       'job_number': 1, 'client_id': 1, 'sent_date': 1,
                                                       'campaign_hash': 1, 'open_hash': 1, 'send_hash': 1}).limit(10)
        data_count = data.count(True)

        # if data has new visitors to process
        if data:

            for item in data:
                m_id = item['_id']
                # check the IP against the local MySQL database
                # if visitor_exists returns True, increment the
                # counter and skip creating the new visitor
                # mysql db contains indexes created for ip_index
                # and campaign_hash_index
                visitor_exists = Visitor.query.filter(and_(
                    Visitor.ip == item['ip'],
                    Visitor.campaign_hash == item['campaign_hash'],
                    Visitor.created_date < one_day_ago
                )).first()

                if visitor_exists:
                    visitor_exists.num_visits += 1
                    visitor_exists.last_visit = datetime.datetime.now()
                    db.session.commit()

                # this IP and campaign_hash was not found
                # in the database, continue to process geoip
                else:

                    # get the geoip
                    geo_data = get_location(item['ip'])

                    if not geo_data:
                        geo_data = {
                            'country_name': 'GeoIP Lookup failed',
                            'city': 'Unknown',
                            'time_zone': 'Unknown',
                            'longitude': 0.00,
                            'latitude': 0.00,
                            'metro_code': 'Unknown',
                            'country_code': None,
                            'country_code3': None,
                            'dma_code': None,
                            'area_code': None,
                            'postal_code': None,
                            'region': 'Unknown',
                            'region_name': 'Unknown',
                            'traffic_type': 'Unknown'
                        }

                    # assign variables to items in data
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
                        processed=False,
                        country_name=geo_data['country_name'],
                        city=geo_data['city'],
                        time_zone=geo_data['time_zone'],
                        longitude=geo_data['longitude'],
                        latitude=geo_data['latitude'],
                        metro_code=geo_data['metro_code'],
                        country_code=geo_data['country_code'],
                        country_code3=geo_data['country_code3'],
                        dma_code=geo_data['dma_code'],
                        area_code=geo_data['area_code'],
                        postal_code=geo_data['postal_code'],
                        region=geo_data['region'],
                        region_name=geo_data['region_name'],
                        traffic_type=''
                    )
                    db.session.add(new_visitor)
                    db.session.commit()

                    # update the processed flag in MongoDB and set to True
                    sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

                # if the visitor already existed and te number of visits was incremented,
                # continue to update the processed flag for the IPs
                # update the processed flag in MongoDB and set to True
                sent_collection.update_one({'_id': m_id}, {'$set': {'processed': 1}}, True)

            return data_count

        else:
            # Log a message to the console
            logger.info('There are zero new visitors waiting to be processed...')
            print('No new visitors.  Process exiting...')
            return 'No Records Found!'

    except pymongo.errors.ConnectionFailure as e:
        logger.critical('Error connecting to MongoDB.  Send alerts.')
        print('Could not connect to the Pixel Tracker MongoDB server: {}'.format(e))


@celery.task(queue='append_visitors')
def append_visitors():
    """
    Send Visitors to M1 for Data Append
    :return: json
    """
    # create request headers
    hdr = {'user-agent': 'EARL Automation Server', 'content-type': 'application/json'}
    retry_value = 3

    try:

        visitors = Visitor.query.filter(and_(
            Visitor.processed == 0,
            Visitor.appended == 0,
            Visitor.country_code == 'US',
            Visitor.retry_counter < retry_value
        )).limit(10).all()

        # if the query returns True
        if visitors:

            for visitor in visitors:

                try:

                    # get our campaign data for the M1 API call
                    campaign = Campaign.query.filter(and_(
                        Campaign.client_id == visitor.client_id,
                        Campaign.job_number == visitor.job_number
                    )).first()

                    if campaign:

                        # we also need the store's zip code
                        store = Store.query.filter(
                            Store.id == campaign.store_id
                        ).first()

                        url = 'https://datamatchapi.com/DMSApi/GetDmsApiData?IP={}&Dealer=DMS&Client=DMS&SubClient=Diamond-CRMDev&product=earl' \
                              '&JobNumber={}&ClientID={}&VendorID=DMS&DaysToSuppress=0&Radius={}&ZipCode={}'.format(visitor.ip,
                                                                                                                    visitor.job_number,
                                                                                                                    visitor.client_id,
                                                                                                                    campaign.radius,
                                                                                                                    store.zip_code)

                        # make the M1 request
                        r = requests.get(url, headers=hdr)

                        # get the response and process appended visitors
                        if r.status_code == 200:
                            json_obj = json.loads(r.text)
                            # print(json_obj)

                            if isinstance(json_obj[0], dict) and 'FirstName' in json_obj[0]:
                                first_name = json_obj[0]['FirstName']
                                last_name = json_obj[0]['LastName']
                                email = json_obj[0]['EMail']
                                credit_range = json_obj[0]['InferredCreditScore']
                                car_make = json_obj[0]['MAKE']
                                state = json_obj[0]['state']
                                city = json_obj[0]['City']
                                zip_code = json_obj[0]['ZipCode']
                                car_year = json_obj[0]['YEAR']
                                car_model = json_obj[0]['MODEL']
                                address = json_obj[0]['Address']
                                zip4 = json_obj[0]['Zip4']
                                phone = json_obj[0]['Cell']

                                # create the appended visitor record and commit
                                appended_visitor = AppendedVisitor(
                                    visitor=visitor.id,
                                    created_date=visitor.created_date,
                                    first_name=first_name.capitalize(),
                                    last_name=last_name.capitalize(),
                                    email=email,
                                    home_phone=phone,
                                    cell_phone=phone,
                                    address1=address,
                                    city=city.title(),
                                    state=state.upper(),
                                    zip_code=zip_code,
                                    credit_range=credit_range,
                                    car_year=car_year,
                                    car_model=car_model.title(),
                                    car_make=car_make.capitalize(),
                                    processed=False
                                )

                                db.session.add(appended_visitor)
                                db.session.commit()

                                # update the visitor instance with the appended flag
                                visitor.appended = True
                                visitor.processed = True
                                visitor.status = 'APPENDED'
                                db.session.commit()
                                logger.info('Visitor Appended: {} {} {} {} {}'.format(
                                    first_name, last_name, city, state, zip_code
                                ))
                            else:
                                # update the visitor instance with the appended flag
                                visitor.appended = False
                                visitor.processed = True
                                visitor.status = 'IPNOTFOUND'
                                db.session.commit()
                                logger.warning('No match on IP: {}'.format(visitor.ip))

                        elif r.status_code == 404:
                            visitor.retry_counter += 1
                            visitor.last_retry = datetime.datetime.now()
                            visitor.status = 'ERROR404'
                            logger.warning('M1 404 Response: Page Not Found/Data Malformed.')
                            print('M1 Returned 404:  Will retry Visitor ID: {} @ IP: {} next round.'.format(
                                visitor.id, visitor.ip
                            ))
                        elif r.status_code == 503:
                            visitor.retry_counter += 1
                            visitor.last_retry = datetime.datetime.now()
                            visitor.status = 'ERROR503'
                            logger.critical('M1 503 Response:  Critical')
                            print('M1 Returned 503:  Service Unavailable')
                        else:
                            print('Did not receive a valid HTTP response code from M1.  Aborting.')

                    else:
                        logger.warning('No campaign found for client_id: {} and job_number: {}'.format(
                            Visitor.client_id,
                            Visitor.job_number
                        ))
                        print('Error:  Campaign Not Found!')

                except exc.SQLAlchemyError as err:
                    logger.warning('The database returned error: {}'.format(str(err)))

        else:
            logger.info('No new visitors to append.  Query returned None.')
            print('There were no records to process so I\'m going back to sleep...')

    except exc.SQLAlchemyError as e:
        print('The database returned error: {}'.format(str(e)))
