import datetime
import time
import pymongo
import config
import json
import csv
import GeoIP
import requests
from celery.signals import task_postrun
from celery.utils.log import get_task_logger
from earlauto import celery, db
from earlauto.models import Visitor, Campaign, CampaignType, AppendedVisitor, Store, Lead, StoreDashboard, \
    GlobalDashboard, CampaignDashboard
from sqlalchemy import and_, between
from sqlalchemy import exc, func
from sqlalchemy import text
from io import StringIO
from datetime import timedelta

# set up our logger utility
logger = get_task_logger(__name__)

# Open the geo data file once and store it in cache memory
gi = GeoIP.open('/var/lib/geoip/GeoLiteCity.dat', GeoIP.GEOIP_INDEX_CACHE | GeoIP.GEOIP_CHECK_CACHE)


def convert_datetime_object(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def convert_utc_to_local(utcdate_obj):
    nowtimestamp = time.time()
    offset = datetime.datetime.fromtimestamp(nowtimestamp) - datetime.datetime.utcfromtimestamp(nowtimestamp)
    return utcdate_obj + offset


def get_location(ip_addr):
    gi_lookup = gi.record_by_addr(ip_addr)
    return gi_lookup


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


@celery.task(queue='get_visitors')
def get_new_visitors():
    """
    Get the list of all unprocessed new visitors from MongoDB and set up for
    processing into MySQL database
    :return: mark_complete
    """

    # set date vars
    current_time = datetime.datetime.now()
    one_day_ago = current_time - datetime.timedelta(hours=24)

    # connect to MongoDB
    try:

        client = pymongo.MongoClient(config.DevelopmentConfig.MONGO_SERVER, 27017)
        mongodb = client[config.DevelopmentConfig.MONGO_DB]

        # query the sent collection for new IP's
        sent_collection = mongodb.sent_collection
        data = sent_collection.find({'processed': 0}, {'_id': 1, 'ip': 1, 'agent': 1, 'send_hash': 1,
                                                       'job_number': 1, 'client_id': 1, 'sent_date': 1,
                                                       'campaign_hash': 1, 'open_hash': 1, 'send_hash': 1}).limit(100)
        data_count = data.count(True)

        # if data has new visitors to process
        if data:

            for item in data:
                m_id = item['_id']

                if item['ip'][-2:] == "::":
                    item['ip'] = "0.0.0.0"
                elif len(item['ip']) > 15:
                    str_ip = item['ip'].split(',')
                    item['ip'] = str_ip[0]

                # check the IP against the local MySQL database
                # if visitor_exists returns True, increment the
                # counter and skip creating the new visitor
                # mysql db contains indexes created for ip_index
                # and campaign_hash_index
                visitor_exists = Visitor.query.filter(and_(
                    Visitor.ip == item['ip'],
                    Visitor.campaign_hash == item['campaign_hash'],
                    Visitor.created_date <= current_time
                )).first()

                if visitor_exists:
                    visitor_exists.num_visits += 1
                    visitor_exists.last_visit = datetime.datetime.now()
                    db.session.commit()

                    # if the visitor already existed and te number of visits was incremented,
                    # continue to update the processed flag for the IPs
                    # update the processed flag in MongoDB and set to True
                    sent_collection.update_one({'_id': m_id}, {'$set': {'processed': 1}}, True)
                    logger.info('Visitor exists. {} has visited {} times.  Last Visit: {}'.format(
                        visitor_exists.id,
                        visitor_exists.num_visits,
                        visitor_exists.last_visit
                    ))

                    # print incrementing message to the console
                    print('Incrementing Visitor Counter!  Next >>')

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
                    sent_date = datetime.datetime.now()
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

                    try:

                        campaign = Campaign.query.filter(and_(
                            Campaign.job_number == job_number,
                            Campaign.client_id == client_id
                        )).first()

                        # check to see if we have a valid campaign
                        if campaign:

                            try:
                                # create the new visitor obj
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
                                    traffic_type='',
                                    retry_counter=0,
                                    last_retry=datetime.datetime.now(),
                                    status='NEW'
                                )

                                # add the new visitor and commit
                                db.session.add(new_visitor)
                                db.session.commit()

                                # update the processed flag in MongoDB and set to True
                                sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

                                # log the result
                                logger.info('Visitor from {} created for Store ID: {} for Campaign: {}. '.format(
                                    new_visitor.ip,
                                    new_visitor.store_id,
                                    new_visitor.campaign_hash
                                ))

                                # next task >> append_visitor
                                append_visitor.delay(new_visitor.id)

                            # catch database exception, process mongo-db visitor
                            # record and update to continue processing
                            except exc.SQLAlchemyError as err:

                                # log the result
                                logger.info('Database returned: {}'.format(str(err)))

                                # update the processed flag in MongoDB and set to True to continue
                                sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

                        # campaign not found
                        else:
                            # log the result
                            logger.warning('Campaign not found for this visitor.  Task aborted.')

                            # update the processed flag in MongoDB and set to True to continue
                            sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

                    # catch database exception, process mongo-db visitor
                    # record and update to continue processing
                    except exc.SQLAlchemyError as err:

                        # log the result
                        logger.info('Database returned: {}'.format(str(err)))

                        # update the processed flag in MongoDB and set to True to continue
                        sent_collection.update_one({'_id': record_id}, {'$set': {'processed': 1}}, True)

            # return new visitor count to the console
            return data_count

        else:
            # Log a message to the console
            logger.info('There are zero new visitors waiting to be processed...')
            print('No new visitors.  Process exiting...')
            return 'No Records Found!'

    except pymongo.errors.ConnectionFailure as e:
        logger.warning('PyMongo returned database error: {}'.format(str(e)))
        print('Back in 3..2..1..')


@celery.task(queue='append_visitors', max_retries=3)
def append_visitor(new_visitor_id):
    """
    Send Visitors to M1 for Data Append
    :arg new_visitor_id
    :return: json
    """

    # create request headers
    hdr = {'user-agent': 'EARL Automation Server v.01', 'content-type': 'application/json'}
    retry_value = 3    

    # check to make sure 'new_visitor_id' is an integer
    # if not, convert to an integer
    if not isinstance(new_visitor_id, int):
        new_visitor_id = int(new_visitor_id)

    try:

        # get the visitor by the ID passed in the previous task
        get_visitor = Visitor.query.filter(
            Visitor.id == new_visitor_id
        ).one()

        # if the query returns True
        if get_visitor:

            # run through our local checks and make sure this
            # is a visitor record we want to append
            if get_visitor.retry_counter < retry_value:

                if not 'GeoIP Lookup failed' in get_visitor.country_name:

                    if 'US' in get_visitor.country_code:

                        if not get_visitor.appended:

                            try:
                                # get our campaign data for the M1 API call
                                campaign = Campaign.query.filter(and_(
                                    Campaign.client_id == get_visitor.client_id,
                                    Campaign.job_number == get_visitor.job_number
                                )).first()

                                if campaign:

                                    # we also need the store's zip code
                                    store = Store.query.filter(
                                        Store.id == campaign.store_id
                                    ).first()

                                    url = 'https://datamatchapi.com/DMSApiV2/DMSApi/GetDmsApiData?IP={}&Dealer=DMS&Client=DMS' \
                                          '&SubClient=Diamond-CRMDev&product=earl' \
                                          '&JobNumber={}&ClientID={}&VendorID=DMS&DaysToSuppress=0&Radius={}&ZipCode={}'\
                                        .format(
                                            get_visitor.ip,
                                            get_visitor.job_number,
                                            get_visitor.client_id,
                                            campaign.radius,
                                            store.zip_code
                                        )

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
                                            state = json_obj[0]['State']
                                            city = json_obj[0]['City']
                                            zip_code = json_obj[0]['ZipCode']
                                            car_year = json_obj[0]['YEAR']
                                            car_model = json_obj[0]['MODEL']
                                            address = json_obj[0]['Address']
                                            zip4 = json_obj[0]['Zip4']
                                            phone = json_obj[0]['Cell']
                                            ppm_type = json_obj[0]['PPM_Type']
                                            ppm_indicator = json_obj[0]['PPM_Indicator']
                                            ppm_segment = json_obj[0]['PPM_Segment']
                                            auto_trans_date = json_obj[0]['First_Seen']
                                            last_seen = json_obj[0]['Last_Seen']
                                            birth_year = json_obj[0]['Birth_Year']
                                            income_range = json_obj[0]['Income_Range']
                                            home_owner_renter = json_obj[0]['Home_OwnerRenter']
                                            auto_purchase_type = json_obj[0]['Auto_Purchase_Type']
                                            
                                            # make sure the birth_year is an integer
                                            if birth_year != '':
                                                if not isinstance(birth_year, int):
                                                    birth_year = int(birth_year)
                                            else:
                                                birth_year = int(0)

                                            # create the appended visitor record and commit
                                            appended_visitor = AppendedVisitor(
                                                visitor=get_visitor.id,
                                                created_date=get_visitor.created_date,
                                                first_name=first_name.capitalize(),
                                                last_name=last_name.capitalize(),
                                                email=email,
                                                home_phone=phone,
                                                cell_phone=phone,
                                                address1=address,
                                                city=city.title(),
                                                state=state.upper(),
                                                zip_code=zip_code,
                                                zip_4=zip4,
                                                credit_range=credit_range,
                                                car_year=car_year,
                                                car_model=car_model.title(),
                                                car_make=car_make.capitalize(),
                                                processed=False,
                                                ppm_type=ppm_type,
                                                ppm_indicator=ppm_indicator,
                                                ppm_segment=ppm_segment,
                                                auto_trans_date=auto_trans_date,
                                                last_seen=last_seen,
                                                birth_year=birth_year,
                                                income_range=income_range,
                                                home_owner_renter=home_owner_renter,
                                                auto_purchase_type=auto_purchase_type
                                            )

                                            db.session.add(appended_visitor)
                                            db.session.commit()

                                            # update the visitor instance with the appended flag
                                            get_visitor.appended = True
                                            get_visitor.processed = True
                                            get_visitor.locked = True
                                            get_visitor.status = 'APPENDED'
                                            db.session.commit()

                                            # log the result
                                            logger.info('Visitor Appended: {} {} {} {} {} @ {}'.format(
                                                first_name.title(),
                                                last_name.title(),
                                                city.title(),
                                                state.upper(),
                                                zip_code,
                                                get_visitor.ip
                                            ))

                                            # next task >> create_lead
                                            create_lead.delay(appended_visitor.id)

                                        else:
                                            # update the visitor instance with the appended flag False
                                            # and the processed flag to True.  Did not append.
                                            get_visitor.appended = False
                                            get_visitor.processed = True
                                            get_visitor.locked = True
                                            get_visitor.status = 'IPNOTFOUND'
                                            db.session.commit()

                                            # log the result
                                            logger.warning('No match on IP: {}'.format(get_visitor.ip))

                                    elif r.status_code == 404:
                                        get_visitor.retry_counter += 1
                                        get_visitor.last_retry = datetime.datetime.now()
                                        get_visitor.status = 'ERROR404'
                                        get_visitor.locked = False
                                        db.session.commit()

                                        # log the result
                                        logger.warning('M1 404 Response: Page Not Found/Data Malformed.')
                                        print('M1 Returned 404:  Will retry Visitor ID: {} @ IP: {} next round.'.format(
                                            get_visitor.id, get_visitor.ip
                                        ))

                                    elif r.status_code == 503:
                                        get_visitor.retry_counter += 1
                                        get_visitor.last_retry = datetime.datetime.now()
                                        get_visitor.status = 'ERROR503'
                                        get_visitor.locked = False
                                        db.session.commit()

                                        # log the result
                                        logger.critical(
                                            'M1 503 Response:  Critical.  Service Unavailable.  Re-queue Task')

                                    else:
                                        # process the record
                                        get_visitor.processed = True
                                        get_visitor.locked = False
                                        get_visitor.status = 'HTTPERROR'
                                        db.session.commit()

                                        # log the result
                                        logger.critical(
                                            'Unknown HTTP Status Code Returned from M1. Re-queue Task')

                                else:
                                    # update the record
                                    get_visitor.processed = True
                                    get_visitor.locked = True
                                    get_visitor.status = 'NOCAMPAIGN'
                                    db.session.commit()

                                    # log the result
                                    logger.warning('No campaign found for client_id: {} and job_number: {}'.format(
                                        get_visitor.client_id,
                                        get_visitor.job_number
                                    ))

                            except exc.SQLAlchemyError as err:
                                logger.warning('The database returned error: {}'.format(str(err)))

                        else:
                            # process the visitor record
                            get_visitor.processed = True
                            get_visitor.locked = True
                            get_visitor.status = 'APPENDED'
                            db.session.commit()

                            # log the result
                            logger.info('Visitor ID: {} is already appended.  Task aborted!')

                    else:
                        # process the visitor record
                        get_visitor.processed = True
                        get_visitor.locked = True
                        get_visitor.status = 'FOREIGNIP'
                        db.session.commit()

                        # log the result
                        logger.info('Visitor ID: {} geo-located outside the U.S.  Task aborted.'.format(
                            get_visitor.id
                        ))
                else:
                    # process the visitor record
                    get_visitor.processed = True
                    get_visitor.locked = True
                    get_visitor.status = 'NOGEODATA'
                    db.session.commit()

                    # log the result
                    logger.warning('Visitor ID: {} did not Geo-Locate.  IP skipped...'.format(
                        get_visitor.id
                    ))

            else:
                # log the result
                logger.info('Visitor ID: {} has exceeded the M1 appended retry ceiling.  Task aborted!'.format(
                    get_visitor.id
                ))

        else:
            logger.info('Visitor ID: {} not found.  Query returned None.  Task aborted!')
            print('There were no records to process so I\'m going back to sleep...')

    except exc.SQLAlchemyError as e:
        logger.warning('Database returned error: {}'.format(str(e)))


@celery.task(queue='create_leads', max_retries=3)
def create_lead(appended_visitor_id):
    """
    Create the lead from the appended visitor data
    :return: num_leads
    """

    visitor_id = appended_visitor_id
    lead_counter = 0

    # make sure visitor_id in an integer
    # if not, convert to integer
    if not isinstance(visitor_id, int):
        visitor_id = int(appended_visitor_id)

    # get the 'appended_visitor' record
    try:
        appended_visitor = AppendedVisitor.query.filter(
            AppendedVisitor.id == visitor_id
        ).one()

        visitor = Visitor.query.filter(
            Visitor.id == appended_visitor.visitor
        ).one()

        campaign = Campaign.query.filter(
            Campaign.id == visitor.campaign_id
        ).one()

        # check campaign status, if not active, skip creating leads
        if campaign.status == 'ACTIVE':

            # check to ensure the query returned a valid record
            if appended_visitor:

                # create the new lead record
                new_lead = Lead(
                    appended_visitor_id=appended_visitor.id,
                    created_date=datetime.datetime.now(),
                    email_verified=False,
                    lead_optout=False,
                    processed=False,
                    followup_email=False,
                    followup_print=False,
                    followup_voicemail=False
                )

                db.session.add(new_lead)
                db.session.commit()

                # log the result
                logger.info('Lead created: {} {} Email: {} on {}'.format(
                    appended_visitor.first_name,
                    appended_visitor.last_name,
                    appended_visitor.email,
                    datetime.datetime.now()
                ))

                # next tasks >> send_lead_to_dealer, send_auto_adf_lead
                send_lead_to_dealer.delay(new_lead.id)
                send_auto_adf_lead.delay(new_lead.id)

            else:
                # log the result
                logger.info('Appended Visitor: {} was not found in the database.  Task aborted')

        else:
            # log the result
            logger.warning('Campaign {} is INACTIVE.  Not sending.  Goodbye and good riddance...'.format(campaign.id))

        # return the task argument 'visitor ID' to the console
        return visitor_id

    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='verify_leads', max_retries=3)
def verify_lead(new_lead_id):
    """
    Perform email validation with Kickbox
    :return: verified email
    """
    # https://api.kickbox.io/v2/verify?email=' + lead.email + '&apikey=' + kickbox_api_key
    newleadid = new_lead_id
    kickbox_api_key = 'test_b2a8972a20c5dafd8b08f6b1ebb323d6660db597fc8fde74e247af7e03776e19'
    kickbox_base_url = 'https://api.kickbox.io/v2/verify?email='

    hdr = {
        'user-agent': 'EARL Automation Server v.01',
        'content-type': 'application/json'
    }

    if not isinstance(newleadid, int):
        newleadid = int(newleadid)

    try:

        # get our leads to process and verify
        newlead = Lead.query.filter(
            Lead.id == newleadid
        ).one()

        if newlead:

            if newlead.lead_optout:

                # lead has already opted out
                # no need to verify this email
                logger.info('The lead email has already been opted-out.  Task aborted!')

            elif newlead.email_verified:

                # the email address has already been verified
                logger.info('The lead email has already been verified.  Task aborted!')

            elif newlead.followup_email:

                # the email has been verified
                # the follow up email sent
                # why is this task even here
                logger.info('The lead follow up email has already been sent.  Task aborted!')

            else:

                try:

                    visitor_data = AppendedVisitor.query.filter(
                        AppendedVisitor.id == newlead.appended_visitor_id
                    ).one()

                    if visitor_data:

                        if visitor_data.email:

                            # set up the remaining part of the url string
                            email_url = visitor_data.email + '&apikey=' + kickbox_api_key

                            # call Kickbox Service to verify the email
                            r = requests.get(kickbox_base_url + email_url, headers=hdr)

                            # if we have a good HTTP status
                            if r.status_code == 200:

                                # set our response variable
                                kickbox_response = r.json()

                                if 'deliverable' in kickbox_response['result']:

                                    # update and mark the lead processed
                                    newlead.email_verified = True
                                    newlead.processed = True
                                    db.session.commit()

                                    # log the result
                                    logger.info('Lead ID: {} email addresses was validated as deliverable'.format(
                                        newlead.id
                                    ))

                                    # call the next task in the process
                                    send_followup_email.delay(newlead.id)

                                else:
                                    # lead email is undeliverable
                                    # update and mark the lead processed
                                    newlead.email_verified = False
                                    newlead.processed = True
                                    db.session.commit()

                                    # log the result
                                    logger.info('Lead ID: {} email address not verified.'.format(newlead.id))

                        # the lead has no usable email address
                        # send to web scraping, maybe in another process
                        else:
                            newlead.email_verified = False
                            newlead.processed = True
                            db.session.commit()

                            # log the result
                            logger.warning('Lead ID: {} has no usable email address.  Can not verify!'.format(
                                newlead.id
                            ))

                    else:
                        # log the result
                        logger.warning('The visitor query returned None.  The visitor attached to '
                                       'Lead ID: {} not found.  Task aborted!'.format(newlead.id))

                # we got a database error
                except exc.SQLAlchemyError as db_err:
                    print('Database returned error: {}'.format(db_err))
                    logger.critical('Database error, aborting process...')

            # the return value for the celery console
            # this is always the return ID value of the task
            return newlead.id

        else:
            # log the result
            logger.info('Lead not found.  Task aborted.')

    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_leads', max_retries=3)
def send_lead_to_dealer(lead_id):
    """
    Send the New Qualified Lead to the Dealer
    :param lead_id:
    :return: MG response
    """
    mailgun_url = 'https://api.mailgun.net/v3/mail.earlbdc.com/messages'
    mailgun_sandbox_url = 'https://api.mailgun.net/v3/sandbox3b609311624841c0bb2f9154e41e34de.mailgun.org/messages'
    mailgun_apikey = 'key-dfd370f4412eaccce27394f7bceaee0e'

    # check to make sure 'lead_id' is an integer
    # if not, convert to integer
    if not isinstance(lead_id, int):
        lead_id = int(lead_id)

    try:
        # get our lead
        verified_lead = Lead.query.filter(
            Lead.id == lead_id
        ).one()

        # make sure we have a good verified lead
        if verified_lead:

            # we already sent this one to the dealer, why are we seeing it again?
            if verified_lead.sent_to_dealer:

                # log the result
                logger.info('Lead ID: {} has already been sent to the dealer.  Task aborted!'.format(
                    verified_lead.id))

            elif verified_lead.email_receipt_id and verified_lead.email_validation_message:

                # log the result
                logger.info('Lead ID: {} was already sent.  Email Receipt: {}'.format(
                    verified_lead.id,
                    verified_lead.email_receipt_id
                ))

            else:

                # do some raw sql to get the store notification email and the campaign name
                sql = text('select l.id as lead_id, c.id as campaign_id, c.name, c.status, s.id as store_id, '
                           's.notification_email, av.first_name, av.last_name, av.email, av.home_phone, av.address1, '
                           'av.address2, av.city, av.state, av.zip_code, av.zip_4, av.credit_range, av.car_year, '
                           'av.car_make, av.car_model, ct.name as tactic '
                           'from leads l, campaigns c, stores s, appendedvisitors av, visitors v, campaigntypes ct '
                           'where l.appended_visitor_id = av.id '
                           'and av.visitor = v.id and v.store_id = s.id '
                           'and v.campaign_id = c.id '
                           'and c.type = ct.id '
                           'and l.id = {}'.format(verified_lead.id))

                # we have a good result
                result = db.engine.execute(sql).fetchone()

                if result[3] == 'ACTIVE':

                    if result[5] and result[2]:

                        text_body = "New EARL Lead:\n\n Name: " + str(result[6]) + " " + str(result[7]) \
                                    + "\n" \
                                    + "Email: " + str(result[8]) + "\n" \
                                    + "Phone #: " + str(result[9]) + "\n" \
                                    + "Street Address: " + str(result[10]) + "\n" \
                                    + "City, State, Zip: " \
                                    + str(result[12]) + ", " + str(result[13]) + " " + str(result[14]) \
                                    + "\n\n" \
                                    + "Credit Range: " + str(result[16]) + "\n" \
                                    + "Auto Details: " \
                                    + str(result[17]) + " " + str(result[18]) + " " + str(result[19]) \
                                    + "\n" \
                                    + "Campaign: " + str(result[2])

                        payload = {
                            "from": "EARL Automation<earl@earlbdc.com>",
                            "to": result[5],
                            "bcc": "earl-email-validation@contactdms.com",
                            "subject": result[2] + ' - ' + result[20],
                            "text": text_body,
                            "o:tracking": "False",
                        }

                        # call mailgun and post the data payload
                        try:
                            r = requests.post(mailgun_url, auth=('api', mailgun_apikey), data=payload)

                            # we have a good HTTP response
                            if r.status_code == 200:
                                mg_response = r.json()

                                if 'id' in mg_response:
                                    verified_lead.processed = True
                                    verified_lead.sent_to_dealer = True
                                    verified_lead.email_receipt_id = mg_response['id']
                                    verified_lead.email_validation_message = mg_response['message']
                                    db.session.commit()

                                    # log the result
                                    logger.info('Lead ID: {} email sent to {} on {}'.format(
                                        verified_lead.id,
                                        result[4],
                                        datetime.datetime.now().strftime('%c')
                                    ))

                                    # call the next task in the workflow
                                    verify_lead.delay(verified_lead.id)

                            # we did not get a valid HTTP response
                            else:
                                # do we want to continue to re-try this task
                                verified_lead.sent_to_dealer = False
                                verified_lead.email_receipt_id = 'HTTP Error: {}'.format(r.status_code)
                                verified_lead.email_validation_message = 'NOT SENT'
                                db.session.commit()

                                # log the result
                                logger.warning('Did not receive a valid HTTP Response from Mailgun.  Will retry...')
                                print('MailGun Response: {}'.format(r.content))

                        # got an exception from requests
                        except requests.HTTPError as http_err:

                            # log the result
                            logger.warning('MailGun communication error: {}'.format(http_err))

                    else:

                        # log the result
                        logger.warning('SQL Query failed to get store and campaign data.')

                else:

                    # log the result
                    logger.info('Campaign {} is INACTIVE.  Not sending.  '
                                'Goodbye and good riddance...'.format(result[1]))

            # set the return value for the console
            return verified_lead.id

        else:
            # no lead id matching the query
            logger.info('Verified Lead ID: {} not found.  Task aborted!'.format(lead_id))

        # return lead ID to the console
        return lead_id

    except exc.SQLAlchemyError as err:
        logger.info('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_adfs', max_retries=3)
def send_auto_adf_lead(lead_id):
    """
    Send the ADF for Dealers using this feature
    :param lead_id:
    :return: lead_id
    """
    r = None
    mailgun_url = 'https://api.mailgun.net/v3/mail.earlbdc.com/messages'
    mailgun_sandbox_url = 'https://api.mailgun.net/v3/sandbox3b609311624841c0bb2f9154e41e34de.mailgun.org/messages'
    mailgun_apikey = 'key-dfd370f4412eaccce27394f7bceaee0e'

    if not isinstance(lead_id, int):
        lead_id = int(lead_id)

    try:
        # get our lead
        lead = Lead.query.filter(
            Lead.id == lead_id
        ).one()

        if lead:

            # make sure we did not already send this adf
            if lead.sent_adf:

                # log this to the console and figure out why this task is here
                logger.info('ADF already sent for Lead ID: {} Task Aborted!')

            else:

                # do some raw sql to get the store notification email and the campaign name
                sql = text(
                    'select l.id, c.id, c.name, c.type, s.id as store_id, s.name as store_name, s.adf_email, av.first_name, '
                    'av.last_name, av.address1, av.city, av.state, av.zip_code, av.email, av.home_phone, av.credit_range, '
                    'av.car_year, av.car_model, av.car_make '
                    'from leads l, campaigns c, stores s, appendedvisitors av, visitors v where l.appended_visitor_id = av.id '
                    'and av.visitor = v.id and v.store_id = s.id and v.campaign_id = c.id and l.id = {}'.format(lead.id)
                )

                # we have a good result, fetch the record
                result = db.engine.execute(sql).fetchone()

                # result is True
                if result:

                    campaign_type_id = result[3]
                    campaign_type = CampaignType.query.filter(
                        CampaignType.id == campaign_type_id
                    ).one()

                    # check for required lead data - first, last, phone and email
                    adf_fields = [str(result[7]), str(result[8]), str(result[13]), str(result[14])]
                    has_adf_fields = all(adf_fields)
                    adf_store_id = result[4]

                    # the store must have a valid ADF email address
                    if result[6]:

                        # some store adf_email fields contain none as a string. quick fix.
                        if result[6] != 'None':

                            # create the payload
                            payload = {
                                "from": "EARL ADF Lead <mailgun@earlbdc.com>",
                                "to": str(result[6]),
                                "cc": "earl-email-validation@contactdms.com",
                                "subject": str(result[5]) + ' ' + campaign_type.name + ' DMS XML Lead',
                                "text": "<?xml version='1.0' encoding='UTF-8'?>" +
                                '<?ADF VERSION="1.0"?>' +
                                "<adf>" +
                                "<prospect>" +
                                "<requestdate>" + datetime.datetime.now().strftime("%x") + "</requestdate>" +
                                '<vehicle interest="trade-in" status="used">' +
                                '<id sequence="1" source="' + str(result[5]) + ' ' + campaign_type.name + ' DMS"></id>' +
                                "<year>" + str(result[16]) + "</year>" +
                                "<make>" + str(result[18]) + "</make>" +
                                "<model>" + str(result[17]) + "</model>" +
                                "</vehicle>" +
                                "<customer>" +
                                "<contact>" +
                                '<name part="full">' + str(result[7]) + ' ' + str(result[8]) + '</name>' +
                                '<address type="home">' +
                                "<street>" + str(result[9]) + "</street>" +
                                "<city>" + str(result[10]) + "</city>" +
                                "<regioncode>" + str(result[11]) + "</regioncode>" +
                                "<postalcode>" + str(result[12]) + "</postalcode>" +
                                "</address>" +
                                "<email>" + str(result[13]) + "</email>" +
                                "<phone>" + str(result[14]) + "</phone>" +
                                "</contact>" +
                                "<comments>Estimated Credit: " + str(result[15]) + "</comments>" +
                                "</customer>" +
                                '<vendor>' +
                                '<id source="' + str(result[5]) + ' DMS">' + str(result[5]) + ' ' + campaign_type.name + ' DMS</id>' +
                                "<vendorname>" + str(result[5]) + "</vendorname>" +
                                "<contact>" +
                                '<name part="full">' + str(result[5]) + '</name>' +
                                "</contact>" +
                                "</vendor>" +
                                "<provider>" +
                                '<name part="full">' + str(result[5]) + ' ' + campaign_type.name + ' DMS</name>' +
                                "<service>" + str(result[5]) + ' ' + campaign_type.name + " DMS</service>" +
                                "<url>None</url>" +
                                "</provider>" +
                                "<leadtype>digital plus</leadtype>" +
                                "</prospect>" +
                                "</adf>",
                                "o:tag": 'ADF CRM email',
                                "o:tracking": 'False',
                            }

                            # call M1 and send the email as plain ascii text
                            # test conditions for Lithia Chrysler
                            if adf_store_id == 34:
                                if has_adf_fields is True:

                                    # call mailgun and send adf for lithia stores if data requirements are met
                                    try:
                                        r = requests.post(mailgun_url, auth=('api', mailgun_apikey), data=payload)

                                    except requests.HTTPError as http_err:
                                        # log the event to the console
                                        logger.info('Mailgun returned HTTP error {} for '
                                                    'ADF email for Lead ID: {}'.format(str(http_err), str(lead_id)))

                                else:
                                    # set up a mailgun payload data obj
                                    data = {"from": "EARL ADF Lead <mailgun@earlbdc.com>",
                                            "to": "craigderington@opython-development-systems.com",
                                            "subject": "Lithia ADF - Lead Data Not Sent",
                                            "text": "Lead data for " + str(lead_id) + " was not sent to Lithia ADF"}

                                    # send the request to mailgun
                                    try:
                                        r = requests.post(mailgun_sandbox_url, auth=('api', mailgun_apikey), data=data)

                                        # log the output
                                        logger.info('Notice: Lithia Chrysler Dodge Jeep Anchorage - Missing '
                                                    'Required Lead Fields for Store ID: {}'.format(adf_store_id))

                                    except requests.HTTPError as http_err:
                                        # log the event
                                        logger.info('Mailgun returned HTTP error {} for '
                                                    'ADF email for Lead ID: {}'.format(str(http_err), str(lead_id)))

                            else:
                                # send ADF email for all other stores
                                try:
                                    r = requests.post(mailgun_url, auth=('api', mailgun_apikey), data=payload)

                                except requests.HTTPError as http_err:
                                    # log the event
                                    logger.info('Mailgun returned HTTP error {} for '
                                                'ADF email for Lead ID: {}'.format(str(http_err), str(lead_id)))

                            # check the response code
                            if r.status_code == 200:

                                # assign the response a variable
                                mg_response = r.json()

                                if 'id' in mg_response:
                                    lead.sent_adf = True
                                    lead.adf_email_receipt_id = mg_response['id']
                                    lead.adf_email_validation_message = mg_response['message']
                                    db.session.commit()

                                    # log the result
                                    logger.info('ADF email sent to {} for Lead ID: {}'.format(
                                        result[5], lead.id
                                    ))

                                    if adf_store_id == 34:
                                        if has_adf_fields is True:
                                            # log this extra result to monitor in the console
                                            logger.info('Lithia Chrysler Store.  '
                                                        'Check Required Fields: {}'.format(adf_fields))

                            # we did not get a valid HTTP response
                            else:
                                # do we want to continue to re-try this task
                                lead.sent_adf = False
                                lead.adf_email_receipt_id = 'HTTP Error: {}'.format(r.status_code)
                                lead.adf_email_validation_message = 'NOT SENT'
                                db.session.commit()

                                # log the result
                                logger.warning('Lead ID: {} ADF email send returned an '
                                               'HTTP Error {}.'.format(lead.id, r.status_code))

                        # the store adf email is None.  Skip the ADF email
                        else:
                            logger.info('Store ID: {} has no ADF email configured.  '
                                        'Task aborted'.format(result.store_id))

                    # the store does not have an adf email configured.  can not send
                    else:
                        logger.info('Store ID: {} has no ADF email configured.  Task aborted'.format(result.store_id))

                # the query on the lead details returned None
                else:
                    logger.info('Unable to retrieve lead details for Lead ID: {}.  Task aborted'.format(lead.id))

            # return the lead id to the console
            return lead.id

        # the database can not find this record
        else:
            logger.info('Lead ID: {} was not found.  Task aborted!'.format(lead_id))

    # log the database error
    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_followups', max_retries=3)
def send_followup_email(lead_id):
    """
    Send the Lead Follow Up Email Including Campaign Creative
    :param lead_id:
    :return: mailgun response obj
    """

    mailgun_url = 'https://api.mailgun.net/v3/mail.earlbdc.com/messages'
    mailgun_sandbox_url = 'https://api.mailgun.net/v3/sandbox3b609311624841c0bb2f9154e41e34de.mailgun.org/messages'
    mailgun_apikey = 'key-dfd370f4412eaccce27394f7bceaee0e'

    if not isinstance(lead_id, int):
        lead_id = int(lead_id)

    # let's try and get some data, shall we...
    try:
        lead = Lead.query.filter(
            Lead.id == lead_id
        ).one()

        # ok, do we have a lead object?
        if lead:

            # did we verify the email address
            if lead.email_verified:

                # if so, let get the appended visitor data too
                av = AppendedVisitor.query.filter(
                    AppendedVisitor.id == lead.appended_visitor_id
                ).one()

                # do we have an appended visitor?
                if av:

                    # does this lead have a verified email address
                    if av.email:

                        # great, get the rest of the data
                        visitor = Visitor.query.filter(
                            Visitor.id == av.visitor
                        ).one()

                        campaign = Campaign.query.filter(
                            Campaign.id == visitor.campaign_id
                        ).one()

                        store = Store.query.filter(
                            Store.id == campaign.store_id
                        ).one()

                        # sanity check
                        if campaign.status == 'ACTIVE':

                            # make sure we have creative
                            if campaign.creative_header and campaign.creative_footer:

                                if campaign.creative_header != 'Enter Header':

                                    creative_header = campaign.creative_header
                                    creative_footer = campaign.creative_footer
                                    body_text = str(av.first_name + ' ' + av.last_name)
                                    html = creative_header + body_text + creative_footer
                                    payload = {
                                        "from": "EARL Automation <mailgun@earlbdc.com>",
                                        "to": av.email,
                                        "subject": campaign.email_subject,
                                        "html": html,
                                        "o:tracking": "True"
                                    }

                                    # post the request to mailgun
                                    try:

                                        # make the call
                                        r = requests.post(mailgun_url, auth=('api', mailgun_apikey), data=payload)

                                        # process the response
                                        if r.status_code == 200:
                                            mg_response = r.json()

                                            if 'id' in mg_response:
                                                lead.processed = True
                                                lead.followup_email = True
                                                lead.followup_email_receipt_id = mg_response['id']
                                                lead.followup_email_status = mg_response['message']
                                                lead.followup_email_sent_date = datetime.datetime.now()
                                                db.session.commit()

                                                # log the result
                                                logger.info('Lead ID: {} email sent to {} on {}'.format(
                                                    lead.id,
                                                    body_text,
                                                    datetime.datetime.now().strftime('%c')
                                                ))

                                                # send the rvm
                                                send_rvm.delay(lead_id)

                                        # we did not get a valid HTTP response
                                        else:
                                            # do we want to continue to re-try this task
                                            lead.followup_email = False
                                            lead.followup_email_receipt_id = 'HTTP Error: {}'.format(r.status_code)
                                            lead.followup_email_status = 'NOT SENT'
                                            db.session.commit()

                                            # log the result
                                            logger.warning('Did not receive a valid HTTP Response from Mailgun.  '
                                                           'Will retry in 5, 4, 3, 2, 1...')
                                            print('MailGun Response: {}'.format(r.content))

                                    # got an exception from requests
                                    except requests.HTTPError as http_err:

                                        # log the result
                                        logger.warning('MailGun communication error: {}'.format(http_err))

                                # default values for creative
                                else:
                                    # log the result
                                    logger.warning('Campaign ID: {} contains default creative.  '
                                                   'Task Aborted!'.format(campaign.id))

                            # no campaign creative
                            else:
                                # log the result
                                logger.warning('Campaign ID: {} does not have campaign creative.  '
                                               'Task Aborted!'.format(campaign.id))

                        # campaign is not active
                        else:
                            # log the result
                            logger.warning('Campaign {} is INACTIVE.  Who is in charge here?  Task Aborted!')

                    # no valid email address
                    else:
                        # log the result
                        logger.warning('Lead ID: {} does not have a valid and verified email address.  '
                                       'Task Aborted!'.format(lead_id))

                # no appended visitor record found, abort
                else:
                    # log the result
                    logger.warning('Lead ID: {} has no related appended visitor to process this task.  '
                                   'Task Aborted!'.format(lead_id))

            # lead email not verified
            else:
                # log the result
                logger.warning('Lead ID: {} email address has not yet been verified.  '
                               'Airdropping to Verify Leads Task Queue...'.format(lead_id))

                # airdrop
                verify_lead.delay(lead_id)

        # no lead found
        else:
            # log the result
            logger.info('Lead ID: {} was not found.  Task Aborted!'.format(lead_id))

    # ouch, hard database error.  he is...down... for... the ... count.
    # it's all over ladies and gentlemen.
    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_rvms', max_retries=3)
def send_rvm(lead_id):
    """
    Send the Ringless Voicemail
    :param lead_id:
    :return:
    """
    dialercentral_url = 'https://webhooks.ivr-platform.com/rvm/ondemand'
    dialercentral_debug_url = 'https://webhooks-ivr--platform-com-ejkcinv1h9fx.runscope.net/rvm/ondemand'

    hdr = {
        'user-agent': 'EARL Automation v.01',
        'content-type': 'application/json'
    }

    # RVM settings
    rvm_limit = 0
    rvm_send_count = 0
    rvm_campaign_id = 0
    current_time = datetime.datetime.now()

    if not isinstance(lead_id, int):
        lead_id = int(lead_id)

    try:
        # queries
        lead = Lead.query.filter(
            Lead.id == lead_id
        ).one()

        av = AppendedVisitor.query.filter(
            AppendedVisitor.id == lead.appended_visitor_id
        ).one()

        visitor = Visitor.query.filter(
            Visitor.id == av.visitor
        ).one()

        campaign = Campaign.query.filter(
            Campaign.id == visitor.campaign_id
        ).one()

        # do we have a lead?
        if lead:

            # ok, then do we have an appended visitor?
            if av:

                # assign the appended visitor a cell phone variable
                rvm_phone = av.cell_phone

                # cool, does this lead object has
                # a cell phone number in this system?
                if rvm_phone:

                    # is there a campaign?
                    if campaign:

                        # set campaign RVM vars
                        rvm_limit = campaign.rvm_limit
                        rvm_campaign_id = campaign.rvm_campaign_id
                        rvm_send_count = campaign.rvm_send_count

                        if rvm_campaign_id:

                            # sanity check
                            if campaign.status == 'ACTIVE' and rvm_campaign_id != 0:

                                # dude, here, lift this
                                payload = {
                                    "webhook_key": "84ea4ff53a8e4a16a4f985e74ff4f547",
                                    "rvm_id": rvm_campaign_id,
                                    "scrub_nat_dnc": "true",
                                    "lead": {
                                        "lead_phone": rvm_phone
                                    }
                                }

                                # post the data payload to the dialer central service
                                r = requests.post(dialercentral_url, headers=hdr, data=json.dumps(payload))

                                # hey look, we got a response from Dialer Central
                                if r.status_code == 201:

                                    # let us format the response
                                    resp = r.json()
                                    str_resp = resp['result']
                                    result = str(str_resp).encode('utf-8')

                                    # is the response object a dict?
                                    if isinstance(resp, dict):

                                        # set up the results to commit
                                        lead.rvm_status = result
                                        lead.rvm_date = datetime.datetime.now()
                                        lead.rvm_message = rvm_phone
                                        lead.rvm_sent = True

                                        # commit to the database
                                        db.session.commit()

                                        # increment the rvm_counter
                                        campaign.rvm_send_count += 1
                                        db.session.commit()

                                        # log the result
                                        logger.info('Lead ID: {} was sent RVM ID: {} to {} on {}'.format(
                                            lead.id, rvm_campaign_id, rvm_phone, current_time
                                        ))

                                # uh oh, we got a DNC response
                                elif r.status_code == 403:

                                    resp = r.json()
                                    msg = str(resp['Error']).encode('utf-8')

                                    if isinstance(resp, dict):
                                        lead.rvm_status = 'ERROR'
                                        lead.rvm_message = msg
                                        lead.rvm_date = datetime.datetime.now()

                                        # commit to the database
                                        db.session.commit()

                                        # log the result
                                        logger.warning('Lead ID: {} was found in the National DO NOT CALL registry.  '
                                                       'I give up.'.format(lead_id))

                                # something is missing or invalid rvm_campaign_id
                                elif r.status_code == 404:

                                    resp = r.json()
                                    result = str(resp['Error']).encode('utf-8')

                                    if isinstance(resp, dict):

                                        # set up the results to commit
                                        lead.rvm_status = 'ERROR'
                                        lead.rvm_message = result
                                        lead.rvm_date = datetime.datetime.now()

                                        # commit to the database
                                        db.session.commit()

                                        # log the result
                                        logger.warning('The RVM Campaign ID was not found. Who is in charge here?  '
                                                       'Should I retry?')

                                # dialer central sent a http response
                                # that I do not understand
                                else:
                                    # log the result and retry
                                    logger.info('Dialer Central sent {} response to our request for Lead ID: {}.  '
                                                'Will retry...'.format(r.status_code, lead_id))
                        else:
                            # log the result
                            logger.info('Campaign {} does not have a valid RVM Campaign ID.  '
                                        'Task Aborted!'.format(campaign.id))
                    else:
                        # log the result
                        logger.info('Campaign not found.  Task Aborted!')

                else:
                    # log the result
                    logger.info('Lead ID: {} does not have a cell phone to send a RVM.  '
                                'Bye, Felicia...'.format(lead_id))

            else:
                # log the result
                logger.warning('I could not find any appended visitor data, so, I give up...  Task Aborted!')

        else:
            # log the result
            logger.info('Lead ID: {} was not found in the database.  Task Aborted!'.format(lead_id))

    # ouch, database error
    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='append_visitors', max_retries=3)
def resend_http_errors():
    """
    Resend any Visitor records that returned Data Provider
    HTTP Errors during append_visitor function
    Timedelta: 12 hours ago
    :return: list
    """
    current_time = datetime.datetime.now()
    twelve_hours_ago = current_time - datetime.timedelta(hours=12)
    visitor_counter = 0
    retry_ceiling = 3

    # get our list of visitors that did not append
    # due to http error 404 or 503 at data provider
    try:

        visitors = Visitor.query.filter(and_(
            Visitor.status.contains('ERROR'),
            Visitor.locked == False,
            Visitor.last_retry > twelve_hours_ago,
            Visitor.retry_counter <= retry_ceiling
        )).all()

        # check to see if we have a valid visitors object

        if visitors:

            # loop the visitors list

            for visitor in visitors:

                # add the visitor ID to the append_visitor task queue
                append_visitor.delay(visitor.id)

                visitor.status = None
                visitor.last_retry = datetime.datetime.now()
                db.session.commit()

                # log the result
                logger.info('Visitor ID: {} was airdropped back into the append_visitor task queue.'.format(
                    visitor.id
                ))

                # increment the counter
                visitor_counter += 1

            # return the number of records to the console
            return visitor_counter

        else:
            # log the result
            logger.info('There are zero M1 HTTP errors records to send.  Back to sleep for 12 hours.  '
                        'Good Morning & Good Night.')

    except exc.SQLAlchemyError as err:

        # log the result
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_leads', max_retries=3)
def resend_leads_to_dealer():
    """
    Resend any Dealer Leads that resulted in an HTTP Error
    :return: list
    """

    current_time = datetime.datetime.now()
    four_hours_ago = current_time - datetime.timedelta(hours=4)
    lead_counter = 0

    try:

        # get the leads to resend
        leads = Lead.query.filter(and_(
            Lead.email_validation_message.contains('NOT SENT'),
            Lead.email_receipt_id.contains('HTTP Error')
        )).all()

        # check to make sure we have a valid data object
        if leads:

            # loop the list
            for lead in leads:

                # send the lead id back into the send_leads queue
                if not lead.sent_to_dealer:

                    # add to the queue
                    send_lead_to_dealer.delay(lead.id)

                    # reset the fields
                    lead.email_receipt_id = None
                    lead.email_validation_message = None
                    db.session.commit()

                    # log the result
                    logger.info('Lead ID: {} was airdropped back into the send leads queue.'.format(lead.id))

                    # increment the counter
                    lead_counter += 1

                else:

                    # log the result
                    logger.warning('Lead ID: {} email already sent to the dealer.  Task aborted!'.format(lead.id))

            # return the count to the console
            return lead_counter

        else:

            # log the result
            logger.warning('There are zero HTTP Mail Error leads to re-add to the queue')

    except exc.SQLAlchemyError as err:

        # log the result
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='send_rvms', max_retries=3)
def resend_rvm_http_errors():
    pass


@celery.task(queue='reports', max_retries=3)
def deactivate_campaign(campaign_id):
    """
    Mark Active Campaign INACTIVE upon campaign end/expiration date
    :param campaign_id:
    :return: none
    """
    # check our campaign ID
    if not isinstance(campaign_id, int):
        campaign_id = int(campaign_id)

    try:
        campaign = Campaign.query.filter(
            Campaign.id == campaign_id
        ).one()

        # campaign found
        if campaign:
            campaign.status = 'INACTIVE'
            db.session.commit()

            # log the result
            logger.info('Campaign {} was set to Inactive with an expiration date of {}.'.format(
                campaign.id, campaign.end_date
            ))

        # campaign id not found
        else:

            # log the result
            logger.info('Campaign {} was not found or has already been deactivated...'.format(campaign_id))

    # database exception
    except exc.SQLAlchemyError as err:

        # log the result
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='reports', max_retries=3)
def send_daily_recap_report(campaign_id):
    """
    Generate Daily Recap Report for each dealer by campaign
    :return: csv file
    """
    # mailgun
    mailgun_url = 'https://api.mailgun.net/v3/mail.earlbdc.com/messages'
    mailgun_sandbox_url = 'https://api.mailgun.net/v3/sandbox3b609311624841c0bb2f9154e41e34de.mailgun.org/messages'
    mailgun_apikey = 'key-dfd370f4412eaccce27394f7bceaee0e'

    # set up our report params
    if not isinstance(campaign_id, int):
        campaign_id = int(campaign_id)

    current_day = datetime.datetime.now()
    one_day_ago = current_day - timedelta(days=1)
    yesterday = one_day_ago.strftime('%Y-%m-%d')
    start_date = datetime.datetime.strptime(yesterday + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(yesterday + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
    rows = []

    try:

        # get our store campaigns
        campaign = Campaign.query.filter(
            Campaign.id == campaign_id
        ).one()

        if campaign:

            # get me the store
            store = Store.query.filter(
                Store.id == campaign.store_id
            ).one()

            # set up some vars we need
            campaign_id = campaign.id
            campaign_name = campaign.name
            campaign_type = campaign.campaign_type
            job_number = campaign.job_number
            store_name = store.name
            store_reporting_email = store.reporting_email

            if not store_reporting_email or store_reporting_email == "None":
                store_reporting_email = "earl-email-validation@contactdms.com"

            report_date = yesterday
            msg_subject = str(store_name) + " " + str(campaign_type) + " Daily Recap Report for " + str(report_date)
            msg_body_text = str(store_name) + ' ' + str(campaign_name) + ' ' + str(campaign_type)\
                            + " Daily Visitors recap report for " + str(yesterday)

            # execute the query and set the results
            results = Visitor.query.join(AppendedVisitor, Visitor.id == AppendedVisitor.visitor)\
                .add_columns(AppendedVisitor.created_date, AppendedVisitor.first_name, AppendedVisitor.last_name,
                             AppendedVisitor.address1, AppendedVisitor.city, AppendedVisitor.state,
                             AppendedVisitor.zip_code, AppendedVisitor.email, AppendedVisitor.cell_phone,
                             AppendedVisitor.credit_range, AppendedVisitor.car_year, AppendedVisitor.car_make,
                             AppendedVisitor.car_model)\
                .filter(
                    Visitor.campaign_id == campaign_id,
                    Visitor.created_date.between(start_date, end_date)
                ).all()

            if results:
                for result in results:
                    row = []
                    row.append(result.created_date)
                    row.append(result.first_name)
                    row.append(result.last_name)
                    row.append(result.address1)
                    row.append(result.city)
                    row.append(result.state)
                    row.append(result.zip_code)
                    row.append(result.email)
                    row.append(result.cell_phone)
                    row.append(result.credit_range)
                    row.append(result.car_year)
                    row.append(result.car_make)
                    row.append(result.car_model)
                    rows.append(row)

                # set the header row
                si = StringIO()
                row_heading = []
                row_heading.append('Created Date')
                row_heading.append('First Name')
                row_heading.append('Last Name')
                row_heading.append('Address')
                row_heading.append('City')
                row_heading.append('State')
                row_heading.append('ZipCode')
                row_heading.append('Email')
                row_heading.append('Phone')
                row_heading.append('Credit Range')
                row_heading.append('Auto Year')
                row_heading.append('Auto Make')
                row_heading.append('Auto Model')

                writer = csv.writer(si)
                writer.writerow(row_heading)

                # loop the rows and write the data
                for row in rows:
                    writer.writerow(row)

                csv_content = si.getvalue().strip('\r\n')

                # name the file
                report_file_name = '{}-{}-Daily-Recap-Report-{}.csv'.format(campaign.name, campaign_type, report_date)
                report_data = csv_content

                # set up mailgun payload
                payload = {
                    "from": "EARL Automation Server v.01 <mailgun@earlbdc.com>",
                    "to": [store_reporting_email],
                    "bcc": "earl-email-validation@contactdms.com",
                    "subject": msg_subject,
                    "text": msg_body_text
                }

                # let's call mailgun and chat
                try:
                    r = requests.post(mailgun_url,
                                      auth=('api', mailgun_apikey),
                                      files={
                                          "attachment": (report_file_name, report_data)
                                      },
                                      data=payload)

                    # mg response is OK!
                    if r.status_code == 200:
                        # log the result
                        logger.info('Campaign {}-{}-{} was just sent the daily '
                                    'recap report'.format(campaign_id, campaign_type, campaign_name))

                    # oh, no, we got an error sending the report
                    else:
                        logger.info('There was an error sending the '
                                    'daily recap report for {}: {}'.format(campaign_id, campaign_name))

                # we got an http error, that's bad news.
                except requests.HTTPError as http_err:
                    # log the result
                    logger.warning('Mailgun returned HTTP Error Code: {}'.format(http_err))

            else:
                # log the result
                logger.warning('Campaign {} has no visitors to report.  '
                               'Task Aborted!'.format(campaign_name))
        else:
            # log the result
            logger.info('Campaign {} not found.  Task aborted!')

    # we need to talk cause we got bigger problems now
    except exc.SQLAlchemyError as err:

        # log the result
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='reports', max_retries=3)
def get_recap_report_campaigns():
    """
    Generate the list of active campaigns and send them to the Recap Report task queue
    :return: active campaign list
    """
    # do some date stuff
    current_time = datetime.datetime.now()
    one_day_ago = current_time - timedelta(days=1)
    yesterday = one_day_ago.strftime('%Y-%m-%d')
    report_date = datetime.datetime.strptime(yesterday + ' 23:59:59', '%Y-%m-%d %H:%M:%S')

    # get a list of active campaigns
    active_campaigns = Campaign.query.filter(
        Campaign.status == 'ACTIVE'
    ).all()

    # awesome.  we haz campaigns.  please continue...
    if active_campaigns:

        # set the count variable
        campaign_count = len(active_campaigns)

        # loop over our list
        for campaign in active_campaigns:
            send_daily_recap_report(campaign.id)

        # log the result
        logger.info('EARL Automation airdropped {} active campaigns '
                    'into the Send Daily Recap Task Queue'.format(campaign_count))

    else:
        # log the result
        logger.info('No active campaigns to air drop to task queue...  Task Aborted!')


@celery.task(queue='reports', max_retries=3)
def get_expired_campaigns():
    """
    Generate a list of active campaigns by ID and send into the Mark Campaign Inactive
    :return: list of active campaign ID's
    """
    campaign_count = 0

    try:
        # do some date stuff
        current_time = datetime.datetime.now()
        str_date = current_time.strftime('%Y-%m-%d')
        report_date = datetime.datetime.strptime(str_date, '%Y-%m-%d')

        # get a list of active campaigns
        active_campaigns = Campaign.query.filter(
            Campaign.status == 'ACTIVE',
            Campaign.end_date < report_date
        ).all()

        # we have active campaigns to update
        if active_campaigns:

            for campaign in active_campaigns:
                deactivate_campaign.delay(campaign.id)
                campaign_count += 1

            # log the result
            logger.info('EARL Automation airdropped {} active campaigns to '
                        'get deactivated.'.format(str(campaign_count)))

        # no active campaigns
        else:
            # log the result
            logger.info('No activate campaigns needs to be deactivated...  Task aborted!')

    # log the result
    except exc.SQLAlchemyError as err:
        logger.warning('Database returned error: {}'.format(str(err)))


@celery.task(queue='stores', max_retries=3)
def update_store_dashboard(store_id):
    """
    Update the store dashboard
    :param store_id:
    :return: None
    """
    current_day = datetime.datetime.now()
    last_updated = (datetime.datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')
    gl_append_rate = 0.00
    uq_append_rate = 0.00
    us_append_rate = 0.00
    total_appends = 0
    total_rvms = 0
    total_rtns = 0
    global_visitors = 0
    total_campaigns = 0
    active_campaigns = 0
    unique_visitors = 0
    dashboard_total_global = 0
    dashboard_total_unique = 0
    dashboard_total_us = 0
    dashboard_total_appends = 0
    dashboard_total_rtns = 0
    dashboard_total_followup_emails = 0
    dashboard_total_rvms = 0

    # check the instance of store_id
    if not isinstance(store_id, int):
        store_id = int(store_id)

    # ok, get the store
    try:
        store = Store.query.filter(
            Store.id == store_id
        ).one()

        # do we have an active store
        if store.status == 'ACTIVE' and store.archived == 0:

            total_campaigns = Campaign.query.filter(Campaign.store_id == store.id).count()

            active_campaigns = Campaign.query.filter(
                Campaign.store_id == store.id,
                Campaign.status == 'ACTIVE'
            ).count()
            
            if active_campaigns > 0:

                dashboard = StoreDashboard.query.filter(
                    StoreDashboard.store_id == store.id
                ).order_by(StoreDashboard.id.desc()).limit(1).one()

                if dashboard:
                    if dashboard.last_update is not None:
                        last_updated = dashboard.last_update
                        dashboard_total_global = dashboard.total_global_visitors
                        dashboard_total_unique = dashboard.total_unique_visitors
                        dashboard_total_us = dashboard.total_us_visitors
                        dashboard_total_appends = dashboard.total_appends
                        dashboard_total_rtns = dashboard.total_sent_to_dealer
                        dashboard_total_followup_emails = dashboard.total_sent_followup_emails
                        dashboard_total_rvms = dashboard.total_rvms_sent

                # start counting visitors since last update
                stmt0 = text("select sum(num_visits) as global_visitors "
                             "from visitors v "
                             "where v.store_id={} "
                             "and v.created_date > '{}'".format(store.id, last_updated))

                global_visitors = db.session.query('global_visitors').from_statement(stmt0).all()

                unique_visitors = Visitor.query.filter(
                    Visitor.store_id == store.id,
                    Visitor.created_date > last_updated
                ).count()

                us_visitors = Visitor.query.filter(
                    Visitor.store_id == store.id,
                    Visitor.created_date > last_updated,
                    Visitor.country_code == 'US'
                ).count()

                total_appends = Visitor.query.join(
                    AppendedVisitor, Visitor.id == AppendedVisitor.visitor
                ).filter(Visitor.store_id == store.id, Visitor.created_date > last_updated).count()

                stmt1 = text("SELECT count(l.id) as total_rtns "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.store_id={} "
                             "and v.created_date > '{}' "
                             "and l.sent_to_dealer=1".format(store.id, last_updated))
                stmt2 = text("SELECT count(l.id) as total_followup_emails "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.store_id={} "
                             "and v.created_date > '{}' "
                             "and l.followup_email=1".format(store.id, last_updated))
                stmt3 = text("SELECT count(l.id) as total_rvms "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.store_id={} "
                             "and v.created_date > '{}' "
                             "and l.rvm_sent=1".format(store.id, last_updated))

                # set the values
                total_rtns = db.session.query('total_rtns').from_statement(stmt1).all()
                total_followup_emails = db.session.query('total_followup_emails').from_statement(stmt2).all()
                total_rvms = db.session.query('total_rvms').from_statement(stmt3).all()

                # calc the rates
                if total_appends > 0:
                    total_global_visitors = int(global_visitors[0][0]) + int(dashboard_total_global)
                    total_unique_visitors = int(unique_visitors) + int(dashboard_total_unique)
                    total_us_visitors = int(us_visitors) + int(dashboard_total_us)
                    total_total_appends = int(total_appends) + int(dashboard_total_appends)
                    total_sent_to_dealer = int(total_rtns[0][0]) + int(dashboard_total_rtns)
                    total_sent_followup_emails = int(total_followup_emails[0][0]) + int(dashboard_total_followup_emails)
                    total_sent_rvms = int(total_rvms[0][0]) + int(dashboard_total_rvms)
                    gl_append_rate = float(int(total_total_appends) / int(total_global_visitors) * 100.0)
                    uq_append_rate = float(int(total_total_appends) / int(total_unique_visitors) * 100.0)
                    us_append_rate = float(int(total_total_appends) / int(total_us_visitors) * 100.0)

                    try:

                        new_dashboard = StoreDashboard(
                            store_id=store.id,
                            total_campaigns=total_campaigns,
                            active_campaigns=active_campaigns,
                            total_global_visitors=total_global_visitors,
                            total_unique_visitors=total_unique_visitors,
                            total_us_visitors=total_us_visitors,
                            total_appends=total_total_appends,
                            total_sent_to_dealer=total_sent_to_dealer,
                            total_sent_followup_emails=total_sent_followup_emails,
                            total_rvms_sent=total_sent_rvms,
                            global_append_rate=gl_append_rate,
                            unique_append_rate=uq_append_rate,
                            us_append_rate=us_append_rate,
                            last_update=current_day
                        )

                        db.session.add(new_dashboard)
                        db.session.commit()

                        # log the result
                        logger.info('Store: {} Dealer Dashboard was updated at {}.'.format(store.name, current_day))

                    # log the exception
                    except exc.SQLAlchemyError as err:
                        logger.info('Database returned error: {}'.format(str(err)))
                        
            else:
                # log the result
                logger.warning('Store: {} has no active campaigns.  Task aborted.'.format(store_id))

        else:
            # log the result
            logger.warning('Store {} not active or archived.  Task Aborted!'.format(store_id))

    except exc.SQLAlchemyError as err:
        logger.info('Database returned error: {}'.format(str(err)))
    
    # return the store ID to the console
    return store_id


@celery.task(queue='stores', max_retries=3)
def get_stores_for_dashboard():
    """
    Generate a list of store ID's and update the dashboard
    :return: none
    """
    store_count = 0

    try:
        stores = Store.query.filter(
            Store.status == 'ACTIVE',
            Store.archived == 0
        ).all()

        if stores:
            for store in stores:
                update_store_dashboard.delay(store.id)
                store_count += 1

            logger.info('EARL Automation airdropped {} stores to the task queue to update the store dashboard.'.format(
                store_count
            ))

        else:
            # log the result
            logger.info('Notice: No active stores for dashboard update...')

    except exc.SQLAlchemyError as err:
        logger.info('Database returned error: {}'.format(str(err)))

    # return the store_count to the console
    return store_count


@celery.task(queue='reports', max_retries=3)
def update_global_dashboard():
    """
    Update the global dashboard on a scheduled interval.
    Every 2 hours to start
    Create a new record each time to maintain historical dashboard
    :return: none
    """
    current_time = datetime.datetime.now()
    total_global_new_visitors = 0
    formatted_datetime = current_time.strftime('%Y-%m-%d %H:%M:%S')

    # run the dashboard queries

    try:
        current_dashboard = GlobalDashboard.query.order_by(
            GlobalDashboard.id.desc()
        ).limit(1).one()

        if current_dashboard:
            dashboard_lastupdated = current_dashboard.last_update
        else:
            dashboard_lastupdated = datetime.datetime.now() - timedelta(hours=2)
            dashboard_lastupdated = datetime.datetime.strftime(dashboard_lastupdated, "%Y-%m-%d %H:%M:%S")

        total_stores = Store.query.count()
        active_stores = Store.query.filter(Store.status == 'ACTIVE').count()
        total_campaigns = Campaign.query.count()
        active_campaigns = Campaign.query.filter(Campaign.status == 'ACTIVE').count()

        stmt0 = text("select sum(v.num_visits) as global_visitors "
                     "from visitors v "
                     "where v.created_date > '{}'".format(dashboard_lastupdated))

        # start counting
        global_visitors = db.session.query('global_visitors').from_statement(stmt0).all()

        if global_visitors[0][0] is None:
            total_global_new_visitors = 0
        else:
            total_global_new_visitors = int(global_visitors[0][0])

        unique_visitors = Visitor.query.filter(
            Visitor.created_date > dashboard_lastupdated
        ).count()

        us_visitors = Visitor.query.filter(
            Visitor.created_date > dashboard_lastupdated,
            Visitor.country_code == 'US'
        ).count()

        total_appends = AppendedVisitor.query.filter(
            AppendedVisitor.created_date > dashboard_lastupdated
        ).count()

        total_rtns = Lead.query.filter(
            Lead.created_date > dashboard_lastupdated,
            Lead.sent_to_dealer == 1
        ).count()

        total_followup_emails = Lead.query.filter(
            Lead.created_date > dashboard_lastupdated,
            Lead.followup_email == 1
        ).count()

        total_rvms = Lead.query.filter(
            Lead.created_date > dashboard_lastupdated,
            Lead.rvm_sent == 1,
            Lead.rvm_status == 'LOADED'
        ).count()

        # calc the percentages
        total_global_visitors = int(current_dashboard.total_global_visitors) + total_global_new_visitors
        total_unique_visitors = int(current_dashboard.total_unique_visitors) + int(unique_visitors)
        total_us_visitors = int(current_dashboard.total_us_visitors) + int(us_visitors)
        total_appends = int(current_dashboard.total_appends) + int(total_appends)
        total_sent_to_dealer = int(current_dashboard.total_sent_to_dealer) + int(total_rtns)
        total_sent_followup_emails = int(current_dashboard.total_sent_followup_emails) + int(total_followup_emails)
        total_rvms_sent = int(current_dashboard.total_rvms_sent) + int(total_rvms)

        global_append_rate = float((total_appends / total_global_visitors) * 100.0)
        unique_append_rate = float((total_appends / total_unique_visitors) * 100.0)
        us_append_rate = float((total_appends / total_us_visitors) * 100.0)

        # add a new dashboard record
        dashboard = GlobalDashboard(
            total_stores=total_stores,
            active_stores=active_stores,
            total_campaigns=total_campaigns,
            active_campaigns=active_campaigns,
            total_global_visitors=total_global_visitors,
            total_unique_visitors=total_unique_visitors,
            total_us_visitors=total_us_visitors,
            total_appends=total_appends,
            total_sent_to_dealer=total_sent_to_dealer,
            total_sent_followup_emails=total_sent_followup_emails,
            total_rvms_sent=total_rvms_sent,
            global_append_rate=global_append_rate,
            unique_append_rate=unique_append_rate,
            us_append_rate=us_append_rate,
            last_update=current_time
        )

        # commit to the database
        db.session.add(dashboard)
        db.session.commit()

        # log the result
        logger.info('Global dashboard was updated on: {}'.format(str(formatted_datetime)))

    # database exception
    except exc.SQLAlchemyError as err:
        # log the result
        logger.info('Database returned error: {}'.format(str(err)))


@celery.task(queue='campaigns', max_retries=3)
def update_campaign_dashboard(campaign_id):
    """
    Update the Campaign dashboard
    :param campaign_id:
    :return: None
    """
    current_day = datetime.datetime.now()
    append_rate = 0.00
    total_appends = 0
    total_rvms = 0
    total_rtns = 0
    us_visitors = 0

    # check the instance of campaign_id
    if not isinstance(campaign_id, int):
        campaign_id = int(campaign_id)

    # ok, get the campaign - active campaigns only
    try:
        campaign = Campaign.query.filter(
            Campaign.id == campaign_id
        ).one()

        # do we have a valid campaign
        if campaign:

            # check the status
            if campaign.status == 'ACTIVE' and campaign.archived == 0:

                campaign_dashboard = CampaignDashboard.query.filter(
                    CampaignDashboard.campaign_id == campaign_id
                ).order_by(CampaignDashboard.id.desc()).limit(1).one()

                if campaign_dashboard:
                    dashboard_lastupdated = campaign_dashboard.last_update
                    total_campaign_appends = campaign_dashboard.total_appends
                    total_campaign_rvms = campaign_dashboard.total_rvms
                    total_campaign_rtns = campaign_dashboard.total_rtns
                    total_campaign_us_visitors = campaign_dashboard.total_visitors
                    total_campaign_followup_emails = campaign_dashboard.total_followup_emails

                    if not dashboard_lastupdated:
                        dashboard_lastupdated = datetime.datetime.now() - timedelta(hours=4)
                        dashboard_lastupdated = datetime.datetime.strftime(dashboard_lastupdated, "%Y-%m-%d %H:%M:%S")

                else:
                    dashboard_lastupdated = datetime.datetime.now() - timedelta(hours=4)
                    dashboard_lastupdated = datetime.datetime.strftime(dashboard_lastupdated, "%Y-%m-%d %H:%M:%S")
                    total_campaign_appends = 0
                    total_campaign_rvms = 0
                    total_campaign_rtns = 0
                    total_campaign_us_visitors = 0
                    total_campaign_followup_emails = 0

                # start counting
                us_visitors = Visitor.query.filter(
                    Visitor.campaign_id == campaign.id,
                    Visitor.created_date > dashboard_lastupdated,
                    Visitor.country_code == 'US'
                ).count()

                total_appends = Visitor.query.join(
                    AppendedVisitor, Visitor.id == AppendedVisitor.visitor
                ).filter(Visitor.campaign_id == campaign.id, Visitor.created_date > dashboard_lastupdated).count()

                stmt1 = text("SELECT count(l.id) as total_rtns "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.campaign_id={} "
                             "and v.created_date > '{}' "
                             "and l.sent_to_dealer=1".format(campaign.id, dashboard_lastupdated))

                stmt2 = text("SELECT count(l.id) as total_followup_emails "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.campaign_id={} "
                             "and v.created_date > '{}' "
                             "and l.followup_email=1".format(campaign.id, dashboard_lastupdated))

                stmt3 = text("SELECT count(l.id) as total_rvms "
                             "from visitors v, appendedvisitors av, leads l where v.id = av.visitor "
                             "and l.appended_visitor_id = av.id "
                             "and v.campaign_id={} "
                             "and v.created_date > '{}' "
                             "and l.rvm_sent=1".format(campaign.id, dashboard_lastupdated))

                # set the values
                total_rtns = db.session.query('total_rtns').from_statement(stmt1).all()
                total_followup_emails = db.session.query('total_followup_emails').from_statement(stmt2).all()
                total_rvms = db.session.query('total_rvms').from_statement(stmt3).all()

                # calc the rates
                if total_appends > 0:
                    total_visitors = int(us_visitors) + int(total_campaign_us_visitors)
                    total_appends = int(total_appends) + int(total_campaign_appends)
                    total_rtns = int(total_rtns[0][0]) + int(total_campaign_rtns)
                    total_followup_emails = int(total_followup_emails[0][0]) + int(total_campaign_followup_emails)
                    total_rvms = int(total_rvms[0][0]) + int(total_campaign_rvms)

                    if total_visitors > 0:
                        append_rate = float(int(total_appends) / int(total_visitors) * 100.0)

                    try:

                        new_dashboard = CampaignDashboard(
                            store_id=campaign.store_id,
                            campaign_id=campaign.id,
                            total_visitors=total_visitors,
                            total_appends=total_appends,
                            total_rtns=total_rtns,
                            total_followup_emails=total_followup_emails,
                            total_rvms=total_rvms,
                            append_rate=append_rate,
                            last_update=current_day
                        )

                        db.session.add(new_dashboard)
                        db.session.commit()

                        # log the result
                        logger.info('Campaign {} Dealer Dashboard was updated at {}.'.format(campaign.name, current_day))

                    # log the exception
                    except exc.SQLAlchemyError as err:
                        logger.info('Database insert record returned an error: {}'.format(str(err)))

            else:
                # log the result
                logger.warning('Campaign: {} NOT ACTIVE.  Task aborted.'.format(str(campaign_id)))

        else:
            # log the result
            logger.info('Campaign {} Not Found.  Task Aborted!'.format(campaign_id))

    except exc.SQLAlchemyError as err:
        logger.info('Database error getting Campaign: {}'.format(str(err)))

    # return the campaign ID to the console
    return campaign_id


@celery.task(queue='campaigns', max_retries=3)
def get_campaigns_for_dashboard():
    """
    Get the Active Campaigns to Send to the Campaign Dashboard Task
    :return: none
    """
    current_date = datetime.datetime.now()
    str_date = current_date.strftime('%Y-%m-%d')
    report_date = datetime.datetime.strptime(str_date + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
    campaign_count = 0

    # get a list of active campaigns
    try:
        campaigns = Campaign.query.filter(
            Campaign.status == 'ACTIVE',
            Campaign.end_date >= report_date
        ).all()

        # ok, we have a queryset object
        if campaigns:

            # loop over the queryset and call the dashboard function
            for campaign in campaigns:
                update_campaign_dashboard.delay(campaign.id)
                campaign_count += 1

            # log the result
            logger.info('EARL Automation airdropped {} campaigns into the '
                        'Campaign Dashboard Task.'.format(str(campaign_count)))

        else:
            # log the result
            logger.info('There were zero active campaigns to call for a Dashboard update.  Task aborted!')

    except exc.SQLAlchemyError as err:
        # log the result
        logger.info('Database returned error: {}'.format(str(err)))

    # return campaign count to the console
    return campaign_count


@celery.task(queue='reports', max_retries=3)
def admin_campaign_report():
    """
    Return a CSV of the admin campaign report and send to EARL Validation Email
    :return: mailgun response
    """
    # mailgun
    mailgun_url = 'https://api.mailgun.net/v3/mail.earlbdc.com/messages'
    mailgun_sandbox_url = 'https://api.mailgun.net/v3/sandbox3b609311624841c0bb2f9154e41e34de.mailgun.org/messages'
    mailgun_apikey = 'key-dfd370f4412eaccce27394f7bceaee0e'

    current_day = datetime.datetime.now()
    one_day_ago = current_day - timedelta(days=1)
    yesterday = one_day_ago.strftime('%Y-%m-%d')
    start_date = datetime.datetime.strptime(yesterday + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(yesterday + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
    # start_date = '2018-03-30 00:00:00'
    # end_date = '2018-03-30 23:59:59'
    rows = []

    try:
        stmt1 = text("select s.name as store_name, c.job_number, c.name as campaign_name, "
                     "sum(v.num_visits) as total_visitors, "
                     "count(av.id) as total_appends "                    
                     "from visitors v, appendedvisitors av, leads l, campaigns c, stores s "
                     "where v.id = av.visitor "
                     "and av.id = l.appended_visitor_id "
                     "and v.campaign_id = c.id "
                     "and v.store_id = s.id "
                     "and c.status = '{}' "
                     "and (v.created_date between '{}' and '{}') "
                     "group by s.name, c.job_number, c.name "
                     "ORDER BY s.name, c.job_number, c.name asc".format('ACTIVE', start_date, end_date))

        results = db.session.query('store_name', 'job_number', 'campaign_name',
                                   'total_visitors', 'total_appends').from_statement(stmt1).all()

        if results:
            """
            for result in results:
                print(result.job_number, result.campaign_name, result.total_visitors, result.total_appends)

            else:
                print('No results...')
            """

            for result in results:
                row = []
                row.append(result.store_name)
                row.append(result.job_number)
                row.append(result.campaign_name)
                row.append(result.total_visitors)
                row.append(result.total_appends)
                rows.append(row)

            # set the header row
            si = StringIO()
            row_heading = []
            row_heading.append('Store Name')
            row_heading.append('Tactic Number')
            row_heading.append('Job Number')
            row_heading.append('Total Visitors')
            row_heading.append('Total Appends')

            writer = csv.writer(si)
            writer.writerow(row_heading)

            for row in rows:
                writer.writerow(row)

            csv_content = si.getvalue().strip('\r\n')

            # name the file
            report_file_name = 'Admin-Campaign-Daily-Report-{}.csv'.format(yesterday)
            report_data = csv_content

            # setup email vars
            msg_subject = 'Admin Campaign Daily Report for {}'.format(str(yesterday))
            msg_body_text = 'Please see attached Admin Campaign Daily Report for {}'.format(str(yesterday))

            # set up mailgun payload
            payload = {
                "from": "EARL Automation Server v.01 <mailgun@earlbdc.com>",
                "to": "earl-email-validation@contactdms.com",
                "bcc": "craigderington@python-development-systems.com",
                "subject": msg_subject,
                "text": msg_body_text
            }

            # let's call mailgun and chat
            try:
                r = requests.post(mailgun_url, auth=('api', mailgun_apikey),
                                  files={"attachment": (report_file_name, report_data)},
                                  data=payload)

                # mg response is OK!
                if r.status_code == 200:
                    # log the result
                    logger.info('The Admin Campaign Daily Report was sent on {}'.format(str(yesterday)))

                # oh, no, we got an error sending the report
                else:
                    logger.info('Mailgun returned: {}.  Will retry soon...'.format(str(r.status_code)))

            # we got an http error, that's bad news.
            except requests.HTTPError as http_err:
                # log the result
                logger.warning('Mailgun returned HTTP Error Code: {}'.format(http_err))

        else:
            # log the result
            logger.info('The Admin campaign Daily Report returned ZERO results.  Task aborted.')

    except exc.SQLAlchemyError as err:
        # log the result
        logger.info('Database returned error: {}'.format(str(err)))
