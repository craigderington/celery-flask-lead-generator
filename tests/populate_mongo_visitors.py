# coding: utf-8

import sys
import codecs
import csv
import datetime
import hashlib
import random
import pymongo
from bson import json_util
import json
import time


# import M1 data file with 10k IP

# define user agent strings for randomness
user_agent_list = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Googlebot/2.1 (+http://www.google.com/bot.html)',
]

ip_list = []
client_id_job_number_list = [
    {
        'client_id': 'pds74840t',
        'job_number': '39650',
        'campaign': 'b7875b4b'
    },
]


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def get_collection(collection):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client['earl-pixel-tracker']
    collection = db[collection]
    return collection


def main():
    """
    Populate the Mongo DB database to simulate real users
    :return: None
    """
    # set the counter to zero
    line_counter = 0

    # read in our sample data
    with open('data/M1-10K-IP-Sample-Data.csv') as csv_file:
        # assign variable to data from csv file
        reader = csv.reader(csv_file, delimiter=',')

        # skip header row
        next(reader)

        for row in reader:
            ip = row[0]
            ip_list.append(ip)

    for ip_addr in ip_list:
        # set variables from the lists above
        agent = random.choice(user_agent_list)
        job_number = client_id_job_number_list[0]['job_number']
        client_id = client_id_job_number_list[0]['client_id']
        campaign = client_id_job_number_list[0]['campaign']

        # create the visitor event record
        event_record = {
            'ip': ip_addr,
            'job_number': job_number,
            'client_id': client_id,
            'campaign': campaign,
            'opens': 0,
            'agent': agent,
            'processed': 0,
            'num_visits': 1
        }

        # create send, campaign and open hashes
        send_hash = hashlib.sha1('{}'.format(event_record).encode('utf-8')).hexdigest()
        campaign_hash = hashlib.sha1(event_record['campaign'].encode('utf-8')).hexdigest()
        open_hash = hashlib.sha1('{}:{}'.format(event_record['campaign'],
                                 event_record['job_number']).encode('utf-8')).hexdigest()

        # update the event record
        event_record['send_hash'] = send_hash
        event_record['campaign_hash'] = campaign_hash
        event_record['open_hash'] = open_hash
        event_record['sent_date'] = datetime.datetime.now()

        sent_collection = get_collection('sent_collection')
        visitor_exists = sent_collection.find_one({'ip': ip, 'send_hash': send_hash})

        if visitor_exists is None:
            new_record = sent_collection.insert_one(event_record).inserted_id
        else:
            sent_collection.update_one({'_id': visitor_exists['_id']}, {'$inc': {'num_visits': 1}}, True)
        # print('Record Created: {}'.format(new_record))

        campaign_collection = get_collection('campaign_collection')
        open_collection = get_collection('opens_collection')

        if campaign_collection.find_one({'campaign_hash': campaign_hash}) is None:
            campaign_collection.insert_one({
                'ip': ip_addr,
                'campaign_hash': campaign_hash,
                'campaign': event_record['campaign'],
                'job_number': event_record['job_number'],
                'client_id': event_record['client_id'],
                'user_agent': event_record['agent'],
                'opens': 0,
                'sends': 1,
                'date_sent': datetime.datetime.now(),
            })
        else:
            campaign_collection.update_one({'campaign_hash': campaign_hash}, {'$inc': {'sends': 1}}, True)

        if open_collection.find_one({'open_hash': open_hash}) is None:
            open_collection.insert_one({
                'ip': ip_addr,
                'open_hash': open_hash,
                'campaign_hash': campaign_hash,
                'campaign': event_record['campaign'],
                'opens': 0,
                'sends': 1,
                'date_sent': datetime.datetime.now(),
            })
        else:
            open_collection.update_one({'open_hash': open_hash}, {'$inc': {'sends': 1}}, True)

        # print the send hash to the console
        print('IP {} record created.  ID: {} Send Hash: {}'.format(ip_addr, new_record, send_hash))
        time.sleep(0.005)
        line_counter += 1

    # output total lines
    print('Total Lines Processed: {}'.format(line_counter))
    return None


if __name__ == '__main__':
    main()

