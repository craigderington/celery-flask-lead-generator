import csv
import json
from datetime import datetime
import requests
import time

# request headers
hdr = {
    'user-agent': 'PyBotG2ByCDFresh',
    'content-type': 'application/json; charset=utf8'
}

ip_adds = []
client_id = '001btl'
zip_code = '01568'
job_number = '29414'
today = str(datetime.now())

# read in our sample data
with open('data/M1-10K-IP-Sample-Data.csv') as f:
    lines = f.readlines()
    for line in lines:
        line_data = line.split(',')
        if 'ip' not in line_data:
            ip = line_data[0]
            ip_data = ip_adds.append(ip)

    ip_list = set(ip_adds)
    # print(ip_list)

for address in ip_list:
    url = 'https://datamatchapi.com/DMSApi/GetDmsApiData?IP={}&Dealer=DMS&Client=DMS&SubClient=Diamond-CRMDev&product=earl' \
          '&JobNumber={}&ClientID={}&VendorID=DMS&DaysToSuppress=0&Radius=0&ZipCode={}'.format(address, job_number,
                                                                                               client_id, zip_code)


    # make the request
    r = requests.get(url, headers=hdr)

    """
    # write to log output
    with open('../logs/feb-9-m1-api-results.txt', 'a') as f1:
        f1.write('{}, {}, {}, {}, {}, {}, {}\n'.format(
            today,
            address,
            job_number,
            client_id,
            zip_code,
            r.status_code,
            r.content
        ))
    """

    if r.status_code == 200:
        json_obj = json.loads(r.text)
        # print(json_obj)
        if isinstance(json_obj, list):
            first_name = json_obj[0]['FirstName']
            last_name = json_obj[0]['LastName']
            print('First Name: {}'.format(first_name))
            print('Last Name: {}'.format(last_name))
        else:
            print(type(json_obj))
    elif r.status_code == 404:
        print('Returned 404')
    elif r.status_code == 503:
        print('Returned 503')
    else:
        print('Did not receive a valid HTTP response code')

    # throttle the api calls
    time.sleep(0.0150)
