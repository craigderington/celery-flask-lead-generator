#! .env/bin/python
import csv
import requests
import time

BASE_URL = 'http://freegeoip.net'

hdr = {
    'user-agent': 'Mozilla/Firefox 5.0/Linux'
}

with open('data/unique-JAN-18-IPs.csv', 'r') as csv_file:
    reader = csv.reader(csv_file, delimiter=',', quotechar='"')

    for row in reader:
        ip = row[0]
        url = BASE_URL + '/json/' + ip
        r = requests.get(url, headers=hdr)
        resp = r.json()
        print(resp)
        time.sleep(0.15)

        with open('logs/jan-2018-visitor-geoipdata-results.txt', 'a') as f1:
            f1.write('IP: ' + resp['ip'] + ' Region Code: ' + resp['region_code'] + ' Country Code: ' +
                     resp['country_code'] + ' LatLong: ' + str(resp['latitude'])
                     + '/' + str(resp['longitude']) + '\n')
