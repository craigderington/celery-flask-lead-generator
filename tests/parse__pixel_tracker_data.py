import csv
import requests
import json
import time

url = 'https://lead-int.earlbdc.com/api/v1/create/visitor/'

vendor = 'DMS'

hdr = {
    'user-agent': 'Mozilla/Linux 5.0',
    'content-type': 'application/json'
}

with open('file_system_path_to_your_file', 'r') as f1:
    reader = csv.reader(f1)

    # skip the header row
    next(reader)

    for row in reader:
        job_number = row[0]
        client_id = row[1]
        ip = row[2]

        payload = {
            "job_number": job_number,
            "ip": ip,
            "client_id": client_id,
            "vendor": vendor
        }

        data = json.dumps(payload)
        print(data)

        try:
            r = requests.post(url, data=data, headers=hdr)
            print(r.status_code, r.content)

            # throttle the API calls and delay 1/4 second
            time.sleep(0.250)

        except requests.HTTPError as e:
            print(e)
