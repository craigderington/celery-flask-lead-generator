import csv
import json
import os
import re

appended_list = []
json_list = []

path = os.getcwd()
datafile = '/data/m1-cronlog.log'
logfile = '/logs/Lakeland-Automall-January-2018.csv'
filename = '/home/craigderington/sites/earl-flask-frontend/data/m1-cronlog.log'
output_file = '/home/craigderington/sites/earl-flask-frontend/logs/Lakeland-Automall-January-29-31-2018.csv'

try:
    with open(filename, 'r') as f1:
        lines = f1.readlines()

        if lines:
            for line in lines:
                if 'FirstName' in line:
                    data = line.split('200,')
                    visitor = data[1]
                    cleaned_visitor = re.sub("[\\]\)['\n\']", "", visitor)
                    appended_list.append(cleaned_visitor)

            # remove duplicates from the appended visitor list
            cleaned_list = set(appended_list)

            for item in cleaned_list:
                json_list.append(json.loads(item))

            try:
                f2 = csv.writer(open(output_file, 'wb+'))
                f2.writerow(["FirstName", "LastName", "Address", "City", "State", "ZipCode", "Zip4", "YEAR", "MAKE",
                             "MODEL", "InferredCreditScore", "EMail", "Cell", "IP", "Dealer", "Product", "JobNumber",
                             "CampaignID", "VendorID"])

                for record in json_list:
                    f2.writerow([
                        record['FirstName'],
                        record['LastName'],
                        record['Address'],
                        record['City'],
                        record['state'],
                        record['ZipCode'],
                        record['Zip4'],
                        record['YEAR'],
                        record['MAKE'],
                        record['MODEL'],
                        record['InferredCreditScore'],
                        record['EMail'],
                        record['Cell'],
                        record['IP'],
                        record['Dealer'],
                        record['Product'],
                        record['JobNumber'],
                        record['CampaignID'],
                        record['VendorID'],
                    ])

                    print(record)

            except IOError as err:
                print('The output file source path can not be accessed: {}'.format(err))

        else:
            print('The data file was read but contains no records.  Aborting.')


except IOError as e:
    print('Could not read the source file: {}'.format(e))
