#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about bus position in Rio
from Data Rio website and insert to a MongoDB.
Author: Luiz Fernando M Carvalho
Email: fernandocarvalho3101 at gmail dot com
'''

import sys
import datetime
import pymongo
import json
import argparse
import requests
from control import Control
from monitoring import Monitoring
from db import DB

reload(sys)
sys.setdefaultencoding('utf-8')


def get_parameters():
    ''' Get the parameters '''
    global args
    parser = argparse.ArgumentParser(description='Crawler of Bus position from \
        the Data Rio website.')
    parser.add_argument('-s','--server',
        help='Name of the MongoDB server', required=True)
    parser.add_argument('-p','--persistence',
        help='Name of the MongoDB persistence slave', required=False)
    parser.add_argument('-d','--database',
        help='Name of the MongoDB database', required=True)
    parser.add_argument('-c','--collection',
        help='Name of the MongoDB collection', required=True)
    parser.add_argument('-t','--sleep_time',
        help='Time sleeping seconds', required=True)
    parser.add_argument('-f', '--control_file',
        help='File to control the execution time', required=True)
    parser.add_argument('-z', '--zabbix_host',
        help='Zabbix host for monitoring', required=True)
    return vars(parser.parse_args())


def get_data():
    ''' Get data from the Data Rio web service '''
    data = []
    link = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/onibus"
    content = requests.get(link)
    content_json = json.loads(content.text)
    for line in content_json['DATA']:
        record = {}
        record['datetime'] = line[0]
        record['vehicle'] = line[1]
        record['bus_line'] = line[2]
        record['latitude'] = line[3]
        record['longitude'] = line[4]
        record['speed'] = line[5]
        record['direction'] = line[6]
        record['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
        data.append(record)
    return data


if __name__ == "__main__":
    args = get_parameters()

    try:
        while True:
            print 'Running crawler Mobility Rio Busposition 2', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('vehicle', pymongo.ASCENDING),
                ('datetime', pymongo.ASCENDING)], unique=True)

            data = get_data()
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
