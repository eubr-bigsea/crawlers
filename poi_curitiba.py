#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about Curitiba Points of interest
from URBS website and insert to a MongoDB.
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
    parser = argparse.ArgumentParser(description='Crawler of events from \
        the Globo.com website.')
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
    parser.add_argument('-u', '--urbs_key',
        help='URBS access code', required=True)
    return vars(parser.parse_args())


def get_data(access_key):
    ''' Get data from the URBS website '''
    data = []
    link = "http://transporteservico.urbs.curitiba.pr.gov.br/getPois.php?c=" + str(access_key)
    content = requests.get(link)
    content_json = json.loads(content.text)
    for record in content_json:
        record['QUERY_DATE'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        data.append(record)
    return data


if __name__ == "__main__":
    args = get_parameters()

    try:
        while True:
            print 'Running crawler POI Curitiba', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('POI_NAME', pymongo.ASCENDING),
                ('QUERY_DATE', pymongo.ASCENDING)], unique=True)

            data = get_data(args['urbs_key'])
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
