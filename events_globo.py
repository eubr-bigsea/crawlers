#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
globo.py
Collect information about concerts from Globo.com
and insert to a MongoDB.
Author: Luiz Fernando M Carvalho
Email: fernandocarvalho3101 at gmail dot com
'''

import sys
import pymongo
import requests
import json
import datetime
import argparse
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
    return vars(parser.parse_args())



def get_data():
    ''' Get data from the Globo website'''
    data = []
    for i in range(1,6):
        link = "http://g1.globo.com/musica/agenda_shows/todos/6/"+str(i)+".json"
        web_content = requests.get(link)
        json_content = json.loads(web_content.text)

        for date in json_content['shows']:
            for record in json_content['shows'][date]:
                data.append(record)

    return data



if __name__ == "__main__":
    args = get_parameters()
    try:
        while True:
            print 'Running crawler Events Globo', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            data = get_data()
            collection = connection.get_collection(args['collection'])
            collection.create_index([('nome_popular', pymongo.ASCENDING),
                ('cidade', pymongo.ASCENDING), ('data_de_inicio', pymongo.ASCENDING)],
                unique=True)
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
