# !/usr/bin/python
# -*- coding: utf-8 -*-

'''
 Collects information about bus position in Curitiba
 from URBS server and inserts it to a mongo database.
 Author: Luiz Fernando M Carvalho
'''

import sys
import datetime
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
    parser = argparse.ArgumentParser(description='Crawler \
        of Points of Interest of Curitiba from the URBS website.')
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
    # Builds the link
    link = "http://transporteservico.urbs.curitiba.pr.gov.br/getVeiculosLinha.php?c=" + str(access_key)
    data = []
    content = requests.get(link)
    records = json.loads(content.text)
    for record in records:
        record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
        data.append(record)
    return data


def send_data(data, collection):
    ''' SEND THE COLLECTED DATA TO THE MONGO DB '''
    count_insertions = 0
    for record in data:
        collection.insert_one(record)
        count_insertions += 1
    return count_insertions


if __name__ == "__main__":
    args = get_parameters()
    try:
        while True:
            print 'Running crawler Mobility Curitiba Busposition', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            data = get_data(args['urbs_key'])
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
