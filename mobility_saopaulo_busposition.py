#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about bus position in Sao Paulo
from SPTrans website and insert to a MongoDB.
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
    parser = argparse.ArgumentParser(description='Crawler of Sao Paulo Bus \
        position from the SP Trans web service.')
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
    parser.add_argument('-token', '--token',
        help='SP Trans token', required=True)
    return vars(parser.parse_args())


def get_data(token):
    ''' Get data from the SP Trans API '''
    link = "http://api.olhovivo.sptrans.com.br/v0"

    # Authentication
    s = requests.Session()
    response = s.post(link+'/Login/Autenticar?token={}'.format(token))


    # Get bus lines
    data = []
    existing_bus_lines = {}
#    content = s.get(link + '/Parada/BuscarParadasPorLinha?codigoLinha={}'.format('2486'))
#    content = s.get(link + '/Parada/Buscar?termosBusca={}'.format('2'))
    content = s.get(link + '/Posicao?codigoLinha={}'.format('2486'))
    content_json = json.loads(content.text)
#        for record in content_json:
#            if not existing_bus_lines.has_key(record['CodigoLinha']):
#                existing_bus_lines['CodigoLinha'] = True
#                record['Data'] = str(datetime.datetime.today())
#                data.append(record)
    print content_json
    return data


if __name__ == "__main__":
    args = get_parameters()

    try:
        while True:
            print 'Running crawler Mobility Sao Paulo Bus Lines', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('CodigoLinha', pymongo.ASCENDING),
                ('Data', pymongo.ASCENDING)], unique=True)

            data = get_data(args['token'])
            sys.exit()

            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
