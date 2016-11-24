#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about the Weather
from the Dark Sky API and insert to a MongoDB.
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


def read_file(data,file_name, region_type):
    ''' Read and return the information about the target points'''
    with open(file_name) as infile:
        for line in infile:
            record = {}
            columns = line.rstrip().split(";")
            record['type'] = region_type
            record['code'] = str(columns[0])
            record['city'] = str(columns[1])
            record['latitude'] = str(columns[2])
            record['longitude'] = str(columns[3])
            data.append(record)


def get_parameters():
    ''' Get the parameters '''
    global args
    parser = argparse.ArgumentParser(description='Get data about the \
        Weather from the Dark Sky API')
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
    parser.add_argument('-ci', '--cities',
        help='CSV file with the target cities information', required=True)
    parser.add_argument('-su', '--subdistricts',
        help='CSV file with the target subdistricts information', required=True)
    parser.add_argument('-a', '--api_key',
        help='Dark SKY API', required=True)
    return vars(parser.parse_args())


def get_data(regions, api_key):
    ''' Get data about the weather '''
    data = []
    for region in regions:
        print region
        # BUILD THE LINK AND GET THE CONTENT
        link = "https://api.darksky.net/forecast/" + api_key + "/" + \
            region['latitude'] + "," + region['longitude']
        #print "\n\n",link,"\n\n"
        content = requests.get(link)
        record = json.loads(content.text)
        #ADD FIELDS ABOUT THE CRAWLING
        record['query'] = {}
        record['query']['datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        record['query']['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
        record['query']['type'] = region['type']
        record['query']['code'] = region['code']
        record['query']['city'] = region['city']
        data.append(record)
    return data


if __name__ == "__main__":
    args = get_parameters()
    regions = []
    read_file(regions, args['cities'], "CITY")
    read_file(regions, args['subdistricts'], "SUBDISTRICT")

    try:
        while True:
            print "Running Crawler Weather Dark Sky " + str(datetime.datetime.now())
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])

            data = get_data(regions, args['api_key'])
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'







