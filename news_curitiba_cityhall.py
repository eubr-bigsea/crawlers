#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
 Collects information about the traffic given by the
 Curitiba's city hall and insert them to a mongo database.
 
 Author: Luiz Fernando M Carvalho
'''


import datetime
import time
import pymongo
import argparse
import json
import feedparser    # FOR RSS REQUISITION
import pytz          # FOR TIME ZONE CONVERTION
import sys
from control import Control
from monitoring import Monitoring
from db import DB

def get_parameters():
    ''' GET THE PARAMETERS '''
    parser = argparse.ArgumentParser(description='Crawler of Curitiba traffic news provided by the Curitiba\'s City Hall')
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
    ''' REQUEST DATA AND CREATE THE DICTIONARIES '''
    rss_url = "http://www.curitiba.pr.gov.br/include/handler/rss.ashx?feed=2" 
    feeds = feedparser.parse(rss_url)
    data = []
    for feed in feeds['items']:
        record = {}
        feed_time = feed['published']
        feed_time = datetime.datetime.strptime(feed_time, '%a, %d %b %Y %H:%M:%S %Z')
        feed_time = pytz.utc.localize(feed_time).astimezone(br_time)
        record['title'] =  feed['title']
        record['summary'] =  feed['summary']
        record['link'] =  feed['link']
        record['date'] = str(feed_time)
        record['request_date'] = str(datetime.date.today())
        data.append(record)
    return data


if __name__ == "__main__":
    args = get_parameters()
    br_time = pytz.timezone('America/Sao_Paulo')

    try:
        while True:
            print "Running Crawler Curitiba City Hall", str(datetime.datetime.now())
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('title', pymongo.ASCENDING),
                                     ('date', pymongo.ASCENDING)], unique=True)


            data = get_data()
            insertions = connection.send_data(data, collection)
            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()
            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'