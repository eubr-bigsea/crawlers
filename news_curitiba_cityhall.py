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
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from zabbix import pyzabbix_sender

def get_parameters():
    ''' GET THE PARAMETERS '''
    parser = argparse.ArgumentParser(description='Crawler of Curitiba traffic news provided by the Curitiba\'s City Hall')
    parser.add_argument('-s','--server', help='Name of the MongoDB server', required=True)
    parser.add_argument('-p','--persistence', help='Name of the MongoDB persistence slave', required=False)
    parser.add_argument('-d','--database', help='Name of the MongoDB database', required=True)
    parser.add_argument('-c','--collection', help='Name of the MongoDB collection', required=True)
    parser.add_argument('-t','--time', help='Time sleeping seconds', required=True)
    return vars(parser.parse_args())


def db_connect(args):
    ''' OPENS THE MONGO DB CONNECTION '''
    ERROR = True 
    count_attempts = 0
    while ERROR:
        try:
            count_attempts += 1
            if (args['persistence'] == None): 
                client = pymongo.MongoClient(args['server'])
            else: 
                client = pymongo.MongoClient([ args['server'], args['persistence'] ])
            client.server_info()
            print 'MongoDB Connection opened after', str(count_attempts), 'attempts', str(datetime.datetime.now())
            ERROR = False
        except pymongo.errors.ServerSelectionTimeoutError:
            print 'MongoDB connection failed after', str(count_attempts), 'attempts. ', \
                  str(datetime.datetime.now()), '. A new attempt will be made in 10 seconds'
            time.sleep(10)
    return client


def db_location(args, client):
    ''' SET THE DB LOCATION AND RETURN THE COLLECTION '''
    db = client[args['database']]
    return db[args['collection']]


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


def send_data(data, collection):
    ''' SEND THE COLLECTED DATA TO THE MONGO DB '''
    count_insertions = 0
    collection.create_index([('title', pymongo.ASCENDING), ('date', pymongo.ASCENDING)], unique = True)
    for record in data:
        try:
            post_id = collection.insert_one(record).inserted_id
            count_insertions += 1
        except pymongo.errors.DuplicateKeyError:
             pass
    return count_insertions


def inform_zabbix(host,trigger,num):
    ''' SEND THE NUMBER OF RECORDS TO ZABBIX MONITORING '''
    pyzabbix_sender.send(host,trigger,num,pyzabbix_sender.get_zabbix_server())


if __name__ == "__main__":

    try:
        br_time = pytz.timezone('America/Sao_Paulo')
        args = get_parameters()
        client = db_connect(args)
        collection = db_location(args, client)

        while (True):
            print "Running Crawler Curitiba City Hall", str(datetime.datetime.now())
            data = get_data()
            count_insertions = send_data(data, collection)
            print "Execution completed with ", count_insertions, "records", str(datetime.datetime.now())
            inform_zabbix('_bigsea', 'news_curitiba_cityhall', count_insertions)
            client.close()
            time.sleep(int(args['time']))

    except:
        print "Execution failed", str(datetime.datetime.now())
        inform_zabbix('_bigsea', 'news_curitiba_cityhall', 0)
        print u'\n\nShutting down...\n\n'

