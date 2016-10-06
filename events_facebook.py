#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about events
from the Facebook and insert to a MongoDB.
The points to be searched are based on the
centroid of each city's subdistrict.
Author: Luiz Fernando M Carvalho
Email: fernandocarvalho3101 at gmail dot com
'''

import sys
import datetime
import pymongo
import json
import argparse
import requests
import random
import time
from control import Control
from monitoring import Monitoring
from db import DB

reload(sys)
sys.setdefaultencoding('utf-8')


def get_parameters():
    ''' Get the parameters '''
    global args
    parser = argparse.ArgumentParser(description='Collects information about \
        events from FB using the location and insert to a MongoDB.')
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
    parser.add_argument('-i','--centroids_infile',
        help='Name of the infile with the points', required=True)
    parser.add_argument('-a','--api_infile',
        help='Prefix of the infiles with the API keys', required=True)
    return vars(parser.parse_args())


def read_api_tokens():
    ''' READS THE API TOKENS AND STORE IN THE ARRAY '''
    global number_api_tokens
    global api_tokens
    with open(args['api_infile']) as infile:
        for line in infile:
            tokens = line.rstrip().split(";")
            for token in tokens:
                api_tokens.append(str(token))


def read_csv_file():
    ''' READ THE FILE INFORMATION AND RETURN A DICT WITH THEM '''
    global args
    global centroids
    with open(args['centroids_infile']) as infile:
        for line in infile:
            record = {}
            columns = line.rstrip().split(";")
            record['code'] = str(columns[0])
            record['city'] = str(columns[1])
            record['latitude'] = str(columns[2])
            record['longitude'] = str(columns[3])
            centroids.append(record)


def chunks(l, n):
    ''' YIELD SUCCESSIVE N-SIZED CHUNKS FROM L. '''
    for i in range(0, len(l), n):
        yield l[i:i+n]


def request_data(link):
    ''' PERFORM A GET REQUEST AND RETURN THE JSON '''
    ERROR = True
    attempts = 0
    while (ERROR):
        if attempts > 10:
            print "Could not get the data! Shuttin down..."
            sys.exit()
        try:
            content = requests.get(link)
            content_json = json.loads(content.text)
            ERROR = False
        except ValueError:
            attempts += 1
            print "Failed to get JSON data (Value error)", attempts, \
                "attempts.", str(datetime.datetime.now())
            time.sleep(120)
        except requests.exceptions.ConnectionError:
            attempts += 1
            print "Failed to get JSON data (Request error)", attempts, \
                "attempts.", str(datetime.datetime.now())
            time.sleep(120)
    return content_json


def get_data(centroid, api_tokens):
    ''' GET INFORMATION ABOUT EVENTS '''
    data = []

    # BUILDS THE LINK
    link_get_ids = "https://graph.facebook.com/v2.5/search?type=place&q=&center=" + \
        centroid['latitude'] + "," + centroid['longitude'] + "&distance=" + \
        str(2000) + "&limit=" + str(5000) + "&fields=id&access_token=" + \
        api_tokens[random.randint(0, len(api_tokens)-1)]

    # GET DATA
    dict_venues_ids = request_data(link_get_ids)
    array_venue_ids = [x['id'] for x in dict_venues_ids['data']]

    for group in list(chunks(array_venue_ids, 50)):
        link_venues_content = "https://graph.facebook.com/v2.4/?ids=" + \
            ','.join(group) + "&fields=id,name,cover.fields(id,source)," + \
            "picture.type(large),location,events.fields(id,name,cover.fields(id,source)\
            ,picture.type(large),description," + "start_time,attending_count,declined_count,\
            maybe_count,noreply_count,place).since(" + str(int(time.time())) + \
            ")&access_token=" + api_tokens[random.randint(0, len(api_tokens)-1)]

        dict_venues_content = request_data(link_venues_content)

        for venue_id in dict_venues_content:

            if dict_venues_content[venue_id].has_key('events'):
                record = {}
                record['query'] = {}
                record['query']['query_city '] = centroid['city']
                record['query']['query_subdistrict'] = centroid['code']
                record['query']['query_date'] = str(datetime.date.today())
                record['query']['query_datetime'] = str(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + " BRT"
                record['venue'] = {}
                record['venue']['venue_id'] = dict_venues_content[venue_id]['id']
                try:
                    record['venue']['venue_name'] = dict_venues_content[venue_id]['name']
                except KeyError:
                    record['venue']['venue_name'] = ''
                try:
                    record['venue']['venue_picture'] = dict_venues_content[venue_id]['picture']['data']
                except KeyError:
                    record['venue']['venue_picture'] = {}
                try:
                    record['venue']['venue_cover'] = dict_venues_content[venue_id]['cover']
                except KeyError:
                    record['venue']['venue_cover'] = {}
                try:
                    record['venue']['venue_location'] = dict_venues_content[venue_id]['location']
                except KeyError:
                    record['venue']['venue_location'] = {}

                for event in dict_venues_content[venue_id]['events']['data']:
                    #print "EVENT", event['id']
                    record['event'] = {}
                    record['event']['event_id'] = event['id']
                    try:
                        record['event']['event_name'] = event['name']
                    except KeyError:
                        record['event']['event_name'] = ''
                    try:
                        record['event']['event_description'] = event['description']
                    except KeyError:
                        record['event']['event_description'] = ''
                    try:
                        record['event']['event_picture'] = event['picture']['data']
                    except KeyError:
                        record['event']['event_picture'] = {}
                    try:
                        record['event']['event_cover'] = event['cover']
                    except KeyError:
                        record['event']['event_cover'] = {}
                    try:
                        record['event']['event_start_time'] = event['start_time']
                    except KeyError:
                        record['event']['event_start_time'] = ''
                    record['event']['event_popularity'] = {}
                    try:
                        record['event']['event_popularity']['attending_count'] = event['attending_count']
                    except KeyError:
                        record['event']['event_popularity']['attending_count'] = ''
                    try:
                        record['event']['event_popularity']['declined_count'] = event['declined_count']
                    except KeyError:
                        record['event']['event_popularity']['declined_count'] = ''
                    try:
                        record['event']['event_popularity']['noreply_count '] = event['noreply_count']
                    except KeyError:
                        record['event']['event_popularity']['noreply_count '] = ''
                    try:
                        record['event']['event_popularity']['maybe_count'] = event['maybe_count']
                    except KeyError:
                        record['event']['event_popularity']['maybe_count'] = ''
                    try:
                        record['event']['event_place'] = event['place']
                    except KeyError:
                        record['event']['event_place'] = {}

                    data.append(record)

    return data


if __name__ == "__main__":
    args = get_parameters()
    centroids = []
    api_tokens = []
    read_csv_file()
    read_api_tokens()


    try:
        while True:

            print 'Running crawler Events Facebook', datetime.datetime.now()

            insertions = 0
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('event.event_id', pymongo.ASCENDING),
                ('query.query_date', pymongo.ASCENDING)], unique=True)

            for centroid in centroids:
                print "Centroid", centroid
                data = get_data(centroid, api_tokens)
                insertions += connection.send_data(data, collection)
            Monitoring().send(args['zabbix_host'],str(args['collection']), insertions)

            connection.client.close()
            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'

