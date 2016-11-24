#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about concerts from Google
and insert to a MongoDB.
Author: Luiz Fernando M Carvalho
Email: fernandocarvalho3101 at gmail dot com
'''

import sys
import datetime
import time
import pymongo
import json
import argparse
import requests
import HTMLParser
from bs4 import BeautifulSoup
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



def get_data(cities):
    ''' Get data from the Google website '''
    data = []

    for city in cities:

        print "City:",city
        link = "https://www.google.com.br/search?hl=pt-BR&q=events"

        for token in (city.split(" ")):
            link += "+" + token
        content = HTMLParser.HTMLParser().unescape(requests.get(link))

        # EXTRACT THE EVENTS TABLE
        bs_content = BeautifulSoup(content.text, "html.parser")
        block = bs_content.find_all('table', {'class': '_KLb'})

        # CHECK IF THE CITY HAS EVENT ON GOOGLE
        try:
            items = block[0].find_all('tr')
        except IndexError:
            # NOTHING TO DO. THE CITY HAS NO EVENT
            items = []

        # FOR EACH EVENT, EXTRACT TITLE, DATE AND PLACE
        for item in items:
            record = {}
            title = item.find('td', {'class': '_JLb _y3h'})
            date = item.find('td', {'class': '_JLb _cZg'})
            place = title.find('div', {'class': '_h8d _g8d'})

            # IF THE PLACE IS NOT EMPTY, GET IT AND SUBTRACT IT FROM THE TITLE
            try:
                record['place'] = str(place.get_text())
                length_place = len(str(place.get_text()))
                record['title'] = str(title.get_text())[:-length_place]
            except AttributeError:
                record['place'] = "Unknown"
                record['title'] = str(title.get_text())
            record['date'] = str(date.get_text())
            record['request_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
            record['city'] = city

            data.append(record)

    return data






if __name__ == "__main__":
    args = get_parameters()
    cities = ("Belo Horizonte", "Brasilia", "Curitiba",
              "Fortaleza", "Manaus", "Porto Alegre", "Recife",
              "Rio de Janeiro", "Salvador", "Sao Paulo")

    try:
        while True:
            print 'Running crawler Events Google', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('title', pymongo.ASCENDING),
                ('date', pymongo.ASCENDING)], unique=True)

            data = get_data(cities)
            insertions = connection.send_data(data, collection)

            Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
