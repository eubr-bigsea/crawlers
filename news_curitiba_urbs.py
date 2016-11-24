#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
 Collects information about the traffic given by the
 Curitiba's city hall and insert them to a mongo database.
 
 Author: Luiz Fernando M Carvalho
'''

import sys
import datetime
import pymongo
import argparse
import HTMLParser
import requests
from bs4 import BeautifulSoup
from control import Control
from monitoring import Monitoring
from db import DB

reload(sys)
sys.setdefaultencoding('utf-8')

def get_parameters():
    ''' Get the parameters '''
    parser = argparse.ArgumentParser(description='Crawler of Curitiba buses \
            news provided by URBS')
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
    ''' Get data from URBS website'''
    data = []
    link = "https://www.urbs.curitiba.pr.gov.br/transporte/boletim-de-transportes"
    content = HTMLParser.HTMLParser().unescape(requests.get(link))
    bs_content = BeautifulSoup(content.text, "html.parser")
    news = bs_content.find_all('div',
                               {'class': 'bg-white width90 inside-content clearfix margin-medium-bottom round-bl'})

    for new in news:
        date_post = datetime.datetime.strptime((new.find_all('span', {'class': 'date resize'})[0].get_text()),
                                               '%d/%m/%Y').date()
        record = {}
        record['title'] = str(new.find_all('h2')[0].get_text())
        record['date'] = str(date_post)
        record['text'] = str(new.find_all('p', {'style': 'text-align: justify;'})[0].get_text())
        record['query_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        try:
            record['link'] = str(new.find_all('a')[0])
        except IndexError:
            record['link'] = ""
        data.append(record)
    return data

if __name__ == "__main__":
    args = get_parameters()

    try:
        while True:
            print "Running Crawler Curitiba URBS \
                (news abouts bus routes and schedules)", str(datetime.datetime.now())
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