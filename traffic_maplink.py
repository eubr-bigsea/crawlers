#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
 Collects information about the traffic status in the
 mains streets of the 10 most populatec cities of Brazil
 and insert them to a mongo database.

           Author: Luiz Fernando M Carvalho
'''

import sys
import argparse
import datetime
import time
import pymongo
import json
import requests
import HTMLParser
import re
from bs4 import BeautifulSoup
from control import Control
from monitoring import Monitoring
from db import DB

reload(sys)
sys.setdefaultencoding('utf-8')


def get_parameters():
    ''' Get the parameters '''
    global args
    parser = argparse.ArgumentParser(description='Crawler of Curitiba poi from \
        the URBS website.')
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


def extract_issue(block):
    ''' EXTRACT INFORMATION ABOUT ISSUES IN THE STREET '''
    info_dict = {}
    description = block.get_text().split("-")
    info_dict['date'] = format_string(description[0])
    info_dict['title'] = ""
    for i in range (1,len(description)):
        info_dict['title'] += format_string(description[i])
    info_dict['link'] = block.find("a").attrs['href']
    return info_dict


def extract_intersection(block):
    ''' EXTRACT INFORMATION ABOUT A CORNER OF THE STREET '''
    info_dict = {}
    info_dict['latitude'] = block.attrs['data-x']
    info_dict['longitude'] = block.attrs['data-y']
    info_dict['intersection_name'] = block.attrs['data-endereco']
    info_dict['average_speed'] = BeautifulSoup(block.attrs['title'], "html.parser").find_all('strong')[0].get_text().replace("<","")
    info_dict['estimated_time'] = BeautifulSoup(block.attrs['title'], "html.parser").find_all('strong')[1].get_text().replace("<","")
    info_dict['status'] = re.search('Tr.nsito (.*) no (.*)', format_string(block.get_text())).group(1)
    return info_dict


def format_string(string):
    ''' FORMAT A STRING: REMOVE SQUEEZE SPACES AND REMOVE LINE BREAKS AND TABS '''
    string = re.sub('\r', '', string)
    string = re.sub('\n', '', string)
    string = re.sub('\t', '', string)
    string = re.sub(' +', ' ', string)
    return string.strip()

def get_data(city):
    ''' Get data from the MapLink website '''

    data = []
    city_link = 'http://www.maplink.com.br/' + city + '/Corredores/transito-todos'

    try:
        content = HTMLParser.HTMLParser().unescape(requests.get(city_link))
    except requests.exceptions.ConnectionError:
        time.sleep(600)
        content = HTMLParser.HTMLParser().unescape(requests.get(city_link))

    bs_content = BeautifulSoup(content.text, "html.parser")

    try:        
        list_streets = bs_content.find('ul', {'class':'verticallist'})
        array_streets = list_streets.find_all('li')
    except AttributeError:
        print "ERROR in " + city + " - " + str(bs_content)
        list_streets = ""
        array_streets = []
    
    # FOR EACH STREET, EXTRACT SOME INFORMATION
    for street in array_streets:

        # CREATE THE DICT AND GET SOME BASIC INFORMATION
        record = {}
        record['query_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        record['query_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        record['city'] = city
        record['street_name'] = (re.sub(' +',' ',street.get_text())).strip()
        record['street_status'] = re.search('Tr.nsito agora (.*) em (.*), (.*)', street.attrs['title']).group(1)

        print "PROCESSING CITY " + city + " STREET " + record['street_name']
 
        # GET THE LINK OF THE STREET STATUS AND GET THE CONTENT
        street_link = (re.sub(' +',' ',"http://maplink.com.br" + str(street.find('a').attrs['href']))).strip()
        try:
            content = HTMLParser.HTMLParser().unescape(requests.get(street_link))
        except requests.exceptions.ConnectionError:
            time.sleep(600)
            content = HTMLParser.HTMLParser().unescape(requests.get(street_link))

        content_street_status = BeautifulSoup(content.text, "html.parser").find('div', {'class':'eight columns'})


        # GET INFORMATION ABOUT THE DIRECTIONS
        record['directions'] = []
        record['issues'] = []
        try:
            content_stretch = content_street_status.find_all('h2', {'class':'trechos subheader'})
            record['query_status'] = "successful"
            for stretch in content_stretch:
                stretch_record = {}
                stretch_record['direction_name'] = "Towards " + stretch.attrs['data-sentido']
                stretch_record['estimated_time'] = {}
                stretch_record['intersections'] = []

                # VISIT THE TAGS EXTRACTING INFORMATION
                next_tag = stretch.find_next()
                while next_tag.name.strip() != "h3":

                    # EXTRACT STATUS OF THE DIRECTION WITH AND WITHOUT TRAFFIC
                    if (next_tag.name.strip() == "p"):
                            title = format_string(next_tag.find('span', {'class':'upper'}).get_text()).replace(' ', '_')
                            content = format_string(next_tag.find('span', {'class':'dados'}).get_text())
                            stretch_record['estimated_time'][title] = content

                    # EXTRACT INFORMATION ABOUT THE CORNER
                    if (next_tag.name.strip() == "li"):
                        stretch_record['intersections'].append(extract_intersection(next_tag))
                    next_tag = next_tag.find_next()

                record['directions'].append(stretch_record)

            # GET INFORMATION ABOUT ISSUES IN THE STREET
            next_tag = content_street_status.find('h3', {'class':'upper'})
            
            # VISIT ALL TAGES RELATED TO ISSUES
            while next_tag.name.strip() != "div":

                # IF IT IS A LINK, RETURN TO THE PREVIOUS TO GET THE COMPLETE INFORMATION
                if (next_tag.name.strip() == "a"):
                    target_tag = next_tag.find_previous()
                    record['issues'].append(extract_issue(target_tag))
                next_tag = next_tag.find_next()

        except AttributeError:
            print "FALHA NA RUA " + str((re.sub(' +',' ',street.get_text())).strip())
            record['query_status'] = "failed"

        data.append(record)

    return data


if __name__ == "__main__":
    args = get_parameters()

    # THE 10 MOST POPULATED CITIES OF BRAZIL
    cities = ("sp/sao_paulo", "pr/curitiba", "rj/rio_de_janeiro", \
        "mg/belo_horizonte", "ce/fortaleza", "pe/recife", "df/brasilia", \
        "rs/porto_alegre", "am/manaus", "ba/salvador", "pa/belem", "go/goiania")

    try:
        while True:
            print "Running Crawler Traffic Maplink", str(datetime.datetime.now())
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])

            for city in cities:
                print "Running city", city
                data = get_data(city)
                insertions = connection.send_data(data, collection)
                Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()

            control.set_end(insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
