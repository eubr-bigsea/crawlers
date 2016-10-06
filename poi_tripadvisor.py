#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Collect information about Points of interest
from the Trip Advisor website and insert to a MongoDB.
Author: Luiz Fernando M Carvalho
Email: fernandocarvalho3101 at gmail dot com
'''



import sys
import datetime
import time
import pymongo
import requests
import HTMLParser
import argparse
import re
from bs4 import BeautifulSoup
from control import Control
from monitoring import Monitoring
from db import DB


reload(sys)
sys.setdefaultencoding('utf-8')


def get_cities():
    ''' Return a dict with information about the 10 most populates cities in Brazil '''
    cities = []
    cities_names = ("belo horizonte", "sao paulo", "curitiba", "rio de janeiro", \
        "fortaleza", "recife", "brasilia", "porto alegre", "manaus", "salvador")
    cities_codes = ("g303374", "g303631", "g303441", "g303506", "g303293",\
        "g304560", "g303322", "g303546", "g303235", "g303272")
    for i in range(0, len(cities_codes)):
        city = {}
        city['name'] = cities_names[i]
        city['code'] = cities_codes[i]
        cities.append(city)
    return cities


def format_string(string):
    ''' Remove spaces from a string '''
    string = re.sub('\r', '', string)
    string = re.sub('\n', '', string)
    string = re.sub('\t', '', string)
    string = re.sub(' +', ' ', string)
    return string.strip()



def get_parameters():
    ''' Get the parameters '''
    global args
    parser = argparse.ArgumentParser(description='Crawler of Points of \
        Interest of the most populated cities in Brazil from the Trip Advisor website.')
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




def get_data(city):
    ''' Get the Trip Advisor data about each city's point of interest '''

    data = []
    city_link = "https://www.tripadvisor.com.br/Attractions-" + city['code']
    error = True
    
    while(error):
        try:
            #city_content = HTMLParser.HTMLParser().unescape(requests.get(city_link))
            city_content = requests.get(city_link, timeout=3)
            error = False
        except:
            print "error requesting", city_link, str(datetime.datetime.now())
            time.sleep(5)
    city_content_bs = BeautifulSoup(city_content.text, "html.parser")

    # GET THE LIST OF CATEGORIES AND ITERATES OVER THEM
    categories = city_content_bs.find_all('div', {'class':'filter filter_xor '})
    for category in categories:

        # EXTRACT THE category_description, SIZE AND CODE
        category_description = category.find('span', {'class':'filter_name'}).get_text().encode("utf-8")
        category_size = category.find('span', {'class':'filter_count'}).get_text().strip("()")
        category_code = "c" + category.attrs['id'].split("_")[2]

        # GET THE FIRST PAGE WITH THE LIST OF PLACES
        first_page_link = "https://www.tripadvisor.com.br/Attractions-"+ \
            city['code'] +"-Activities-" + category_code
        error = True
        while(error):
            try:
                #first_page_content = HTMLParser.HTMLParser().unescape(requests.get(first_page_link))
                first_page_content = requests.get(first_page_link, timeout=3)
                error = False
            except:
                print "error requesting", first_page_link, str(datetime.datetime.now())
                time.sleep(5)
        first_page_content_bs = BeautifulSoup(first_page_content.text, "html.parser")

        # ARRAY TO STORE ALL THE LINKS OF PAGES WHICH LIST PLACES OF THE CATEGORY IN THE CITY
        pages_link = []
        pages_link.append(first_page_link)

        # GET THE OTHER PAGES WITH THE LIST OF PLACES
        try:
            places_pages = first_page_content_bs.find('div', {'class':'pageNumbers'}).find_all('a')
            for places_page in places_pages:
                pages_link.append("https://www.tripadvisor.com.br" + places_page.attrs['href'])

        except AttributeError:
            places_pages = []

        # FOR EACH PAGE, EXTRACT SOME INFORMATION ABOUT THE PLACES AND VISIT THE PLACE PAGE
        for current_link in pages_link:
            # GET THE CONTENT OF THE CURRENT PAGES
            error = True
            while(error):
                try:
                    #current_page_content = HTMLParser.HTMLParser().unescape(requests.get(current_link))
                    current_page_content = requests.get(current_link, timeout=3)
                    error = False
                except:
                    print "error requesting", current_link, str(datetime.datetime.now())
                    time.sleep(5)
            current_page_content_bs = BeautifulSoup(current_page_content.text, "html.parser")

            # GET ALL BLOCKS WITH TAGS WITH ID 'ATTR_ENTRY'
            places_blocks = current_page_content_bs.find_all('div', {'id':re.compile("ATTR_ENTRY_.*")})

            # FOR EACH BLOCK RELATED TO A PLACE, EXTRACT SOME INFORMATION AND THE LINK OF THE PLACE
            for place_block in places_blocks:
                place_info = {}

                # GET SOME BASIC INFORMATION ABOUT THE PLACE FROM ITS FRAM IN THE LIST OF PLACES
                place_info['city'] = city['name']
                place_info['trip_advisor'] = {}
                place_info['trip_advisor']['source_link'] = "https://www.tripadvisor.com.br" + \
                    place_block.find('div', {'class':'property_title'}).find('a').attrs['href']
                place_info['place_name'] = place_block.find('div',
                    {'class':'property_title'}).find('a').get_text()
                #print place_info['place_name']
                place_info['category'] = category_description
                place_info['popularity'] = {}
                place_info['popularity']['ranking_position'] = format_string(place_block.find('div',
                    {'class':'popRanking wrap'}).get_text())

                # GET THE AVERAGE RATING OF THE PLACE
                try:
                    place_info['popularity']['rating_score'] = format_string(place_block.find('div',
                        {'class':'rs rating'}).find('img', {'class':'sprite-ratings'}).attrs['alt'])
                except AttributeError:
                    place_info['popularity']['rating_score'] = "Not rated"

                # GET THE NUMBER OF RATINGS THAT THE PLACE RECEIVED
                try:
                    place_info['popularity']['number_of_votes'] = format_string(place_block.find('div',
                        {'class':'rs rating'}).find('a').get_text())
                except AttributeError:
                    place_info['popularity']['number_of_votes'] = str(0)

                # SAVE THE CODES USED TO ACCESS THE INFORMATION IN THE TRIP ADVISOR
                place_info['trip_advisor']['city_code'] = city['code']
                place_info['trip_advisor']['category_code'] = category_code
                place_info['trip_advisor']['place_code'] = place_block.attrs['id'].split("_")[2]
                place_info['scraping_date'] = datetime.datetime.now().strftime("%Y-%m-%d")

                # GET THE PAGE WITH INFORMATION ABOUT THE PLACE
                error = True
                while(error):
                    try:
                        current_place_content = requests.get(place_info['trip_advisor']['source_link'],
                            timeout=3)
                        current_place_content_bs = BeautifulSoup(current_place_content.text, "html.parser")
                        error = False
                    except:
                        print "error requesting", place_info['trip_advisor']['source_link'], \
                            str(datetime.datetime.now())
                        time.sleep(5)


                # GET THE TAGS / SUBCATEGORIES
                place_info['tags'] = []
                tags_block = current_place_content_bs.find('div', {'class':'heading_details'}).find_all('a')
                for tag_block in tags_block:
                    place_info['tags'].append(tag_block.get_text())

                # GET THE ADDRESS
                place_info['location'] = {}
                place_info['location']['address'] = {}
                place_info['location']['address']['street'] = current_place_content_bs.find('span',
                    {'property':'address'}).find('span',{'class':'street-address'}).get_text()
                try:
                    place_info['location']['address']['complement'] = current_place_content_bs.find('span',
                        {'property':'address'}).find('span',{'class':'extended-address'}).get_text()
                except AttributeError:
                    place_info['location']['address']['complement'] = ""
                place_info['location']['address']['locality'] = current_place_content_bs.find('span',
                    {'property':'address'}).find('span',{'property':'addressLocality'}).get_text()
                place_info['location']['address']['region'] = current_place_content_bs.find('span',
                    {'property':'address'}).find('span',{'property':'addressRegion'}).get_text()
                place_info['location']['address']['postal_code'] = current_place_content_bs.find('span',
                    {'property':'address'}).find('span',{'property':'postalCode'}).get_text()
                try:
                    place_info['location']['address']['phone'] = current_place_content_bs.find('div',
                        {'class':'phoneNumber'}).get_text()
                    place_info['location']['address']['phone'] = re.sub("N.mero de telefone: ","",
                        place_info['location']['address']['phone'])
                except:
                    place_info['location']['address']['phone'] = ""


                # IF THE PLACE HAS OR NOT A QUALITY CERTIFICATE
                try:
                    place_info['popularity']['quality_certificate'] = current_place_content_bs.find('span',
                        {'class':'taLnk text'}).get_text()
                    place_info['popularity']['quality_certificate'] = "Yes"
                except AttributeError:
                    place_info['popularity']['quality_certificate'] = "No"


                # GET THE LATITUTE AND THE LONGITUDE
                place_info['location']['coordinates'] = {}
                try:
                    place_info['location']['coordinates']['latitude'] = current_place_content_bs.find('div',
                        {'class':'mapContainer'}).attrs["data-lat"]
                    place_info['location']['coordinates']['longitude'] = current_place_content_bs.find('div',
                        {'class':'mapContainer'}).attrs["data-lng"]
                except AttributeError:
                    place_info['location']['coordinates']['latitude'] = ""
                    place_info['location']['coordinates']['longitude'] = ""


                # GET HOW MANY VOTES EACH QUALITY LEVEL HAS RECEIVED
                try:
                    rates = current_place_content_bs.find('ul',{'class':'barChart'}).find_all("li")
                    place_info['popularity']['scores'] = {}
                    for rate in rates:
                        key = "votes_" + re.sub(" ", "", rate.find('div',
                            {'class':'label fl part'}).get_text().lower())
                        place_info['popularity']['scores'][key] = rate.find('div',
                            {'class':'valueCount fr part'}).get_text()
                except AttributeError:
                    place_info['popularity']['scores'] = {}

                data.append(place_info)

    return data





if __name__ == "__main__":
    args = get_parameters()

    try:
        while True:
            print 'Running crawler POI Trip Advisor', datetime.datetime.now()
            connection = DB(args)
            control = Control(args['sleep_time'],args['control_file'])
            control.verify_next_execution()

            collection = connection.get_collection(args['collection'])
            collection.create_index([('trip_advisor.place_code', pymongo.ASCENDING),
                ('city', pymongo.ASCENDING), ('scraping_date', pymongo.ASCENDING)], unique=True)

            total_insertions = 0
            for city in get_cities():
                print "City: ",city
                data = get_data(city)
                insertions = connection.send_data(data, collection)
                total_insertions += insertions
                Monitoring().send(args['zabbix_host'], str(args['collection']), insertions)

            connection.client.close()
            control.set_end(total_insertions)
            control.assign_next_execution()

    except KeyboardInterrupt:
        print u'\nShutting down...'
