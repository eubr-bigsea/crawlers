# !/usr/bin/python
# -*- coding: utf-8 -*-

'''
 Collects information about the bus lines of Curitiba
 and insert them to a mongo database.
 Author: Luiz Fernando M Carvalho
'''

import datetime
import requests
import time
import pymongo
import argparse
from zabbix import pyzabbix_sender


def get_parameters():
    ''' Get the parameters '''
    parser = argparse.ArgumentParser(description='Crawler \
        of Points of Interest of Curitiba from the URBS website.')
    parser.add_argument('-s', '--server', help='Name of the MongoDB server', required=True)
    parser.add_argument('-p', '--persistence', help='Name of the MongoDB \
        persistence slave', required=False)
    parser.add_argument('-d', '--database', help='Name of the MongoDB database', required=True)
    parser.add_argument('-c', '--collection', help='Name of the MongoDB collection', required=True)
    parser.add_argument('-t', '--time', help='Time sleeping seconds', required=True)
    parser.add_argument('-u', '--urbs_key', help='URBS access code', required=True)
    return vars(parser.parse_args())


def db_connect(args):
    ''' Opens the MongoDB connection '''
    ERROR = True
    count_attempts = 0
    while ERROR:
        try:
            count_attempts += 1
            if (args['persistence'] == None):
                client = pymongo.MongoClient(args['server'])
            else:
                client = pymongo.MongoClient([args['server'], args['persistence']])
            client.server_info()
            print 'MongoDB Connection opened after', str(count_attempts), 'attempts', str(datetime.datetime.now())
            ERROR = False
        except pymongo.errors.ServerSelectionTimeoutError:
            print 'MongoDB connection failed after', str(count_attempts), 'attempts. ', \
                str(datetime.datetime.now()), '. A new attempt will be made in 10 seconds'
            time.sleep(10)
    return client


def db_location(args, client, collection_name):
    ''' Set the collection location '''
    db = client[args['database']]
    return db[collection_name]


def get_data(access_key):
    # Builds the link
    link = "http://transporteservico.urbs.curitiba.pr.gov.br/getLinhas.php?c=" + str(access_key)

    ERROR = True
    attempts = 0

    data = []

    while (ERROR):
        try:
            #content = requests.get(link)
            #data = json.loads(content.text)
            data = {}
            ERROR = False
        except ValueError:
            attempts += 1
            print "Failed to get JSON data (Value error)", attempts, "attempts.", str(datetime.datetime.now())
            time.sleep(120)
        except requests.exceptions.ConnectionError:
            attempts += 1
            print "Failed to get JSON data (Request error)", attempts, "attempts.", str(datetime.datetime.now())
            time.sleep(120)
    return data


def send_data(data, collection):
    ''' SEND THE COLLECTED DATA TO THE MONGO DB '''
    count_insertions = 0
    collection.create_index([('DATE', pymongo.ASCENDING), ('COD', pymongo.ASCENDING)], unique=True)
    for record in data:
        try:
            record['DATE'] = datetime.datetime.now().strftime("%Y-%m-%d")
            post_id = collection.insert_one(record).inserted_id
            count_insertions += 1
        except pymongo.errors.DuplicateKeyError:
            pass
    return count_insertions


def inform_zabbix(host, trigger, num):
    ''' SEND THE NUMBER OF RECORDS TO ZABBIX MONITORING '''
    pyzabbix_sender.send(host, trigger, num, pyzabbix_sender.get_zabbix_server())


if __name__ == "__main__":

        print "\n\n\nRunning Crawler Mobility Curitiba Bus Lines", str(datetime.datetime.now())


        args = get_parameters()
        client = db_connect(args)
        collection = db_location(args, client, args['collection'])

        try:
            with open(args['collection'] + ".time") as infile_time:
                input_time = float(infile_time.read())
        except IOError:
            input_time = time.time() + 10
        while (time.time() <= input_time):
            print "Could not execute now", str(datetime.datetime.now())
            print "Sleeping for {} seconds. Until {}".format(
                input_time - time.time(),datetime.datetime.fromtimestamp(input_time))
            time.sleep(input_time - time.time())


        while (True):

            print "Loop Crawler Mobility Curitiba Bus Lines", str(datetime.datetime.now())

            # GET THE TIME OF THE LOOP BEGIN
            init_time = time.time()

            data = get_data(args['urbs_key'])
            count_insertions = send_data(data, collection)

            print "Execution completed with ", count_insertions, "records", str(datetime.datetime.now())

            inform_zabbix('_bigsea', args['collection'] , count_insertions)

            # GET THE TIME OF THE LOOP END AND THE DURATION
            finish_time = time.time()
            duration_time = finish_time - init_time

            print "Execution took", str(duration_time), "seconds."
            print "Next will occur in", float(args['time']) - duration_time, "seconds."
            next_time = time.time() + float(args['time']) - duration_time
            print "Next will occur at", datetime.datetime.fromtimestamp(next_time)

            output_time = open(args['collection']+".time", "w")
            print next_time
            output_time.write(str(next_time))


            # CLOSE THE CONNECTION AND WAIT
            client.close()
            if (int(args['time']) > duration_time):
                time.sleep(int(args['time']) - duration_time)


        print "Execution failed", str(datetime.datetime.now())
        inform_zabbix('_bigsea', args['collection'], 0)
        print u'\n\nShutting down...\n\n'
