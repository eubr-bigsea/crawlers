# !/usr/bin/python
# -*- coding: utf-8 -*-



'''
 Collects information about the public transportation
 of Curitiba from the URBS webserver and insert to the MongoDB.
 The information are about bus lines, stops, paths, routes,
 schedules and vehicles.
 Author: Luiz Fernando M Carvalho
'''



import datetime
import requests
import time
import json
import pymongo
import argparse
from zabbix import pyzabbix_sender



class Bus:
    def __init__(self, key):
        self.key = key
        self.data = []

    def get_latest_buslines(self, collection):
        # Get the date of the most recent bus line collected
        latest_date = collection.find().sort([['_id', pymongo.DESCENDING]]).limit(1)[0]['DATE']
        # Get the latest existing bus lines
        latest_bus_lines = collection.find({'DATE': latest_date}).limit(2)
        self.bus_lines = []
        for bus_line in latest_bus_lines:
            self.bus_lines.append(bus_line['COD'])


class BusLines (Bus):
    '''
    Class for collecting data about bus lines in Curitiba from the URBS webserver.
    '''
    def get_data(self):
        link = "http://transporteservico.urbs.curitiba.pr.gov.br/getLinhas.php?c=" + self.key
        error = True
        attempts = 0
        while (error):
            try:
                content = requests.get(link)
                error = False
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
        for record in json.loads(content.text):
            if record.has_key('COD_LINHA'):
                record['DATE'] = datetime.datetime.now().strftime("%Y-%m-%d")
                self.data.append(record)
            else:
                pass



class BusStops (Bus):
    def get_data(self):
        for bus_line in self.bus_lines:
            error = True
            attempts = 0
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getPontosLinha.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Get data from the URBS web server
            while (error):
                try:
                    content = requests.get(link)
                    error = False
                # Error in the Json content
                except ValueError:
                    attempts += 1
                    print "Failed to get JSON data (Value error)", attempts, "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
                # Requisition error
                except requests.exceptions.ConnectionError:
                    attempts += 1
                    print "Failed to get JSON data (Request error)", attempts, \
                        "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
            # Split the array and add some information
            for record in json.loads(content.text):
                record['DATE'] = datetime.datetime.now().strftime('%Y-%m-%d')
                record['BUS_LINE'] = bus_line
                # Append to the data array
                self.data.append(record)


class BusPaths (Bus):
    def get_data(self):
        for bus_line in self.bus_lines:
            error = True
            attempts = 0
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getTrechosItinerarios.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Get data from the URBS web server
            while (error):
                try:
                    content = requests.get(link)
                    error = False
                # Error in the Json content
                except ValueError:
                    attempts += 1
                    print "Failed to get JSON data (Value error)", attempts, "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
                # Requisition error
                except requests.exceptions.ConnectionError:
                    attempts += 1
                    print "Failed to get JSON data (Request error)", attempts, \
                        "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
            # Split the array and add some information
            for record in json.loads(content.text):
                record['DATE'] = datetime.datetime.now().strftime('%Y-%m-%d')
                record['BUS_LINE'] = bus_line
                # Append to the data array
                self.data.append(record)



class BusRoutes (Bus):
    def get_data(self):
        for bus_line in self.bus_lines:
            error = True
            attempts = 0
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getShapeLinha.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Get data from the URBS web server
            while (error):
                try:
                    content = requests.get(link)
                    error = False
                # Error in the Json content
                except ValueError:
                    attempts += 1
                    print "Failed to get JSON data (Value error)", attempts, "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
                # Requisition error
                except requests.exceptions.ConnectionError:
                    attempts += 1
                    print "Failed to get JSON data (Request error)", attempts, \
                        "attempts.", str(datetime.datetime.now())
                    time.sleep(120)
            # Split the array and add some information
            record = {}
            record['ROUTES'] = json.loads(content.text)
            record['DATE'] = datetime.datetime.now().strftime('%Y-%m-%d')
            record['BUS_LINE'] = bus_line
            # Append to the data array
            self.data.append(record)



class BusSchedules (Bus):
    def get_data(self):
        print "CODE HERE"



class BusVehicles (Bus):
    def get_data(self):
        print "CODE HERE"



class IO:
    def __init__(self):
        ''' Get the parameters '''
        parser = argparse.ArgumentParser(description='Crawler \
            of static data about public transportation of Curitiba from the URBS website.')
        parser.add_argument('-s', '--server', help='Name of the MongoDB server', required=True)
        parser.add_argument('-p', '--persistence', help='Name of the MongoDB \
            persistence slave', required=False)
        parser.add_argument('-d', '--database', help='Name of the MongoDB database', required=True)
        parser.add_argument('-t', '--sleep_time', help='Time sleeping seconds', required=True)
        parser.add_argument('-u', '--urbs_key', help='URBS access code', required=True)
        parser.add_argument('-f', '--control_file', help='File to control the execution time', required=True)
        parser.add_argument('-z', '--zabbix_host', help='Zabbix host for monitoring', required=True)
        parser.add_argument('-b', '--buslines_collection', help='Name of the Mongo collection\
            with the URBS bus lines', required=True)
        parser.add_argument('-m', '--metadata', help='CSV with the URBS databases metadata in two \
            columns. Format: class,collection.', required=True)
        self.args = vars(parser.parse_args())

    def read_metadata(self, metadata_file):
        ''' Read the metadata with the databases classes and collection '''
        self.databases = []
        with open(metadata_file) as infile:
            for line in infile:
                record = {}
                record['name'] = line.rstrip().split(",")[0]
                record['class'] = line.rstrip().split(",")[1]
                record['collection'] = line.rstrip().split(",")[2]
                self.databases.append(record)




class DB:
    def __init__(self, args):
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
        self.client = client
        self.database = client[args['database']]

    def get_collection(self, collection_name):
        ''' Set the collection location '''
        return self.database[collection_name]


    def send_data(self, data, collection):
        ''' Send data to the MongoDB '''
        count_insertions = 0
        for record in data:
            try:
                collection.insert_one(record)
                count_insertions += 1
            except pymongo.errors.DuplicateKeyError:
                pass
        return count_insertions



class Control:

    def __init__(self, sleep, control_file):
        ''' Set the init and sleep time, and the name of the file
        that controls the execution frequency'''
        self.init_time = time.time()
        self.sleep_time = float(sleep)
        self.control_file = control_file

    def set_end(self, number_records):
        ''' Set the duration and next execution '''
        self.duration = time.time() - self.init_time
        self.next_execution = time.time() + float(self.sleep_time) - self.duration
        if (self.sleep_time > self.duration):
            self.sleep_time -= self.duration

    def verify_next_execution(self):
        ''' Verify if the execution can proceed due to the sleep time assigned '''
        try:
            with open(self.control_file) as infile:
                input_time = float(infile.read())
        except IOError:
            input_time = time.time() + 1
        except ValueError:
            input_time = time.time() + 1
        while (time.time() <= input_time):
            print "Could not execute now", str(datetime.datetime.now())
            print "Sleeping for {} seconds. Until {}".format(
                input_time - time.time(), datetime.datetime.fromtimestamp(input_time))
            time.sleep(input_time - time.time())

    def assign_next_execution(self):
        ''' Write the data of next execution for future verifications  '''
        output_time = open(self.control_file, "w")
        output_time.write(str(self.next_execution))
        output_time.flush()
        output_time.close()
        print "Execution took", str(self.duration), "seconds."
        print "Next will occur in", float(self.sleep_time) - self.duration, "seconds."
        print "Next will occur at", datetime.datetime.fromtimestamp(self.next_execution)
        time.sleep(self.sleep_time)



class Monitoring:
    def send(self, host, item, number_records):
        ''' Send the number of records to the Zabbix monitoring system '''
        pyzabbix_sender.send(host, item, number_records, pyzabbix_sender.get_zabbix_server())
        print "Execution completed with ", number_records, "records", str(datetime.datetime.now())



if __name__ == "__main__":

        print "\n\n\nRunning Crawler Mobility Curitiba Static", \
            str(datetime.datetime.now())

        parameters = IO()
        parameters.read_metadata(parameters.args['metadata'])

        try:
            while(True):

                connection = DB(parameters.args)

                control = Control(parameters.args['sleep_time'],
                                  parameters.args['control_file'])
                control.verify_next_execution()

                for database in parameters.databases:

                    print database['name']

                    crawler = eval(database['class'])(parameters.args['urbs_key'])
                    buslines_collection = connection.get_collection(\
                        parameters.args['buslines_collection'])
                    crawler.get_latest_buslines(buslines_collection)
                    crawler.get_data()

                    collection = connection.get_collection(database['collection'])
                    insertions = connection.send_data(crawler.data, collection)

                    monitor = Monitoring()
                    monitor.send(parameters.args['zabbix_host'], database['collection'], insertions)

                connection.client.close()

                control.set_end(insertions)
                control.assign_next_execution()

        except KeyboardInterrupt:
            print u'\n\nShutting down...\n\n'
