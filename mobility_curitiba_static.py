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
from control import Control
from monitoring import Monitoring
from db import DB



class Bus:
    def __init__(self, key, query_collection):
        self.key = key
        self.data = []
        # Source collection is the name of the collection with the list
        # of objects to be searched, For example, to get the buses schedule,
        # it is necessary to know the existing bus lines. Thus, the query_collection
        # is the collection with the list of existing bus lines.
        self.source_collection = query_collection

    def get_latest_buslines(self):
        # Get the date of the most recent bus line collected
        latest_date = self.source_collection.find().sort([['_id', pymongo.DESCENDING]]).limit(1)[0]['DATA']
        # Get the latest existing bus lines
        latest_bus_lines = self.source_collection.find({'DATA': latest_date})
        self.bus_lines = []
        for bus_line in latest_bus_lines:
            self.bus_lines.append(bus_line['COD'])

    def get_latest_vehicles(self):
        # Get the date of the most recent bus line collected
        latest_date = self.source_collection.find().sort([['_id', pymongo.DESCENDING]]).limit(1)[0]['DATE']
        # Get the latest existing bus lines
        latest_bus_vehicles = self.source_collection.find({'DATA': latest_date})
        self.bus_vehicles = []
        for bus_vehicle in latest_bus_vehicles:
            if not bus_vehicle['PREFIXO'] in self.bus_vehicles:
                self.bus_vehicles.append(bus_vehicle['PREFIXO'])

    def request(self, link):
        error = True
        attempts = 0
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
        return json.loads(content.text)


class BusLines (Bus):
    def get_data(self):
        link = "http://transporteservico.urbs.curitiba.pr.gov.br/getLinhas.php?c=" + self.key
        for record in self.request(link):
            if record.has_key('COD'):
                record['DATA'] = datetime.datetime.now().strftime("%Y-%m-%d")
                self.data.append(record)
            else:
                pass



class BusStops (Bus):
    def get_data(self):
        self.get_latest_buslines()
        for bus_line in self.bus_lines:
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getPontosLinha.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Split the array and add some information
            for record in self.request(link):
                record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
                record['COD_LINHA'] = bus_line
                # Append to the data array
                self.data.append(record)


class BusPaths (Bus):
    def get_data(self):
        self.get_latest_buslines()
        for bus_line in self.bus_lines:
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getTrechosItinerarios.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Split the array and add some information
            for record in self.request(link):
                record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
                record['COD_LINHA'] = bus_line
                # Append to the data array
                self.data.append(record)



class BusRoutes (Bus):
    def get_data(self):
        self.get_latest_buslines()
        for bus_line in self.bus_lines:
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getShapeLinha.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Split the array and add some information
            record = {}
            record['COORDENADAS'] = self.request(link)
            record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
            record['COD_LINHA'] = bus_line
            # Append to the data array
            self.data.append(record)



class BusSchedules (Bus):
    def get_data(self):
        self.get_latest_buslines()
        for bus_line in self.bus_lines:
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getTabelaLinha.php?linha=" + str(
                bus_line) + "&c=" + self.key
            # Split the array and add some information
            for record in self.request(link):
                record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
                record['COD_LINHA'] = bus_line
                # Append to the data array
                self.data.append(record)



class BusVehicles (Bus):
    def get_data(self):
        self.get_latest_vehicles()
        for bus_vehicle in self.bus_vehicles:
            link = "http://transporteservico.urbs.curitiba.pr.gov.br/getTabelaVeiculo.php?carro=" + str(
                bus_vehicle) + "&c=" + self.key
            # Split the array and add some information
            for record in self.request(link):
                record['DATA'] = datetime.datetime.now().strftime('%Y-%m-%d')
                # Append to the data array
                self.data.append(record)





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
        parser.add_argument('-m', '--metadata', help='CSV with the URBS databases metadata in two \
            columns. Format: class,collection.', required=True)
        self.args = vars(parser.parse_args())

    def read_metadata(self, metadata_file):
        ''' Read the metadata with the databases classes and collection '''
        self.databases = []
        with open(metadata_file) as infile:
            for line in infile:
                columns = line.rstrip().split(",")
                record = {}
                record['name'] = columns[0]
                record['class'] = columns[1]
                record['collection'] = columns[2]
                record['source_collection'] = columns[3]
                self.databases.append(record)





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

                    if (database['source_collection']):
                        source_collection = connection.get_collection( \
                            database['source_collection'])
                    else:
                        source_collection = None

                    crawler = eval(database['class'])(parameters.args['urbs_key'], source_collection)
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
