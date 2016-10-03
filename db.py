import json
import pymongo
import time
import datetime

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
