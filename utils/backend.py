from pymongo import MongoClient

import hashlib

class PleDB(object):
    def __init__(self, task_name='v1'):
        self.db_name = 'plens'
        self.collection_name = task_name

    def connect(self):
        client = MongoClient("localhost", 27017)
        db = client[self.db_name]
        self.backend = db[self.collection_name]

    def insert(self, key, value):
        # element should be a dictionary with elements meta and time
        if not key or not value:
            print(key,': ',value)
            raise ValueError('session does not have a key or empty')
        # check if exists
        h = hashlib.md5(key.encode('utf8')).hexdigest()
        ref = self.backend.find_one({'_id': h})
        if not ref:
            value['_id'] = h
            self.backend.insert(value)
        else:
            msg = '%s is already in db. skipping'%key
            logging.info(msg)

    def get(self, element):
        if type(element) == str:
            key = element
        elif type(element) == dict:
            if not element.get('url'):
                msg = 'queried element does not have a key'\
                      ' i.e. a url element'
                raise ValueError(msg)
            key = element['url']
        h = hashlib.md5(key.encode('utf8')).hexdigest()
        ref = self.backend.find_one({'_id':h})
        if not ref:
            return None
        return ref
