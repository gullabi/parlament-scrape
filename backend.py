from pymongo import MongoClient

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
        ref = self.backend.find_one({'_id': key})
        if not ref:
            self.backend.insert({'_id': key,
                                 'date': value['date'],
                                 'name': value['name'],
                                 'interventions': value['interventions']})
        else:
            msg = '%s is already in db. skipping'%key
            logging.info(msg)
