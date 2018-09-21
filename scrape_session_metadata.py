import os

from pymongo import MongoClient

base = 'https://www.parlament.cat'
sessions_meta_pages = base+'/web/canal-parlament/activitat/plens/index.html'
outfile = ''

def main():
    db = PleDB(task_name='test')
    db.connect()
    get_all_session_metadata(db)
    #wo data

def get_all_session_metadata(db):
    sessions = get_session_list()
    for session in sessions:
        get_session_meta(session, db)

def get_session_meta(session, db):
    current_session = Session(session)
    current_session.get_ple_code()
    if db.backend.find_one({'_id': current_session.ple_code}):
        msg = '%s already in db. skipping'%current_session.ple_code
        print(msg)
        #logging.info(msg)
    current_session.get_interventions()
    db.backend.insert(current_session.ple_code, current_session.meta_to_dict())
    msg = '%s with %i interventions inserted to db'\
           %(current_session.ple_code, current_sessions.no_interventions)
    print(msg)

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
        ref = self.cache.find_one({'_id': key})
        if not ref:
            self.backend.insert({'_id': key,
                                 'date': value['date'],
                                 'name': value['name'],
                                 'interventions': value['interventions']})
        else:
            msg = '%s is already in db. skipping'%key
            logging.info(msg)

class Session(object):
    def __init__(self, ple_code, date, name):
        self.__dict__.update(locals())

    def get_ple_code(self):
        self.ple_code = None

    def get_interventions(self):
        self.interventions = None

    def meta_to_dict(self):
        return {}
