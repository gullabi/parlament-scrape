import os

from pymongo import MongoClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrape_sessions import request_html

base = 'https://www.parlament.cat/'
sessions_meta_pages = urljoin(base,
                             '/web/canal-parlament/activitat/plens/index.html')

def main():
    db = PleDB(task_name='test')
    db.connect()
    get_all_session_metadata(db)

def get_all_session_metadata(db):
    sessions = get_session_list()
    for session in sessions:
        get_session_meta(session, db)

def get_session_list():
    msg = "scraping the list of sessions"
    print(msg)
    pages = get_session_pages()
    sessions = []
    for page in pages:
        sessions += extract_sessions(page)
    msg = "%i sessions extracted from %i pages"%(len(sessions), len(pages))
    print(msg)
    return sessions

def get_session_pages():
    no_pages = get_page_no()
    return [urljoin(sessions_meta_pages,'?p_cp20=',page_no+1) \
            for page_no in range(no_pages)] 

def get_page_no():
    #TODO
    return 1

def extract_sessions(url):
    html = request_html(url)
    soup = BeautifulSoup(html,'html.parser')
    sessions = parse_for_sessions(soup)
    return sessions

def parse_for_sessions(soup):
    #TODO
    return [] 

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

if __name__ == "__main__":
    main()
