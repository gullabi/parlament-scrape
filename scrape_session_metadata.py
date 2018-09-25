import os
import sys
import re

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
    sessions = []
    key_convert = {'Data':'date', 'Durada':'duration'}
    llista = soup.find('ul', attrs={'class':'llista_videos'})
    for element in llista.find_all('h2'):
        session = {}
        div = element.find_parent()
        session['url'] = div.find('a').get('href')
        session['name'] = div.find('a').text
        for p in div.find_all('p'):
            # we expet to find Data: <date> or Durada: <duration>
            if ':' in p.text:
                key, value = p.text.split(':')
                session[key_convert[key.strip()]] = value.strip()
        sessions.append(session)
    return sessions

def get_session_meta(session, db):
    try:
        current_session = Session(**session)
    except TypeError as e:
        print(e)
        print(session)
        sys.exit()

    if current_session.ple_code:
        if db.backend.find_one({'_id': current_session.ple_code}):
            msg = '%s already in db. skipping'%current_session.ple_code
            print(msg)
            #logging.info(msg)
        else:
            current_session.get_interventions()
            db.insert(current_session.ple_code,
                      current_session.meta_to_dict())
            msg = '%s with %i interventions inserted to db'\
                 %(current_session.ple_code, current_sessions.no_interventions)
            print(msg)
    else:
        msg = 'no ple_code found for %s'%str(current_session)
        raise ValueError(msg)

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
    def __init__(self, url, date, name, duration):
        self.__dict__.update(locals())
        self.get_ple_code()
        self.interventions = []

    def __str__(self):
        return str(self.__dict__)

    def get_ple_code(self):
        '''Extracts the ple_code by scraping only the first
           intervention
        '''
        self.get_act_links()
        html = request_html(urljoin(base, self.act_urls[-1]))
        soup = BeautifulSoup(html, 'html.parser')
        ls = soup.find('ul', attrs={'class':'llista_videos'})
        for intervention_el in ls.findChildren('li', recursive=False):
            intervention = self.get_act_intervention(intervention_el)
            if intervention['ple_code']:
                self.ple_code = intervention['ple_code']
                break

    def get_act_links(self):
        html = request_html(urljoin(base, self.url))
        soup = BeautifulSoup(html, 'html.parser')
        act_list = soup.find('ul', attrs={'class':'pagina_llistat'})
        self.act_urls = [element.get('href') for element in act_list.find_all('a')]

    @staticmethod 
    def get_act_intervention(intervention_el):
        '''Extracts intervention metadata
           not all interventions have diari references or title urls
        '''
        code_date = None
        diari_code = None
        ple_code = None
        page_reference = None
        for element in intervention_el.find_all('p'):
            formatted_date = re.search('(\d\d)/(\d\d)/(\d{4})', element.text)
            if formatted_date:
                #TODO use groups to set up a datetime variable?
                # date is in ple_code format
                if code_date:
                    msg = 'date had already been extracted from intervention.\n'
                    print('WARNING:', msg, element.text)
                code_date = '_'.join(formatted_date.groups()[::-1])
            else:
                if 'Intervinent' in element.text:
                    intervinent = element.text.split('Intervinent:')[1].strip()
                    intervinent_links = [links.get('href')\
                                         for links in element.find_all('a')]
                elif 'Diari' in element.text:
                    diari_url = element.find('a').get('href')
                    diari_code, page = os.path.basename(diari_url).split('.')
                    m = re.search('(?<=page\=)\d+',page)
                    if m:
                        page_reference = m.group()
                    else:
                        msg = 'page not found in %s'%page
                        raise ValueError(msg)
                elif 'Durada' in element.text:
                    #TODO start end time and the duration
                    pass
                elif 'titol_pod' in element.attrs['class']:
                    title = element.text
                    link_parent = element.find('a')
                    if link_parent:
                        title_url = link_parent.get('href')
                    else:
                        title_url = None
                elif 'Javascript' in element.text:
                    pass
                else:
                    msg = 'unknown element %s'%element.text
                    print(msg)
        if diari_code and code_date:
            ple_code = '_'.join([code_date, diari_code])
            #TODO assert ple_code is the same as session ple_code
        intervention = {'intervinent':intervinent,
                        'intervinen_urls':intervinent_links,
                        'ple_code':ple_code,
                        'page_reference':page_reference,
                        'title':title,
                        'title_url':title_url}
        return intervention

    def get_interventions(self):
        if not self.act_urls:
            self.get_act_links()
        if self.interventions:
            msg = 'session already has interventions'
            print('WARNING:', msg)
        for url in self.act_urls[:1]:
            self.interventions += self.get_act_interventions(url)

    def get_act_interventions(self, url):
        html = request_html(urljoin(base, url))
        soup = BeautifulSoup(html, 'html.parser')
        ls = soup.find('ul', attrs={'class':'llista_videos'})
        page_interventions = []
        for intervention_el in ls.findChildren('li', recursive=False):
            if intervention_el.find_all('p'): 
                intervention = self.get_act_intervention(intervention_el)
                self.interventions.append(intervention)
            else:
                msg = 'no paragraphs found in %s'%intervention_el
                print('WARNING:', msg)
        return page_interventions

    def meta_to_dict(self):
        return {}

if __name__ == "__main__":
    main()
