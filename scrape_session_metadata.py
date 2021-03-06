from urllib.parse import urljoin
from bs4 import BeautifulSoup

from utils.models import Session
from utils.backend import PleDB
from scripts.scrape_sessions import request_html

base = 'https://www.parlament.cat/'
sessions_meta_pages = urljoin(base,
                             '/web/canal-parlament/activitat/plens/index.html')

def main():
    db = PleDB(task_name='v1')
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
    return [urljoin(sessions_meta_pages,'?p_cp20=%i'%(page_no+1)) \
            for page_no in range(no_pages)] 

def get_page_no():
    #TODO
    return 33

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
        session['base_url'] = base
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
        print(session)
        raise TypeError()

    if db.get(current_session.url):
        msg = '%s - %s already in db. skipping'%(current_session.ple_code,
                                                 current_session.name)
        print(msg)
        #logging.info(msg)
    else:
        current_session.get_interventions()
        db.insert(current_session.url,
                  current_session.meta_to_dict())
        msg = '%s - %s with %i interventions inserted to db'\
             %(current_session.ple_code,
               current_session.name,
               current_session.no_interventions)
        print(msg)

if __name__ == "__main__":
    main()
