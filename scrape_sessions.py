import requests
import os
import re
import yaml

from bs4 import BeautifulSoup
from urllib.parse import urljoin
#from retrying import retry

base = 'https://www.parlament.cat'
sessions_pages = base+'/web/documentacio/publicacions/diari-ple/index.html?p_pagina=%i'

out_path = 'pdfs'

def main():
    sessions = scrape_sessions()
    #sessions = yaml.load(open('sessions.yml'))
    download_session_files(sessions)
            

def scrape_sessions():
    pages = [sessions_pages%(i+1) for i in range(20)]
    sessions_dict = {}
    sessions = []
    for page in pages:
        print('page %s'%page)
        sessions += get_sessions(page)
    with open('sessions.yml','w') as w:
        yaml.dump(sessions,w)
    return sessions

def get_sessions(link):
    html = request_html(link)
    soup = BeautifulSoup(html,'html.parser')
    sessions = parse_sessions(soup)

    return sessions

#@retry(stop_max_attempt_number=3, wait_fixed=1000) 
def request_html(base_url):
    html = requests.get(base_url)
    if html.status_code != 200:
        raise ConnectionError('ERROR: cannot reach %s'%base_url)
    return html.text

def parse_sessions(soup):
    sessions = []
    for th in soup.find_all('th', attrs={'class':'col_document_s icona'}):
        href = th.find('a').get('href').strip()
        key = ''
        date = ''
        for tr in th.find_next_siblings():
            classes = tr.get('class')
            if len(classes) > 1:
                print('WARNING: more classes than expected %s'%classes)
            if classes[0] == 'col_descripcio':
                key = tr.text.strip()
            elif classes[0] == 'col_data':
                date = tr.text.strip()
        if not key or not date:
            raise ValueError("Expected elements not found.")
        filename = gen_name(date,href)
        sessions.append([date, key, base+href, filename])
    return sessions

def gen_name(date,href):
    return '_'.join(date.split('/')[::-1])+'_'+href.split('/')[-1]

def download_session_files(sessions):
    for session in sessions:
        if not os.path.exists(os.path.join(out_path,session[-1])):
            download_file(session[2],session[3],out_path=out_path)
    return True

def download_file(url, filename, out_path=''):
    if out_path:
        local_filename = os.path.join(out_path,filename)
    else:
        local_filename = filename
    print('downloading %s'%local_filename)
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename
 
if __name__ == "__main__":
    main()
