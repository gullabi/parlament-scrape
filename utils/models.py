import re
import os

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scripts.scrape_sessions import request_html

class Session(object):
    def __init__(self, base_url, url, date, name, duration):
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
        last_act = urljoin(self.base_url, self.act_urls[-1])
        html = request_html(last_act)
        soup = BeautifulSoup(html, 'html.parser')
        ls = soup.find('ul', attrs={'class':'llista_videos'})
        for intervention_el in ls.findChildren('li', recursive=False):
            intervention = self.get_act_intervention(intervention_el)
            if intervention['ple_code']:
                self.ple_code = intervention['ple_code']
                break

    def get_act_links(self):
        html = request_html(urljoin(self.base_url, self.url))
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
        intervinents = []
        intervinent_urls = []
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
                    intervinents.append(intervinent)
                    intervinent_urls.append(intervinent_links)
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
                    m = re.search('De (.+) a (.+) - Durada', element.text)
                    if not m:
                        msg = 'Problem with start end information string %s'\
                                                                  %element.text
                        raise ValueError(msg)
                    start, end = m.groups()
                elif 'titol_pod' in element.attrs['class']:
                    title = [element.text]
                    title_url = []
                    link_parent = element.find('a')
                    if link_parent:
                        title_url.append(link_parent.get('href'))
                elif 'Javascript' in element.text:
                    pass
                elif 'mes_videos' in element.attrs['class']:
                    if 'titol_pod' not in element.find('a').get('class'):
                        msg = 'unknown element %s'%element
                        print(msg)
                    else:
                        pod_id = element.find('a').get('id')[3:]
                        title = []
                        title_url = []
                        for title_el in element.findParent()\
                                               .find('div',\
                                                     attrs={'id':pod_id})\
                                               .find_all('li'):
                            title.append(title_el.text)
                            link_parent = title_el.find('a')
                            if link_parent:
                                title_url.append(link_parent.get('href'))
                            else:
                                title_url.append(None)
                else:
                    msg = 'unknown element %s'%element.text
                    print(msg)
        if diari_code and code_date:
            ple_code = '_'.join([code_date, diari_code])
        #Extract audio url from intervention div element
        media_el = intervention_el.find('li', attrs={'class':'audio'})
        if not media_el:
            media_el = intervention_el.find('li', attrs={'class':'video'})
        media_url = media_el.find('a').get('href')
        intervention = {'intervinent':intervinents,
                        'intervinen_urls':intervinent_urls,
                        'ple_code':ple_code,
                        'page_reference':page_reference,
                        'title':title,
                        'title_url':title_url,
                        'start':start,
                        'end':end,
                        'media_url':media_url}
        return intervention

    def get_interventions(self):
        if not self.act_urls:
            self.get_act_links()
        if self.interventions:
            msg = 'session already has interventions'
            print('WARNING:', msg)
        for url in self.act_urls:
            self.interventions += self.get_act_interventions(url)

    def get_act_interventions(self, url):
        html = request_html(urljoin(self.base_url, url))
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
        session = {'name': self.name,
                   'ple_code': self.ple_code,
                   'date': self.date,
                   'url': self.url,
                   'duration': self.duration,
                   'interventions': self.interventions}
        return session

    @property
    def no_interventions(self):
        return len(self.interventions)
