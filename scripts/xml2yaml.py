from lxml import etree
from html import unescape
from math import isclose
from collections import Counter
import yaml
import re
import os

def main():
    directory = '../../../parlament-scrape/xmls'
    out_path = 'yamls'
    #out_path = 'xml_tests02'
    for filename in os.listdir(directory):
        if filename.endswith('.xml'):
            print(filename)
            out_file = os.path.join(out_path,filename).replace('.xml','_speaker.yaml')
            filepath = os.path.join(directory,filename)
            if not os.path.exists(out_file):
                p = parseXML(filepath)
                p.parse_xml()
                p.output_lines(out_path=out_path)
            else:
                print('INFO: file exists. skipping...')


class parseXML(object):
    def __init__(self, filename):
        self.filename = filename
        self.read_xml()
        self.get_properties()

        self.speaker_font = None
        self.speaker_height = None
        self.speaker_tree = []

    def read_xml(self):
        xml = open(self.filename,'rb').read()
        self.elements = etree.XML(xml)

    def get_properties(self):

        page_attributes = [(e.attrib['width'], e.attrib['height']) \
                                        for e in self.elements.xpath('//page')]
        page_attributes = list(set(page_attributes))
        if len(page_attributes) > 1:
            print("WARNING: pages of different sizes")
            print(' width, height: ',page_attributes)
        self.page_width = float(page_attributes[-1][0])
        self.page_height = float(page_attributes[-1][1])

        self.get_page_structure()
        lines = []
        for page in self.pages[2:]:
            # usually the main text starts after the second page
            # it is not a problem to skip the cover pages to detect
            # column size, main text font and the header/footer
            # in fact in the case of little main text, column extract
            # does not work correctly
            for line in page:
                lines.append(line)

        self.get_column_size(lines)
        self.get_text_font(lines)
        self.get_header_footer(lines)

    def get_page_structure(self):
        self.pages = []
        for i, page in enumerate(self.elements.xpath('//page')):
            lines = []
            for line in page.xpath('text'):
                line_dict = self.attribute2dict(line.attrib)
                try:
                    line_dict['text'] = self.get_fulltext(line)
                except Exception as e:
                    print(e)
                    print('WARNING: text was not found for')
                    print(etree.tostring(line))
                    print(' inserting empty string')
                    line_dict['text'] = ''
                line_dict['page'] = i
                lines.append(line_dict)
            self.pages.append(lines)

    def get_column_size(self, lines):
        fraction = 0.75
        found = False
        while not found:
            found = self.detect_column_size(lines,
                                            fraction_of_lefts=fraction)
            fraction += 0.025

    def detect_column_size(self, lines, fraction_of_lefts=0.75):
        lefts = [float(t['left']) for t in lines]
        unique_lefts = Counter(lefts)

        no = sum(unique_lefts.values())
        cumulative = 0
        self.content_columns = []
        for i, t in enumerate(unique_lefts.most_common()):
            cumulative += t[1]
            self.content_columns.append(t[0])
            if cumulative/no > fraction_of_lefts:
                break
        if i == 3:
            self.column_size = 2
        elif i == 1:
            self.column_size = 1
        else:
            print('WARNING: something went wrong with column'\
                             ' size detection. %i left(s) have '\
                             '%1.3f of text '%(i+1,fraction_of_lefts))
            return False
        print('INFO: %i column(s) detected with the content cols'\
              %self.column_size,
               self.content_columns)
        return True

    def get_text_font(self, lines):
        fonts = [t['font'] for t in lines]
        self.fonts_counter = Counter(fonts)
        self.text_font = self.fonts_counter.most_common(1)[0][0]

    def get_header_footer(self, lines):
        tops = []
        for line in lines:
            # pick up only the ones which confusion with the main text
            # is possible i.e. same left margin but do not have the text_font
            # the left margin check is only applicable because in filter_lines
            # we get rid of the rest
            if line['font'] != self.text_font and \
               float(line['left']) in self.content_columns:
                tops.append((line['top'],line['font'],line['text']))
        top_counter = Counter(tops)
        h_f = [top for top in top_counter.most_common() if top[1]>1]
        self.header_footer = {}
        if not h_f:
            pass
        elif len(h_f) > 2:
            # suspicious header footer
            # we take the top results and check if they are at the very 
            # top or bottom
            tops = [float(h_f[0][0][0]), float(h_f[1][0][0])]
            print("WARNING: Header and footer might be wrong.")
            print(h_f[:4])
            if min(tops) < self.page_height*0.1 and \
               max(tops) > self.page_height*0.89:
                print(" choosing the most probable two")
                h_f = h_f[:2]
            else:
                print(" not header or footer")
                h_f = None
        elif len(h_f) < 2:
            h_f = None
        if h_f:
            self.header_footer['top'] = [h_f[0][0][0], h_f[1][0][0]]
            self.header_footer['font'] = h_f[0][0][1] 
            #TODO check if both fonts are the same
            print(self.header_footer)

    def parse_xml(self):
        self.filter_lines()
        self.get_speakers()

    def filter_lines(self):
        self.filtered_lines = []
        self.content_columns.sort()
        columns_left = [self.content_columns[:2], self.content_columns[2:]]
        self.eliminated = []
        for i, page in enumerate(self.pages):
            columns = [[],[]]
            page = self.merge_columns(page)
            for line in page:
                if self.header_footer:
                   if line['top'] in self.header_footer['top']:
                        continue
                if float(line['left']) in columns_left[0]:
                    columns[0].append(line)
                elif float(line['left']) in columns_left[1]:
                    columns[1].append(line)
                else:
                    self.eliminated.append(line) 
            self.filtered_lines += columns[0]+columns[1]
        print(len(self.filtered_lines))

    def merge_columns(self, lines):
        new_lines = []
        while len(lines) != 0:
            if len(new_lines) == 0:
                new_lines.append(lines[0])
                lines.pop(0)
            else:
                found_continuation = False
                if isclose(float(new_lines[-1]['top']),\
                           float(lines[0]['top']),\
                           abs_tol=1):
                    if isclose(float(new_lines[-1]['left'])+\
                               float(new_lines[-1]['width']),\
                               float(lines[0]['left']),\
                               abs_tol=1):
                        new_text = new_lines[-1]['text']+lines[0]['text']
                        new_lines[-1]['text'] = new_text
                        new_lines[-1]['width'] = str(int(new_lines[-1]['width'])+\
                                                     int(lines[0]['width'])-1)
                        lines.pop(0)
                        found_continuation = True
                if not found_continuation:
                    new_lines.append(lines[0])
                    lines.pop(0)
        return new_lines

    @staticmethod 
    def attribute2dict(attribute):
        return {key:value for key, value in attribute.items()}

    @staticmethod
    def get_fulltext(line):
        m = re.match('(\<text.*?\>)(.+)(</text\>)',\
                                           etree.tostring(line).decode('utf8'))
        return unescape(m.group(2))

    def get_speakers(self):
        self.get_speaker_properties()
        self.merge_speakers()
        #print(self.filename, self.speakers)
        self.create_speaker_tree()

    def get_speaker_properties(self):
        if not self.filtered_lines:
            raise ValueError('filtered_lines does not exist, '
                             'cannot extract speakers ')
        fonts = [line['font'] for line in self.filtered_lines]
        font_counter = Counter(fonts).most_common()
        if font_counter[0][0] != self.text_font:
            raise ValueError('The most common font in %s is not %s'\
                             'but %s'%(self.filename,
                                       self.text_font,
                                       font_counter[0][0]))
        for font, count in font_counter[1:]:
            speaker_vs_height = {line['text'].strip().lower():line['height']\
                            for line in self.filtered_lines\
                            if line['font'] == font}
            # dangerous: if key is not unique it will be overwritten
            for speaker in set(speaker_vs_height):
                if speaker.lower().startswith('el president') or\
                   speaker.lower().startswith('la presidenta') or\
                   speaker.lower().startswith('la vicepresidenta') or\
                   speaker.lower().startswith('el vicepresident') or\
                   speaker.lower().startswith('<b>'):
                    key = speaker
                    self.speaker_font = font
                    self.speaker_height = speaker_vs_height.get(key)
                    print('INFO: Speaker font vs height found for %s, %s'\
                           %(font, self.speaker_height))
                    break
            if self.speaker_font:
                # if speaker font found, break from the counter loop
                break
        if not self.speaker_font or not self.speaker_height:
            print('columns', self.content_columns)
            raise ValueError('Speaker fonts are not found for %s:\n%s'\
                             %(self.filename,str(font_counter)))

    def merge_speakers(self):
        speakers = set({line['text'].strip():True\
                             for line in self.filtered_lines\
                             if line['font'] == self.speaker_font and\
                                line['height'] == self.speaker_height})
        merged_speakers = []
        for i, line in enumerate(self.filtered_lines):
            if i == 0:
                continue
            current_sp = line['text'].strip()
            prior_line = self.filtered_lines[i-1]
            prior_sp = prior_line['text'].strip()
            if prior_sp in speakers and\
               line['text'] != self.text_font:
                # only merges two consecutive lines when the latter 
                # line has a font different then the main text
                # this allows for merging lines which have fonts
                # other than the self.speaker_font
                if isclose(float(prior_line['top'])+\
                           float(prior_line['height']),\
                           float(line['top']),abs_tol=2):
                    print('merging speakers',prior_sp,current_sp)
                    if current_sp in speakers:
                        speakers.remove(current_sp)
                    speakers.remove(prior_sp)
                    speakers.add(prior_line['text']+line['text'])
        self.speakers = speakers

    def create_speaker_tree(self):
        if not self.speakers:
            raise ValueError('ERROR: Cannot create speaker trees'\
                             ' speakers are not parsed')
        current_speaker = None
        speaker_discourse = {}
        for line in self.filtered_lines:
            if line['text'].strip() in self.speakers:
                if speaker_discourse:
                    # append the current discourse to the list
                    if not list(speaker_discourse.values()):
                        print('WARNING: no text for speaker',\
                              current_speaker)
                    self.speaker_tree.append(speaker_discourse)
                current_speaker = line['text'].strip()
                speaker_discourse = {current_speaker:''}
                print(current_speaker)
            else:
                if current_speaker:
                    # skips the first pages
                    speaker_discourse[current_speaker] += line['text']
        if speaker_discourse:
            # the last intervention needs to be added
            self.speaker_tree.append(speaker_discourse)

    def output_lines(self, out_path=None, debug=False):
        if out_path:
            out_base_filename = os.path.join(out_path,
                                             os.path.basename(self.filename))
        else:
            out_base_filename = self.filename
        if debug:
            with open(out_base_filename.replace('.xml','_out.yaml'),'w') as w:
                yaml.dump(self.filtered_lines,w)
            with open(out_base_filename.replace('.xml','.txt'),'w') as w:
                for line in self.filtered_lines:
                    w.write('%s\n'%line['text'])
        with open(out_base_filename.replace('.xml','_deleted.yaml'),'w') as w:
            yaml.dump(self.eliminated, w)
        if self.speaker_tree:
            with open(out_base_filename.replace('.xml','_speaker.yaml'),'w') as w:
                yaml.dump(self.speaker_tree, w)

if __name__ == "__main__":
    main()
