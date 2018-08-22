from lxml import etree
from html import unescape
from math import isclose
from collections import Counter
import yaml
import re
import os

def main():
    directory = 'xml_tests02'
    for filename in os.listdir(directory):
        if filename.endswith('.xml'):
            filepath = os.path.join(directory,filename)
            p = parseXML(filepath)
            p.parse_xml()
            p.output_lines()


class parseXML(object):
    def __init__(self, filename):
        self.filename = filename
        self.read_xml()
        self.get_properties()

    def read_xml(self):
        xml = open(self.filename,'rb').read()
        self.elements = etree.XML(xml)

    def get_properties(self):

        page_attributes = [(e.attrib['width'], e.attrib['height']) \
                                        for e in self.elements.xpath('//page')]
        page_attributes = list(set(page_attributes))
        if len(page_attributes) > 1:
            print("Warning: pages of different sizes")
            print('width, height: ',page_attributes)
        self.page_width = float(page_attributes[-1][0])
        self.page_height = float(page_attributes[-1][1])

        self.get_page_structure()
        lines = []
        for page in self.pages:
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
                line_dict['text'] = self.get_fulltext(line)
                line_dict['page'] = i
                lines.append(line_dict)
            self.pages.append(lines)

    def get_column_size(self, lines):
        lefts = [float(t['left']) for t in lines]
        unique_lefts = Counter(lefts)

        no = sum(unique_lefts.values())
        cumulative = 0
        self.content_columns = []
        for i, t in enumerate(unique_lefts.most_common()):
            cumulative += t[1]
            self.content_columns.append(t[0])
            if cumulative/no > 0.75:
                break
        if i == 3:
            self.column_size = 2
        elif i == 1:
            self.column_size = 1

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

    def filter_lines(self):
        self.ordered_lines = []
        self.content_columns.sort()
        columns_left = [self.content_columns[:2], self.content_columns[2:]]
        self.eliminated = []
        for i, page in enumerate(self.pages):
            columns = [[],[]]
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
            self.ordered_lines += columns[0]+columns[1]
        print(len(self.ordered_lines))

    def merge_columns(self, page):
        lines = page.xpath('text')
        for i, line in enumerate(lines):
            if i != 0:
                if isclose(float(line.attrib['top']),\
                           float(lines[i-1].attrib['top']),\
                           abs_tol=1):
                    if isclose(float(lines[i-1].attrib['left'])+\
                               float(lines[i-1].attrib['width']),\
                               float(line.attrib['left']),\
                               abs_tol=1):
                        # do smt
                        new_text = self.get_fulltext(lines[i-1])+\
                                   self.get_fulltext(line)

    @staticmethod 
    def attribute2dict(attribute):
        return {key:value for key, value in attribute.items()}

    @staticmethod
    def get_fulltext(line):
        m = re.match('(\<text.*?\>)(.+)(</text\>)',\
                                           etree.tostring(line).decode('utf8'))
        return unescape(m.group(2))

    def output_lines(self):
        with open(self.filename.replace('.xml','_out.yaml'),'w') as w:
            yaml.dump(self.ordered_lines,w)
        with open(self.filename.replace('.xml','.txt'),'w') as w:
            for line in self.ordered_lines:
                w.write('%s\n'%line['text'])
        with open(self.filename.replace('.xml','_deleted.yaml'),'w') as w:
            yaml.dump(self.eliminated, w)

if __name__ == "__main__":
    main()
