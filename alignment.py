from utils.backend import PleDB
from collections import Counter
import yaml
import os
import sys
import itertools

def main(ple_code):
    yaml_path = 'yamls'
    task_name = 'v1'
    db = PleDB(task_name=task_name)
    db.connect()
    if ple_code == 'all':
        get_all(db)
    else:
        get_one(db, ple_code)

def get_all(db):
    for session in db.backend.find():
        if session['ple_code']:
            if 'sessió constitutiva' in session['name']\
                or 'constitució' in session['name'].lower():
                msg = 'INFO: skipping sessió constitutiva'
                continue
            print(session['ple_code'])
            yaml_file = os.path.join('yamls',session['ple_code']+'_speaker.yaml')
            if os.path.isfile(yaml_file):
                align = Alignment(ple_code=session['ple_code'], db=db)
                align.block_align()
            else:
                msg = 'WARNING: %s does not exist, skipping'%yaml_file
                print(msg)

def get_one(db, ple_code):
    align = Alignment(ple_code=ple_code, db=db)
    align.block_align()

class Alignment(object):
    def __init__(self, ple_code=None, yaml_path='yamls', db=None):
        self.db = db
        self.db.connect()

        self.ple_code = ple_code
        self.yaml_path = yaml_path
        self.get_metadata()
        self.get_text()

    def get_metadata(self):
        self.metadata = [] 
        for r in self.db.get_from_code(self.ple_code):
            self.metadata.append(r)

    def get_text(self):
        yaml_file = os.path.join(self.yaml_path,
                                 self.ple_code+'speaker.yaml') 
        self.text = yaml.load(open(os.path.join(self.yaml_path,
                                         self.ple_code+'_speaker.yaml'), 'r'))
    def block_align(self):
        self.get_mesa()
        self.get_metadata_speakers()
        self.get_text_speakers()
        self.metadata_blocks = self.get_blocks(self.metadata_intervinents, self.mesa)
        self.text_blocks = self.get_blocks(self.text_intervinents, self.mesa)
        if len(self.metadata_blocks) != len(self.text_blocks):
            msg = 'WARNING: alignment blocks are not of the same size. %i vs %i'\
                  %(len(self.metadata_blocks),len(self.text_blocks))
            print(msg)
        with open('blocks/%s.blk'%self.ple_code,'w') as out:
            yaml.dump([compare\
                       for compare in itertools.zip_longest(\
                                                self.metadata_blocks,\
                                                self.text_blocks)], out)

    def get_mesa(self):
        all_metadata_intervinents = []
        all_text_intervinents = []
        for m in self.metadata:
            for intervention in m['interventions']:
                all_metadata_intervinents += intervention['intervinent']
        first_meta_int = all_metadata_intervinents[0]
        last_meta_int = all_metadata_intervinents[-1]
        most_meta_int = Counter(all_metadata_intervinents).most_common()[0][0]

        for t in self.text:
            speaker = list(t.keys())[0]
            text = t[speaker]
            if text and 'ORDRE' not in speaker:
                all_text_intervinents.append(speaker)
        first_text_int = all_text_intervinents[0]
        last_text_int = all_text_intervinents[-1]
        most_text_int = Counter(all_text_intervinents).most_common()[0][0]

        if first_meta_int == most_meta_int and last_meta_int == most_meta_int:
            self.metadata_mesa = first_meta_int
        else:
            msg = 'WARNING: mesa person not found.\n'\
                  'ple_code:%s, first:%s, last:%s, most:%s'\
                  %(self.ple_code, first_meta_int, last_meta_int, most_meta_int)
            print(msg)
            if first_meta_int == most_meta_int:
                print('using first and the most')
                self.metadata_mesa = first_meta_int
            elif last_meta_int == first_meta_int:
                print('using first and the last')
                self.metadata_mesa = last_meta_int
            elif last_meta_int == most_meta_int:
                print('using most and the last')
                self.metadata_mesa = last_meta_int
            else:
                msg = 'WARNING: using the most frequent'
                print(msg)
                self.metadata_mesa = most_meta_int

        if first_text_int == most_text_int and last_text_int == most_text_int:
            self.text_mesa = first_text_int
        else:
            msg = 'WARNING: mesa person not found.\n'\
                  'ple_code:%s, first:%s, last:%s, most:%s'\
                  %(self.ple_code, first_text_int, last_text_int, most_text_int)
            print(msg)
            if first_text_int == most_text_int:
                print('using the first and the most')
                self.text_mesa = first_text_int
            elif first_text_int == last_text_int:
                print('using the first and the last')
                self.text_mesa = last_text_int
            elif most_text_int == last_text_int:
                print('using the most and the last')
                self.text_mesa = last_text_int
            else:
                msg = 'WARNING: using the most frequent'
                print(msg)
                self.text_mesa = most_text_int

        self.mesa = 'mesa'

    def get_metadata_speakers(self):
        if not self.metadata_mesa:
            self.get_mesa()
        self.metadata_intervinents = []
        metadata_int_structured = []
        for m in self.metadata:
            for intervention in m['interventions']:
                ints = []
                for intervinent in intervention['intervinent']:
                    if self.metadata_mesa in intervinent:
                        speaker = self.mesa
                    else:
                        speaker = intervinent.split('|')[0].strip().replace('\xa0',' ')
                        #print(speaker)
                    ints.append(speaker)
                metadata_int_structured.append(ints)
        self.metadata_intervinents = self.post_process_db(metadata_int_structured)
        return metadata_int_structured

    @staticmethod
    def post_process_db(lines):
        processed = []
        for line in lines:
            if len(line) == 1:
                processed.append(line[0])
            else:
                index = -1
                try:
                    processed.append(line[0])
                except:
                    print('WARNING: determined "mesa" not in list')
                if index != -1:
                    line.pop(index)
                if len(line) > 2:
                    print('after post-process there are still multiple intervinents')
                    print(line)
                else:
                    processed.append(line[0])
        return processed 

    def get_text_speakers(self):
        self.text_intervinents = []
        for d in self.text:
            speaker = list(d.keys())[0]
            talk = d[speaker]
            if talk and 'ORDRE' not in speaker:
                if speaker == self.text_mesa:
                    speaker = self.mesa
                self.text_intervinents.append(speaker)

    @staticmethod 
    def get_blocks(ls, pause):
        uniques = []
        for p in ls: 
            if not p == pause:
                if not uniques:
                    uniques.append(p)
                else:
                    if p != uniques[-1]:
                        uniques.append(p)

        search_beginning = False
        blocks = []
        end = 0
        for u in uniques:
            search_beginning = True
            #print(u)
            for i, p in enumerate(ls):
                if i > end:
                    if search_beginning:
                        if p == pause or p == u:
                            start = i
                            search_beginning = False
                            #print('start',start)
                    else:
                        if p != u and p != pause:
                            end = i-1
                            #print('end',end)
                            break
            search_beginning = True
            if end < start:
                end = start
            blocks.append((start,end,u))
        return blocks

if __name__ == "__main__":
    ple_code = sys.argv[1]
    main(ple_code)
