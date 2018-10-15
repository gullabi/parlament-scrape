from utils.backend import PleDB
from collections import Counter
from unicodedata import normalize
from fuzzywuzzy import fuzz
from utils.seq_aligner import needle, finalize

import yaml
import os
import sys
import re
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
            ints = sorted(r['interventions'], key=lambda dct:dct['start'])
            r['interventions'] = ints
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
        self.match_speakers()
        if len(self.speaker_index) > 2:
            self.align_speakers()
        self.write_blocks()

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
                found = False
                for interv in [first_meta_int, last_meta_int, most_meta_int]:
                    if 'parlament' in interv.lower():
                        msg = "WARNING: using the name with parlament in it"
                        print(msg)
                        found = True
                        self.metadata_mesa = interv
                        break
                if not found:
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
                found = False
                for interv in [first_text_int, last_text_int, most_text_int]:
                    if 'parlament' in interv.lower():
                        msg = "WARNING: using the name with parlament in it"
                        print(msg)
                        found = True
                        self.text_mesa = interv
                        break
                if not found:
                    msg = 'WARNING: using the most frequent'
                    print(msg)
                    self.text_mesa = most_text_int
        msg = 'INFO: mesa pdf; %s |  mesa db; %s'%(self.text_mesa,
                                                   self.metadata_mesa)
        print(msg)
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
            if talk and 'ORDRE' not in speaker and not speaker[0].islower():
                if speaker == self.text_mesa:
                    speaker = self.mesa
                self.text_intervinents.append(speaker)
        self.text_intervinents = self.post_process_text_names(self.text_intervinents)

    def post_process_text_names(self, speakers):
        self.name_dict = {}
        for speaker in set(speakers):
            match = re.search('(.+)\((.+)\)',speaker)
            if match:
                title, name = match.groups()
                self.name_dict[self.normalize(speaker)] = name.strip()
                self.name_dict[self.normalize(title)] = name.strip()
        new = []
        for speaker in speakers:
            normalized_speaker = self.normalize(speaker)
            if self.name_dict.get(normalized_speaker):
                new.append(self.name_dict[normalized_speaker])
            else:
                new.append(speaker)
        return new

    def normalize(self, input_str):
        return self.remove_accents(input_str.strip().lower())

    @staticmethod
    def remove_accents(input_str):
        nfkd_form = normalize('NFKD', input_str)
        only_ascii = nfkd_form.encode('ASCII', 'ignore')
        return only_ascii

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

    def write_blocks(self):
        with open('blocks/%s.blk'%self.ple_code,'w') as out:
            yaml.dump([compare\
                       for compare in itertools.zip_longest(\
                                                self.metadata_blocks,\
                                                self.text_blocks)], out)

    def match_speakers(self):
        text_int_set = set(self.text_intervinents)
        metadata_int_set = set(self.metadata_intervinents)

        print('matching', len(text_int_set), 'vs', len(metadata_int_set))
        text_int_set, metadata_int_set, matched_tvsm = \
                     self.match_loop(text_int_set, metadata_int_set, 90)
        if text_int_set:
            msg = "INFO: there are unmatched pdf speakers."
            for t_int in list(text_int_set):
                for key, value in self.name_dict.items():
                    '''
                    if the pdf speaker name is written in a different way (for
                    example a ministers title is missing a word) maybe it was
                    not picked up by the self.name_dict()

                    this loop assumes that the normalized version was already
                    matched and matched_tvsm should have it
                    '''
                    if b'president' not in self.normalize(t_int) and\
                       self.match_speaker(self.normalize(t_int),
                                          key,
                                          threshold=80):
                        # not to end up with false positives of vicepresident
                        # of different types
                        msg = "INFO: %s found in name similarity dict as %s"\
                               %(t_int, value)
                        print(msg)
                        try:
                            matched_tvsm.append((t_int,\
                                       self.find_in_tuple(matched_tvsm, value)))
                            text_int_set.remove(t_int)
                        except Exception as e:
                            print('ERROR: the element in name_dict was actually'\
                                  ' not matched with any db speaker')
                            print(e)
                        break

        if text_int_set and metadata_int_set:
            text_int_set, metadata_int_set, new_matched_tvsm = \
                self.match_loop(text_int_set, metadata_int_set, 80)
            if new_matched_tvsm:
                print('INFO: found more matches comparing the initial '\
                      'list of unmatched\n', new_matched_tvsm)
            matched_tvsm += new_matched_tvsm
        if text_int_set or metadata_int_set:
            print('INFO: unmatched', text_int_set, metadata_int_set)
        else:
            print('INFO: success! All matched.')
        self.get_speaker_index_tuples(matched_tvsm,
                                      text_int_set,
                                      metadata_int_set)
        #self.matched_tvsm = matched_tvsm
        #self.text_int_set = text_int_set
        #self.metadata_int_set = metadata_int_set

    def match_loop(self, text_int_set, metadata_int_set, threshold):
        matched_tvsm = []
        for t_int in list(text_int_set):
            if metadata_int_set:
                for m_int in list(metadata_int_set):
                    if self.match_speaker(t_int.lower(),
                                          m_int.lower(),
                                          threshold=threshold):
                        #print('%s vs %s'%(t_int,m_int))
                        matched_tvsm.append((t_int, m_int))
                        metadata_int_set.remove(m_int)
                        try:
                            text_int_set.remove(t_int)
                        except:
                            print('WARNING: pdf speaker %s has two or more '\
                                  'metadata counterparts %s'%(t_int,m_int))
                        #print(text_int_set)
        return text_int_set, metadata_int_set, matched_tvsm

    def match_speaker(self, t_int, m_int, threshold=90):
        if fuzz.token_set_ratio(t_int, m_int) > threshold:
            return True
        return False

    @staticmethod
    def find_in_tuple(tpl, val):
        '''
        in a tuple searches val within in the first elements, 
        returns the corresponding second element
        '''
        for t in tpl:
            if t[0] == val:
                return t[1]
        msg = 'ERROR: tuple does not have %s in its first elements'\
               %(str(val))
        raise ValueError(msg)

    def get_speaker_index_tuples(self, tvsm, t_set, m_set):
        '''Converts the matched and unmatched speaker lists into one list of
           speaker(s) vs index tuples.
           Does not yet take into account the repetitions (only one-to-one)
        '''
        self.speaker_index = []
        index = 0
        for tm in tvsm:
            self.speaker_index.append((tm[0], tm[1], index))
            index += 1
        for t in t_set:
            self.speaker_index.append((t, '', index))
            index += 1
        for m in m_set:
            self.speaker_index.append(('', m, index))
            index += 1

    def align_speakers(self):
        '''Uses the built blocks to align the speakers
           Otherwise the repetitions in one block hinders the discovery
           of "good" alignments
        '''
        self.normalized_many2many_dict = {}
        text_block_int = [e[2] for e in self.text_blocks]
        meta_block_int = [e[2] for e in self.metadata_blocks]
        text_int_seq = self.get_sequence(text_block_int, 0)
        meta_int_seq = self.get_sequence(meta_block_int, 1)

        text_int_seq = self.replace_many2many(text_int_seq)
        meta_int_seq = self.replace_many2many(meta_int_seq)
        text_int_aligned, meta_int_aligned = needle(text_int_seq, meta_int_seq)
        finalize(text_int_aligned, meta_int_aligned)
        for elements in self.speaker_index:
            print(elements[::-1])
        print(self.normalized_many2many_dict)
        self.text_blocks = self.sequence2name(text_int_aligned, self.text_blocks, 0)
        self.metadata_blocks = self.sequence2name(meta_int_aligned, self.metadata_blocks, 1)

    def get_sequence(self, name_seq, name_index):
        seq = []
        for name in name_seq:
            index = [ref[2] for ref in self.speaker_index\
                            if ref[name_index] == name]
            for i in index[1:]:
                self.normalized_many2many_dict[i] = index[0]
            # if more than one index is found, the first index will be assigned
            # they are stored in the normalized_many2many_dict
            # the indicies will be replaced accordingly after both sequences
            # are populated
            seq.append(index[0])
        return seq

    def replace_many2many(self, seq):
        new_seq = []
        for s in seq:
            replace = self.normalized_many2many_dict.get(s)
            if replace:
                new_seq.append(replace)
            else:
                new_seq.append(s)
        return new_seq

    def sequence2name(self, seq, blocks, index):
        new_blocks = []
        for s in seq[::-1]:
            if s != '--':
                name = self.speaker_index[int(s)][index]
                if not name == blocks[-1][2]:
                    # TODO unless we invert replace_many2many operation
                    # there is nothing else to do. We just have to trust
                    # the index order
                    print('WARNING: name is not as expected.'\
                          ' Could be another equivalent name.',\
                          (s,name,blocks[-1][2]))
                new_blocks.append(blocks[-1])
                blocks.pop()
            else:
                new_blocks.append((None, None, None))
        return new_blocks[::-1]

if __name__ == "__main__":
    ple_code = sys.argv[1]
    main(ple_code)
