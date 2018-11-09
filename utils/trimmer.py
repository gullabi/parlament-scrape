import os
import re
import yaml
import logging

from copy import copy
from pocketsphinx import AudioFile, get_model_path, get_data_path
from subprocess import call, Popen, PIPE

FILE_PATH = os.path.abspath(os.path.dirname(__file__))
MODEL_PATH = '/home/baybars/scripts/repositories/cmusphinx-models'
DICT_PATH = os.path.join(MODEL_PATH, 'ca-es/pronounciation-dictionary.dict')
CONV_PATH = os.path.join(FILE_PATH, 'UPC_samba2cmu_conversion.csv')
CONFIG = {
    'verbose': False,
    'audio_file': '',
    'buffer_size': 2048,
    'no_search': False,
    'full_utt': False,
    'keyphrase': None,
    'hmm': os.path.join(MODEL_PATH, 'ca-es/acoustic-model'),
    'lm': False,
    'dict': DICT_PATH,
    'frate': 100 # frames per second (default=100)
}
DICT = {line.strip().split()[0]: ' '.join(line.strip().split()[1:])\
        for line in open(CONFIG['dict']).readlines()}
CONV_DICT = {line.strip().split(',')[2]: line.strip().split(',')[3]\
             for line in open(os.path.join(CONV_PATH)).readlines()}
OUTPATH = './test'
JSGF = '''
#JSGF V1.0;

grammar words;
<word> = %s

public <words> = <word>+;
'''

class Trimmer(object):

    def __init__(self, text, audio_file, option='safe'):
        self.text = text
        self.audio_file = audio_file
        self.fsg_result_files = []

    def __str__(self):
        return 'TrimmerObject(%s)'%self.audio_file.filepath

    def crop_longaudio(self):
        text_snippets = self.get_text_start_end(self.text)
        audio_snippets, audio_offsets = self.get_audio_start_end()
        match_results = []
        for i, (text_snippet, audio_snippet) in enumerate(zip(text_snippets,
                                                         audio_snippets)):
            if i == 0:
                operation = 'beginning'
            elif i == 1:
                operation = 'ending'
            else:
                msg = 'For the file %s audio and text snippets are more than two'\
                      %audio_file
                raise ValueError(msg)
            match_results.append(self.fsg_search(text_snippet,
                                                 audio_snippet,
                                                 audio_offsets[i],
                                                 operation=operation))
        print(match_results)
        new_text = ''
        if match_results[0][0] and match_results[1][0]:
            new_text = self.get_global_text(text, match_results)
            for f in self.fsg_result_files:
                self.remove_file(f)
        return (match_results[0][0], match_results[1][0], new_text)

    @staticmethod
    def get_text_start_end(text):
        '''returns the first and last 6 words. 
           TODO clean the text?
        '''
        words = text.split()
        return [words[:6], words[-6:]]

    def get_audio_start_end(self):
        audio_filepath = self.audio_file.filepath
        out_files = []
        offsets = []
        outpath = OUTPATH
        if self.audio_file.duration > 100.:
            starts_ends = self.get_audio_limits(self.audio_file.duration)
            if starts_ends:
                for start, end in starts_ends:
                    fileout = self.audio_file.segment(start=start,
                                                      end=end,
                                                      outpath=outpath)
                    out_files.append(fileout)
                    offsets.append(start)
        else:
            fileout = self.audio_file.segment(start=0.0,
                                              end=self.audio_file.duration,
                                              outpath=outpath)
            out_files = [fileout, fileout]
            offsets = [0,0]
        return out_files, offsets

    @staticmethod
    def get_audio_limits(duration):
        '''Gives the first one minute and the last one minute of the recording
        '''
        if duration < 100.:
            logging.warning("duration too short shouldn't be here")
            return []
        return [(0.0, 60.0),(duration-60.0, duration)]

    def fsg_search(self, text_snippet, audio_snippet, offset_seconds,
                                             operation='beginning', option='safe'):
        # create grammar file for the fsg search
        fsg_file = self.generate_fsg(text_snippet, operation)

        # store the name of the file which stores the search results
        fsg_result_file = fsg_file.replace('.jsgf','.yaml')
        self.fsg_result_files.append(fsg_result_file)

        CONFIG['jsgf'] = fsg_file
        CONFIG['audio_file'] = audio_snippet
        audio = AudioFile(**CONFIG)
        result_sequence = []
        for phrase in audio:
            #print(phrase.probability(), phrase.score(), phrase.confidence())
            for s in phrase.seg():
                start_time = s.start_frame / CONFIG['frate']
                end_time = s.end_frame / CONFIG['frate']
                if start_time != end_time and s.word != '<sil>':
                    # getting rid if NULL elements and silences
                    result_sequence.append((start_time,
                                            end_time,
                                            s.word))
        with open(fsg_result_file, 'w') as out:
            yaml.dump(result_sequence, out)
        self.remove_file(fsg_file)
        # should return the best match text snippet with beginning end
        if operation == 'beginning':
            search_snippet = copy(text_snippet)
            match_result = self.find_match(result_sequence, search_snippet)
            # assert that offset_seconds is zero
            if match_result:
                result_seconds = offset_seconds + match_result[0]
            else:
                if option == 'safe':
                    result_seconds, search_snippet = None, []
                else:
                    result_seconds = offset_seconds
                    search_snippet = text_snippet
        elif operation == 'ending':
            search_snippet = copy(text_snippet)[::-1]
            match_result = self.find_match(result_sequence[::-1], search_snippet)
            if match_result:
                result_seconds = offset_seconds + match_result[1]
                search_snippet = search_snippet[::-1]
            else:
                if option == 'safe':
                    result_seconds, search_snippet = None, []
                else:
                    # get result second from the audio_snippet filename
                    m = re.search('.+_\d+\.\d+_(\d+\.\d+).wav', audio_snippet)
                    if m:
                        result_seconds = float(m.groups()[0])
                    else:
                        result_seconds = end_time # input total duration
                    search_snippet = text_snippet
        else:
            raise ValueError('option %s not known'%option)
        return result_seconds, search_snippet

    def generate_fsg(self, text_snippets, operation):
        '''Creates the grammar file and also the dictionary file if necessary
        '''
        basename = os.path.basename(self.audio_file.filepath).split('.')[0]
        CONFIG['dict'] = DICT_PATH # reverting back the dict path just in case
        # check if tokens are in phonetic dictionary
        missing_tokens = []
        for token in text_snippets:
            if token not in DICT:
                missing_tokens.append(token)
        if missing_tokens:
            g2p = self.get_token_phonemes(missing_tokens)
            # write a new dict file
            dict_filename = '_'.join([basename, operation])+'.dict'
            dict_path = os.path.join(OUTPATH, dict_filename)
            with open(dict_path, 'w') as out:
                for token in text_snippets:
                    ph = DICT.get(token) or g2p.get(token)
                    out.write('%s\t%s\n'%(token, ph))
            CONFIG['dict'] = dict_path

        # write the fsg grammar to a file 
        fsg_filename = '_'.join([basename, operation])+'.jsgf'
        fsg_path = os.path.join(OUTPATH, fsg_filename)
        fsg_query = ' | '.join(text_snippets)+';'
        with open(fsg_path,'w') as out:
            out.write(JSGF%fsg_query)
        return fsg_path

    def get_token_phonemes(self, tokens):
        cmd_st = 'espeak -vca \'%s\' --ipa=3 -q | '\
              'sed \'s/aɪ/a_j/g\' | sed \'s/v/ʋ/g\''
        g2p = {}
        for token in tokens:
            cmd = (cmd_st%token).split()
            process = Popen(cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()
            phonemes = stdout.decode('utf8').strip()
            phonemes_ls = phonemes.strip().replace('ˈ','').replace('ˌ','')\
                                          .replace(' ','_').split('_')
            cmu_phonemes = []
            for phoneme in phonemes_ls:
                cmu_phonemes.append(CONV_DICT[phoneme])
            g2p[token] = ' '.join(cmu_phonemes)
        return g2p

    def find_match(self, full_sequence, search_sequence):
        '''Searches for an exact match with gradually shorter search sequences
        '''
        match = []
        while len(search_sequence) > 2 and not match:
            match = self.sequence_match(full_sequence, search_sequence)
            if not match:
                search_sequence.pop(0)
        if not match:
            logging.error('match not found ')
        return match

    @staticmethod
    def sequence_match(full_sequence, search_sequence):
        for i, phrase in enumerate(full_sequence):
            if i > len(full_sequence)-len(search_sequence):
                continue
            match = True
            step = 0
            for search_word in search_sequence:
                match = match and (full_sequence[i+step][2] == search_word)
                step += 1
            if match:
                return phrase
        return False

    @staticmethod 
    def remove_file(filepath):
        cmd = ['rm', filepath]
        call(cmd)

    @staticmethod
    def get_global_text(text, match_results):
        start_text = ' '.join(match_results[0][1])
        end_text = ' '.join(match_results[1][1])
        start_index = text.find(start_text)
        end_index = text.find(end_text)+len(end_text)
        if start_index == -1 or end_index < len(end_text):
            msg = 'having difficulty finding the new start or/and end'
            raise ValueError(msg)
        return text[start_index:end_index]
