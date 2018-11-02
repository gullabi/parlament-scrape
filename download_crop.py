import os
import re
import sys
import yaml
import pickle

from subprocess import call, check_output, DEVNULL
from multiprocessing.dummy import Pool
#from tqdm import *

lexicon = 'utils/lexicon_set_ca.bin'
with open(lexicon, 'rb') as lexicon_file:
    lexicon_set = pickle.load(lexicon_file)

token_clean = '\.|,|:|;|!|\?|"|\.\.\.|\(|\)|-|-#| - |’|‘|¿|¡| · | \' |<.+>'

def main(option):
    if option == 'all':
        candidates = get_candidates()
    else:
        candidates = get_candidate(option)
    '''
    download_media(candidates)
    crop_media(candidates)
    '''

def get_candidates(path='sessions'):
    candidates = []
    for session in os.listdir(path):
        texts = get_canditate(session)
        if texts:
            candidates += texts
    return candidates

def get_candidate(session, path='sessions'):
    texts = []
    speakers_file = os.path.join(path, session, 'aligned_speakers.ls')
    session_text_path = os.path.join(path, session, 'text')
    if not os.path.exists(speakers_file) or\
       not os.listdir(session_text_path):
        msg = 'speaker alignments of text files do not exist'
        print('WARNING: %s for %s'%(msg,session))
        return False
    for textfile in os.listdir(session_text_path):
        filepath = os.path.join(session_text_path, textfile)
        with open(filepath) as read,\
             open(speakers_file) as sf:
            text_dict = yaml.load(read)
            speakers = yaml.load(sf)
            if process_text(text_dict, speakers):
                texts.append(text_dict)
            else:
                print('%s rejected'%filepath)
    return texts

def process_text(interventions, speakers):
    '''Check for certain properties, if acceptable returns a 
       cleaned text with a media url
    '''
    if len(interventions['urls']) > 1:
        return False
    text_mesa, metadata_mesa = speakers[0]
    intervinents_ls = [intervention[0].strip() for intervention in interventions['text']]
    intervinents = set(intervinents_ls)
    intervinents.remove(text_mesa)
    if len(intervinents) > 1:
        msg = "potentially more than one speaker found"
        print('WARNING: %s %s'%(msg,str(intervinents)))

    # if mesa intervention in the beginning or in the end, remove them
    for i in [0, -1]:
        intervention = interventions['text'][i]
        if intervention[0].strip() == text_mesa.strip():
            interventions['text'].pop(i)

    full_text = ''
    # clean text for each speaker
    for intervention in interventions['text']:
        full_text += intervention[1]

    if not is_catalan(full_text):
        return False

    for intervention in interventions['text']:
        cleaned_text = clean_text(intervention[1])
        intervention = (intervention[0], cleaned_text)

    return True

def clean_text(text):
    return text

def is_catalan(text, threshold=0.7):
    tokens = re.sub(token_clean, '', text).lower().split()
    if tokens:
        tokens_in_language = 0
        no_tokens = len(set(tokens))
        tokens_in_language = len(set(tokens).intersection(lexicon_set))
        score = float(tokens_in_language)/float(no_tokens)
        if score < threshold:
            print('%2.2f not catalan'%(1.0-score))
            return False
    return True 

if __name__ == "__main__":
    option = sys.argv[1]
    main(option)
