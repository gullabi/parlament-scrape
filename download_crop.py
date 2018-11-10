import os
import re
import sys
import yaml
import pickle
import logging
import itertools

from datetime import datetime, timedelta
from multiprocessing import Pool
from tqdm import *

from utils.clean import structure_clean, punctuation_normalize, hyphenfix,\
                        tokenize
from utils.download import download_files, download_files_star,\
                           create_local_paths
from utils.audio import Audio
from utils.trimmer import Trimmer

lexicon = 'utils/lexicon_set_ca.bin'
with open(lexicon, 'rb') as lexicon_file:
    lexicon_set = pickle.load(lexicon_file)

token_clean = '\.|,|:|;|!|\?|"|\.\.\.|\(|\)|–|-#| - |’|‘|¿|¡| · | \' |\<i\>|\</i\>'

def main(option):
    cache_file = 'processed_session_texts.json'
    base_path = 'audio'
    if os.path.isfile(cache_file):
        candidates = yaml.load(open(cache_file, 'r'))
    else:
        candidates = {}

    if option == 'all':
        candidates = get_candidates(candidates)
    else:
        if not candidates.get(option):
            candidate = get_candidate(option)
            if candidate:
                candidates[option] = candidate
        else:
            logging.info('session %s already processed skipping'%option)
    if candidates:
        with open('processed_session_texts.json', 'w') as out:
            yaml.dump(candidates, out)
    #download_media(candidates, threads=3, base_path=base_path)
    crop_media(candidates, base_path)

def get_candidates(candidates, path='sessions'):
    for session in os.listdir(path):
        if not candidates.get(session):
            logging.info('processing session %s'%session)
            texts = get_candidate(session)
            if texts:
                candidates[session] = texts
    return candidates

def get_candidate(session, path='sessions'):
    texts = {}
    speakers_file = os.path.join(path, session, 'aligned_speakers.ls')
    session_text_path = os.path.join(path, session, 'text')
    if not os.path.exists(speakers_file) or\
       not os.listdir(session_text_path):
        msg = 'speaker alignments of text files do not exist'\
              ' for %s'%session
        logging.warning(msg)
        return False
    for textfile in os.listdir(session_text_path):
        filepath = os.path.join(session_text_path, textfile)
        with open(filepath) as read,\
             open(speakers_file) as sf:
            text_dict = yaml.load(read)
            speakers = yaml.load(sf)
            msg = 'processing %s'%filepath
            logging.info(msg)
            if process_text(text_dict, speakers):
                texts[filepath] = text_dict
            else:
                msg = '%s rejected'%filepath
                logging.info(msg)
    return texts

def process_text(interventions, speakers):
    '''Check for certain properties, if acceptable returns a 
       clean text with a media url
    '''
    if len(interventions['urls']) > 1:
        return False
    text_mesa, metadata_mesa = speakers[0]
    intervinents_ls = [intervention[0].strip() for intervention in interventions['text']]
    intervinents = set(intervinents_ls)
    try:
        intervinents.remove(text_mesa)
    except:
        msg = 'Mesa speaker not found in intervinents for the text'\
              ' %s...'%str(interventions['text'])[:1000]
        logging.warning(msg)
    if len(intervinents) > 1:
        msg = "potentially more than one speaker found"\
              "%s"%(str(intervinents))
        logging.warning(msg)
    intervinent = list(intervinents)[0]

    # if mesa intervention in the beginning or in the end, remove them
    for i in [0, -1]:
        intervention = interventions['text'][i]
        if intervention[0].strip() == text_mesa.strip():
            interventions['text'].pop(i)

    full_text = ''
    full_speaker_text = ''
    # clean text for each speaker
    for intervention in interventions['text']:
        full_text += intervention[1]
        if intervention[0] == intervinent:
            full_speaker_text += intervention[1]

    if not is_catalan(full_speaker_text):
        return False

    if len(full_speaker_text.split()) < 130:
        msg = "speech too short"
        logging.warning(msg)
        return False

    speaker_word_fraction = len(full_speaker_text.split())/len(full_text.split())
    if speaker_word_fraction < 0.7:
        msg = '%s only speaks only a %1.2f fraction of the words'\
               %(intervinent, speaker_word_fraction)
        logging.warning(msg)
        return False

    for i, intervention in enumerate(interventions['text']):
        cleaned_text = clean_text(intervention[1])
        interventions['text'][i] = (intervention[0], cleaned_text)
    return True

def clean_text(text):
    clean_text = hyphenfix(punctuation_normalize(structure_clean(text)))
    return clean_text

def is_catalan(text, threshold=0.7):
    tokens = re.sub(token_clean, '', text).lower().split()
    if tokens:
        tokens_in_language = 0
        no_tokens = len(set(tokens))
        tokens_in_language = len(set(tokens).intersection(lexicon_set))
        score = float(tokens_in_language)/float(no_tokens)
        if score < threshold:
            msg = '%2.2f not catalan'%(1.0-score)
            logging.warning(msg)
            return False
    return True 

def download_media(candidates, threads=1, base_path='sessions'):
    urls = prepare_for_download(candidates)
    start = datetime.now()
    if threads == 1:
        for url in urls:
            download_files(base_path, url)
    else:
        with Pool(threads) as pool:
            with tqdm(total=len(urls)) as pbar:
                for i, _ in tqdm(enumerate(pool.imap(download_files_star,
                                   zip(itertools.repeat(base_path), urls)))):
                    pbar.update()
    end = datetime.now()
    print("It took: %s"%(end-start))

def prepare_for_download(candidates):
    urls = []
    for session in candidates.values():
        for key, value in session.items():
            urls.append((key, value['text'], value['urls'][0][1]))
    return urls

def crop_media(candidates, base_path):
    for session in candidates.values():
        for text_path, contents in session.items():
            # the url element should always have a single file
            paths = create_local_paths(base_path, (text_path, '',
                                                   contents['urls'][0][1]))
            if os.path.isfile(paths['audio_path']):
                # only process single speaker interventions
                if len(contents['text']) == 1:
                    text = contents['text'][0][1]
                    full_text = ' '.join(tokenize(text)).lower()
                    clean_text = re.sub(token_clean, '', full_text)
                    audio_file = Audio(paths['audio_path'])
                    trimmer = Trimmer(clean_text, audio_file)
                    try:
                        start, end, new_text = trimmer.crop_longaudio()
                    except Exception as e:
                        print(e)
                        print((clean_text[:100], clean_text[-100:]))
                        raise ValueError()
                    print(text_path)
                    print(start, end, new_text[:100], new_text[-100:])

if __name__ == "__main__":
    option = sys.argv[1]

    log_file = 'download_crop.log'
    current_path = os.getcwd()
    logging.basicConfig(filename=os.path.join(current_path,log_file),
                        format="%(asctime)s-%(levelname)s: %(message)s",
                        level=logging.INFO,
                        filemode='a')
    main(option)
