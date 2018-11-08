import os
import re
import yaml
import logging

from copy import copy
from pocketsphinx import AudioFile, get_model_path, get_data_path

MODEL_PATH = '/home/baybars/scripts/repositories/cmusphinx-models'
CONFIG = {
    'verbose': False,
    'audio_file': '',
    'buffer_size': 2048,
    'no_search': False,
    'full_utt': False,
    'keyphrase': None,
    'hmm': os.path.join(MODEL_PATH, 'ca-es/acoustic-model'),
    'lm': False,
    'dict': os.path.join(MODEL_PATH, 'ca-es/pronounciation-dictionary.dict'),
    'frate': 100 # frames per second (default=100)
}
OUTPATH = './test'
JSGF = '''
#JSGF V1.0;

grammar words;
<word> = %s

public <words> = <word>+;
'''

def crop_longaudio(text, audio_file):
    text_snippets = get_text_start_end(text)
    audio_snippets, audio_offsets = get_audio_start_end(audio_file)
    match_results = []
    for i, (text_snippet, audio_snippet) in enumerate(zip(text_snippets,
                                                     audio_snippets)):
        if i == 0:
            option = 'beginning'
        elif i == 1:
            option = 'ending'
        else:
            msg = 'For the file %s audio and text snippets are more than two'\
                  %audio_file
            raise ValueError(msg)
        match_results.append(fsg_search(text_snippet,
                                        audio_snippet,
                                        audio_offsets[i],
                                        option=option))
    print(match_results)
    new_text = get_global_text(text, match_results)
    return (match_results[0][0], match_results[1][0], new_text)

def get_text_start_end(text):
    '''returns the first and last 6 words. 
       TODO clean the text?
    '''
    words = text.split()
    return [words[:6], words[-6:]]

def get_audio_start_end(audio_file):
    audio_filepath = audio_file.filepath
    out_files = []
    offsets = []
    outpath = OUTPATH
    if audio_file.duration > 100.:
        starts_ends = get_audio_limits(audio_file.duration)
        if starts_ends:
            for start, end in starts_ends:
                fileout = audio_file.segment(start=start,
                                             end=end,
                                             outpath=outpath)
                out_files.append(fileout)
                offsets.append(start)
    else:
        # TODO convert to wav
        fileout = audio_file.segment(start=0.0,
                                     end=audio_file.duration,
                                     outpath=outpath)
        out_files = [fileout, fileout]
        offsets = [0,0]
    return out_files, offsets

def get_audio_limits(duration):
    '''Gives the first one minute and the last one minute of the recording
    '''
    if duration < 100.:
        logging.warning("duration too short shouldn't be here")
        return []
    return [(0.0, 60.0),(duration-60.0, duration)]

def fsg_search(text_snippet, audio_snippet, offset_seconds,
                                                           option='beginning'):
    fsg_file = generate_fsg(text_snippet)
    CONFIG['jsgf'] = fsg_file
    CONFIG['audio_file'] = audio_snippet
    audio = AudioFile(**CONFIG)
    result_sequence = []
    for phrase in audio:
        #print(phrase.probability(), phrase.score(), phrase.confidence())
        for s in phrase.seg():
            start_time = s.start_frame / CONFIG['frate']
            end_time = s.end_frame / CONFIG['frate']
            if start_time != end_time:
                # getting rid if NULL elements
                # TODO get rid of silences that are not "too long"
                result_sequence.append((start_time,
                                        end_time,
                                        s.word))
    with open(fsg_file+'.yml','w') as out:
        yaml.dump(result_sequence, out)
    # delete fsg files
    # should return the best match text snippet with beginning end
    if option == 'beginning':
        search_snippet = copy(text_snippet)
        match_result = find_match(result_sequence, search_snippet)
        # assert that offset_seconds is zero
        if match_result:
            result_seconds = offset_seconds + match_result[0]
        else:
            result_seconds = offset_seconds
            search_snippet = text_snippet
    elif option == 'ending':
        search_snippet = copy(text_snippet)[::-1]
        match_result = find_match(result_sequence[::-1], search_snippet)
        if match_result:
            result_seconds = offset_seconds + match_result[1]
            search_snippet = search_snippet[::-1]
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

def generate_fsg(text_snippets):
    # use a disctinctive name for the temporary files
    filename = ''.join(text_snippets)[:7]+'.jsgf'
    fsg_path = os.path.join(OUTPATH, filename)
    fsg_query = ' | '.join(text_snippets)+';'
    with open(fsg_path,'w') as out:
        out.write(JSGF%fsg_query)
    return fsg_path

def find_match(full_sequence, search_sequence):
    '''Searches for an exact match with gradually shorter search sequences
    '''
    match = []
    while len(search_sequence) > 2 and not match:
        match = sequence_match(full_sequence, search_sequence)
        if not match:
            search_sequence.pop(0)
    if not match:
        logging.error('match not found ')
    return match

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

def get_global_text(text, match_results):
    start_text = ' '.join(match_results[0][1])
    end_text = ' '.join(match_results[1][1])
    start_index = text.find(start_text)
    end_index = text.find(end_text)+len(end_text)
    if start_index == -1 or end_index < len(end_text):
        msg = 'having difficulty finding the new start or/and end'
        raise ValueError(msg)
    return text[start_index:end_index]

