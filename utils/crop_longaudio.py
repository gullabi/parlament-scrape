# -*- coding: utf-8 -*-
import os
import yaml

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

def main(audio_file, text_file):
    text = get_text(text_file)
    start, end, new_text = crop_longaudio(text, audio_file, outpath)
    # TODO output text in case the last word is not detected
    print(start, end, new_text)

def crop_longaudio(text, audio_file, outpath):
    text_snippets = get_text_start_end(text)
    audio_snippets, audio_offsets = get_start_end(audio_file)
    match_results = []
    for text_snippet, audio_snippet in zip(text_snippets, audio_snippets):
        match_results.append(fsg_search(text_snippet,
                                        audio_snippet,
                                        offset_seconds))
    return get_global_limits(match_results)

def get_start_end(audio_file)
    duration = get_duration(audio_file)
    if duration > 150:
        starts_ends = get_audio_start_end(audio_file)
        if starts_ends:
            for start, end in starts_ends:
                fileout = ''
                crop_audio(start, end, audio_file, fileout)
                out_files.append(fileout)
                offsets.append(start)
        else:
            # TODO convert to wav
            out_files = [audio_file, audio_file]
            offsets = [0,0]
    return out_files, offsets

def fsg_search(text_snippet, audio_snippet, offset_seconds):
    fsg_file = generate_fsg(text_snippet)
    CONFIG['jsgf'] = fsg_file
    CONFIG['audio_file'] = audio_snippet
    audio = AudioFile(**config)
    result_sequence = []
    for phrase in audio:
        #print(phrase.probability(), phrase.score(), phrase.confidence())
        for s in phrase.seg():
            result_sequence.append(s.start_frame / CONFIG['frate'],
                                   s.end_frame / CONFIG['frate'],
                                   s.word)
    # delete fsg files
    # should return the best match text snippet with beginning end
    start, end, match_snippet = find_match(result_sequence, text_snippet)
    return start+offset_seconds, end+offset_seconds, match_snippet

def generate_fsg(text_snippets):
    # use a disctinctive name for the temporary files
    return fsg_path

def find_match(result_sequence, text_sequence)
    return start, end, match_snippet

def get_global_limits(match_results):
    return global_start, global_end, final_text

if __name__ == "__main__":
    audio_file = sys.argv[1]
    text_file = sys.argv[2]
    main(audio_file, text_file)
