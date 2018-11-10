import os
import re
import sys
import yaml
import logging

from utils.audio import Audio
from utils.trimmer import Trimmer
from utils.clean import tokenize

def main(audio_filepath, text_filepath):
    text = get_text(text_filepath)
    token_clean = '\.|,|;|:|\?|!|\.\.\.'
    tokenized_text = ' '.join(tokenize(text))
    clean_text = re.sub(token_clean,'',tokenized_text).lower()
    audio_file = Audio(audio_filepath)
    trimmer = Trimmer(clean_text, audio_file)
    start, end, start_word_index, end_word_index = trimmer.crop_longaudio()
    print(start, end, tokenized_text.split()[start_word_index],
                      tokenized_text.split()[end_word_index-1])

def get_text(text_file):
    text_dict = yaml.load(open(text_file))
    text = ''
    for element in text_dict['text']:
        text += element[1]
    return text

if __name__ == "__main__":
    audio_filepath = sys.argv[1]
    text_filepath = sys.argv[2]

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s-%(levelname)s: %(message)s",
                        handlers=[logging.StreamHandler()])

    main(audio_filepath, text_filepath)
