import os
import re
import sys
import yaml
import logging

from utils.audio import Audio
from utils.trimmer import Trimmer

def main(audio_filepath, text_filepath):
    text = get_text(text_filepath)
    token_clean = '\.|,|;|:|\?|!|\.\.\.'
    clean_text = re.sub(token_clean,' ',text).lower()
    clean_text = re.sub(' {2,}', ' ', clean_text)
    audio_file = Audio(audio_filepath)
    trimmer = Trimmer(clean_text, audio_file)
    start, end, new_text = trimmer.crop_longaudio()
    print(start, end, new_text[:100], new_text[-100:])

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
