import yaml
import sys
import re
from unicodedata import normalize

def main(filepath):
    compare = yaml.load(open(filepath))
    text_speakers = post_process([dd[1][2] for dd in compare if dd[1]])
    meta_speakers = [dd[0][2] for dd in compare if dd[0]]
    print(len(set(text_speakers)),'vs',len(set(meta_speakers)))

def post_process(speakers):
    name_dict = {}
    for speaker in set(speakers):
        match = re.search('(.+)\((.+)\)',speaker)
        if match:
            title, name = match.groups()
            name_dict[remove_accents(speaker.strip().lower())] = name.strip()
            name_dict[remove_accents(title.strip().lower())] = name.strip()

    new = []
    for speaker in speakers:
        normalized_speaker = remove_accents(speaker.strip().lower())
        if name_dict.get(normalized_speaker):
            new.append(name_dict[normalized_speaker])
        else:
            new.append(speaker)
    return new

def remove_accents(input_str):
    nfkd_form = normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii

if __name__ == "__main__":
    main(sys.argv[1])
