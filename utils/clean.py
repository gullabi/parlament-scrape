import sys
import re
import pickle
import logging

lexicon = 'utils/lexicon_set_ca2.bin'
with open(lexicon, 'rb') as lexicon_file:
    lexicon_set = pickle.load(lexicon_file)

token_sign = "(\.\.\.|!\"|!'|!|\?\"|\?'|\?|\.'|\.\"|\.)(?=( {1,}[A-ZÀÉÈÜÚÍÏÓÒÇ]|[A-ZÀÉÈÜÚÍÏÓÒÇ]))"
re_token_limits = re.compile(token_sign)

non_sp_captions = re.compile('\<i\>\(.*?\) ?\</i\>')
formatting = re.compile('\<.{1,3}\>')
in_cbrackets = re.compile('{.+}')
in_paranthesis = re.compile('\(.*?\)')
w_spaces = re.compile(' {2,}')
ws_dot = re.compile(' (?=(\.))')
apostrophes = re.compile('`|’')
ex_apostrophes = re.compile("'{1,}")
quotes = re.compile('“|”|«|»|"')

valid = '[a-zA-Z0-9àáéèüúíïóòçÀÁÉÈÜÚÍÏÓÒÇ .!\-,;:\'"?·$°£€%]+'
valid_word = '[a-zA-Z0-9àáéèüúíïóòçÀÁÉÈÜÚÍÏÓÒÇ\-\']+'

def main(filename):
    with open(filename,'r') as corpus,\
         open('clean_sentences.txt','w') as wout:
        for line in corpus.readlines():
            clean_paragraph = structure_clean(line.strip())
            for token in tokenize(clean_paragraph):
                if token:
                    token = hyphenfix(punctuation_normalize(token))
                    wout.write('%s\n'%token)


def structure_clean(text):
    text = text.replace('\xa0',' ')
    text = text.replace('\xad','-')
    text = non_sp_captions.sub('',text)
    text = in_paranthesis.sub('',text)
    text = in_cbrackets.sub('',text)
    text = w_spaces.sub(' ',text)
    text = ws_dot.sub('',text)
    text = formatting.sub('',text)
    return text

def tokenize(text):
    token_limits = []
    for found in re_token_limits.finditer(text):
        token_limits.append(found.end())
    token_limits.append(-1)

    sentences = []
    last_limit = 0
    for tl in token_limits:
        if tl!=-1:
            sentence = text[last_limit:tl].strip()
        else:
            sentence = text[last_limit:].strip()
        if sentence:
            sentences.append(sentence)
        last_limit = tl
    return sentences

def punctuation_normalize(text):
    text = apostrophes.sub("'",text)
    text = ex_apostrophes.sub("'",text)
    text = quotes.sub('',text)
    return text

def hyphenfix(text):
    '''Fixes non-grammar hyphens
       The text needs to be clean before the fix
    '''
    replace_tasks = {}
    for word in text.strip().split():
        #clean_word = re.sub(token_clean, '', word)
        m = re.search('.+-.+', word)
        if m:
            clean_match = re.search(valid_word, word)
            if clean_match:
                # this way we save the punctuation and change only the word
                # when and if we need to replace
                clean_word = clean_match.group()
            else:
                logging.warning('%s is not a valid word?'%word)
            replaced = clean_word.replace('-','').lower()
            if clean_word.lower() in lexicon_set:
                continue
            elif replaced in lexicon_set:
                replace_tasks[clean_word] = replaced
            else:
                logging.info("unknown hyphen %s"%word)
    for key, value in replace_tasks.items():
        text = text.replace(key,replace_tasks[key])
    return text

def correct_orthography(text):
    '''In order to omit sentences based on wiki specific format issues
    '''
    if '|' in text or '[' in text:
        # If wiki code left in corpus
        return False
    if re.search('\d:\d',text):
        # If time or duration
        return False
    if re.search('\s·',text):
        # If single line bullets
        return False
    if len(text) < 31 or len(text) > 600:
        # If tokenization failed
        return False
    match = re.match(valid,text)
    if match:
        if match.end() != len(text):
            # If sentence has non-valid characters
            return False
    else:
        logging.warning(text)
        return False
    return True 

if __name__ == "__main__":
    filename = sys.argv[1]
    main(filename)
