from utils.backend import PleDB

import yaml
import os

ple_code = '2018_03_01_243878'
yaml_path = 'yamls'

def main():
    meta = get_session_metadata(ple_code)
    texts = get_session_text(ple_code)
    compare_intervinents(meta, texts)

def get_session_metadata(ple_code):
    db = PleDB(task_name='test')
    db.connect()
    results = []
    for r in db.get_from_code(ple_code):
        results.append(r)
    return results

def get_session_text(ple_code):
    speakers = yaml.load(open(os.path.join(yaml_path, ple_code+'_speaker.yaml'),'r'))
    return speakers

def compare_intervinents(meta, text):
    meta_inter = []
    meta_speaker = []
    for m in meta:
        for intervention in m['interventions']:
            sp = intervention['intervinent'].split('|')[0].strip().replace('\xa0','')
            if 'President del Parlament' in sp:
                sp = 'El president'
            meta_inter.append(sp)
    for dic in text:
        speaker = list(dic.keys())[0]
        text = dic[speaker]
        if text:
            meta_speaker.append(speaker)

    #print(meta_inter, meta_speaker)
    '''
    print(len(meta_inter), len(meta_speaker))
    with open('pdf.ls','w') as out:
        for m in meta_speaker:
            out.write('%s\n'%m)
    with open('db.ls','w') as out:
        for i, m in enumerate(meta_inter):
            out.write('%s\n'%m)
    '''
    pause = 'El president'
    pdf_block = get_blocks(meta_speaker, 'El president')
    db_block = get_blocks(meta_inter, 'El president')
    with open('pdf_block.ls','w') as out:
        for m in pdf_block:
            out.write('%s\n'%m[2])
    with open('db_block.ls','w') as out:
        for i, m in enumerate(db_block):
            out.write('%s\n'%m[2])

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
                    if p == u:
                        end = i
                        #print('end',end)
                        break
        search_beginning = True
        blocks.append((start,end,u))
    return blocks

if __name__ == "__main__":
    main()
