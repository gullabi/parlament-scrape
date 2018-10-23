from utils.backend import PleDB
from utils.alignment import Alignment

import os
import sys
import re

def main(ple_code):
    yaml_path = 'yamls'
    task_name = 'v1'
    db = PleDB(task_name=task_name)
    db.connect()
    if ple_code == 'all':
        get_all(db)
    else:
        get_one(db, ple_code)

def get_all(db):
    re_ple = re.compile('\d{4}_\d{2}_\d{2}_\d+')
    if not os.path.isdir('yamls'):
        raise IOError('yamls directory is not where expected.')
    for f in os.listdir('yamls'):
        if f.endswith('_speaker.yaml'):
            found = re_ple.search(f)
            if not found:
                raise ValueError('ple code could not be extracted from'\
                                 ' filename %s'%f)
            ple_code = found.group()
            for session in db.get_from_code(ple_code):
                if 'sessió constitutiva' in session['name']\
                   or 'constitució' in session['name'].lower():
                    msg = 'INFO: skipping sessió constitutiva'
                    continue
            print(ple_code)
            get_one(db, ple_code) 

def get_one(db, ple_code):
    found = True
    if not db.backend.find_one(ple_code):
        date = convert_code(ple_code)
        msg = 'ple_code %s not found in db. falling back to date %s'\
              %(ple_code, date)
        print('WARNING: %s'%msg)
        if not db.backend.find_one({"date":date}):
            msg = '         date query also failed. skipping'
            print(msg)
            found = False
    if found:
        align = Alignment(ple_code=ple_code, db=db)
        align.block_align()
        align.output_media_vs_text()

def convert_code(ple_code):
    split = ple_code.split('_')[:-1]
    return '/'.join(split[::-1])

if __name__ == "__main__":
    ple_code = sys.argv[1]
    main(ple_code)
