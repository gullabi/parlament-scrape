from utils.backend import PleDB
from utils.alignment import Alignment

import os
import sys

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
    for session in db.backend.find():
        if session['ple_code']:
            if 'sessió constitutiva' in session['name']\
                or 'constitució' in session['name'].lower():
                msg = 'INFO: skipping sessió constitutiva'
                continue
            print(session['ple_code'])
            yaml_file = os.path.join('yamls',session['ple_code']+'_speaker.yaml')
            if os.path.isfile(yaml_file):
                align = Alignment(ple_code=session['ple_code'], db=db)
                align.block_align()
            else:
                msg = 'WARNING: %s does not exist, skipping'%yaml_file
                print(msg)

def get_one(db, ple_code):
    align = Alignment(ple_code=ple_code, db=db)
    align.block_align()
    align.output_media_vs_text()

if __name__ == "__main__":
    ple_code = sys.argv[1]
    main(ple_code)
