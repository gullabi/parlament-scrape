import os
import sys
import time
import yaml
import itertools
import logging

from subprocess import call, check_output, DEVNULL
from retrying import retry

def main(input_path, threads):
    base_path = os.path.abspath(input_path)
    if not os.path.isdir(base_path):
        raise IOError("%s does not exist or is not a directory"%base_path)

    audios = get_audios()
    start = datetime.now()
    if threads == 1:
        for audio in audios:
            download_files(base_path, audio)
    else:
        with Pool(threads) as pool:
            with tqdm(total=len(audios)) as pbar:
                for i, _ in tqdm(enumerate(pool.imap(download_files_star,
                                   zip(itertools.repeat(base_path), audios)))):
                    pbar.update()

    end = datetime.now()
    print("It took: %s"%(end-start))

def get_audios():
    audios = []
    person = yaml.load(open(yaml_in))
    for session, talks in person.items():
        for talk_id, talk in talks.items():
            audios.append([talk_id, talk['text'], talk['audio']])
    return audios

def download_files_star(base_path_audio):
    return download_files(*base_path_audio)

def download_files(base_path, audio):
    '''creates the paths and initiates the download of files
    '''
    text_path, text, link = audio
    paths = create_local_paths(base_path, audio)
    audio_path = os.path.dirname(paths['audio_path'])
    if not os.path.isdir(audio_path):
        try:
            os.makedirs(audio_path)
        except FileExistsError:
            logging.warning("conflicting mkdir on %s. Safely skipping."%audio_path)
    check_download_convert(link,paths['audio_path'])

def create_local_paths(base_path, audio):
    '''takes an object with id, text and url; outputs a paths dictionary
    '''
    text_path, text, link = audio
    paths = {}
    base_name = os.path.basename(link)
    paths['audio_path'] = os.path.join(base_path,
                                       base_name[0],
                                       base_name[1],
                                       base_name)
    paths['txt_path'] = text_path.replace('text', 'clean_text')
    return paths

def check_download_convert(uri, filepath):
    '''downloads a mp3, converts it to mp3 and then deletes the original.
    '''
    if not filepath.endswith('.mp3'):
        raise ValueError('Expected a mp3 audio file but found %s'%filepath)
    if not os.path.isfile(filepath):
        # download
        check_download(uri, filepath)
    else:
        msg = 'skipping %s for %s'%(filepath, uri)
        logging.info(msg)

def check_download(uri, filepath):
    if not os.path.exists(filepath):
        curl_download(uri, filepath)
    else:
        logging.info("%s exists, skipping."%filepath)

@retry(stop_max_attempt_number=3, wait_fixed=1000)
def curl_download(uri, filepath):
    msg = 'checking %s'%uri
    logging.info(msg)
    # check the http headers
    status, uri = get_status_code(uri)
    if status == 302:
        # redirect uri should have been extracted to the uri variable
        status, uri = get_status_code(uri)
    if status != 200:
        error = 'the resource in the url %s cannot be reached'\
                              'with status %i.'%(uri,status)
        logging.error(error)
        if status == 401:
            return None
        else:
            raise ConnectionError(error)

    # create file
    with open(filepath,'w') as fout:
        cmd = ['curl','-g',uri]
        logging.info("downloading %s"%uri)
        call(cmd, stdout=fout, stderr=DEVNULL) #seems dangerous but 404s are
                                               #caught by the get_status_code
def get_status_code(url):
    cmd = ['curl','-I',url]
    header = check_output(cmd, stderr=DEVNULL)
    header_list = header.split(b'\n')
    code = int(header_list[0].split()[1])
    uri = url
    if code == 302:
        for h in header_list:
            if h.startswith(b'Location: '):
                uri = h.strip().decode('ascii')[10:]
                if 'http' not in uri:
                    code = 401
    return code, uri

def simple_convert(source, target):
    '''makes a subprocess call to ffmpeg -i <source> <target>
    '''
    if not os.path.exists(source):
        msg = "%s does not exists (for conversion)"%source
        logging.error(msg)
        raise IOError(msg)
    cmd = "ffmpeg -i %s %s -hide_banner -loglevel panic"%(source, target)
    logging.info(cmd)
    call(cmd.split(), stdout=DEVNULL)

if __name__ == "__main__":
    input_path = sys.argv[1]
    if len(sys.argv) > 2:
        threads = int(sys.argv[2])
        if threads > 4:
            raise ValueError("cannot have threads larger than 4")
    else:
        threads = 1

    log_file = 'parlament_download.log'
    current_path = os.getcwd()
    logging.basicConfig(filename=os.path.join(current_path,log_file),
                        format="%(asctime)s-%(levelname)s: %(message)s",
                        level=logging.INFO,
                        filemode='a')

    main(input_path, threads)
