import subprocess
import os
import re
import logging

from math import floor

PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))
TMP_PATH = os.path.join(PROJECT_PATH,'tmp')

class Audio(object):
    def __init__(self,filepath):
        if not filepath or not os.path.isfile(filepath):
            raise IOError('%s file is not given or does not exist.'%str(filepath))
        self.filepath = filepath
        self.get_duration() 

    def __srt__(self):
        return self.filepath

    def get_duration(self):
        args = ['ffprobe','-v','error','-show_entries',
                'format=duration','-of','default=noprint_wrappers=1:nokey=1']
        popen = subprocess.Popen(args+[self.filepath],stdout=subprocess.PIPE)
        popen.wait()
        self.duration = float(popen.stdout.read().strip())

    def segment(self, start=None, end=None, outpath=TMP_PATH, wav=True):
        try:
            duration = end - start
        except:
            raise ValueError('start or end for segmentation is not given.')
        if outpath == TMP_PATH and not os.path.isdir(TMP_PATH):
            os.mkdir(TMP_PATH)
        audio_tool = 'ffmpeg'
        seek = floor(start)
        seek_start = start - seek
        filename = os.path.basename(self.filepath)
        basename, extension = filename.split('.')
        if wav:
            extension = 'wav'
        segment_filename = '_'.join([basename,"%.3f_%.3f"%(start,end)])
        segment_path = os.path.join(outpath,segment_filename)+'.%s'%extension
        args = [audio_tool, '-hide_banner', '-loglevel', 'panic',
                '-ss', str(seek), '-i', self.filepath, '-ss', \
                str(seek_start), '-t', str(duration), '-ac', '1', '-ar', '16000', \
                segment_path]
        if os.path.isfile(segment_path):
            logging.info("%s already exists skipping"%segment_filename)
            return segment_path
        else:
            logging.info('creating %s'%segment_path)
            logging.debug(' '.join(args))
            subprocess.call(args)
            if not os.path.isfile(segment_path):
                raise IOError("File not created from %s operation"
                              " %s"%(audio_tool,segment_path))
            return segment_path

