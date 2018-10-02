import sys
import os

from xml2yaml import parseXML

def main(filename):
    if filename.endswith('.xml'):
        print(filename)
        out_file = os.path.join(filename).replace('.xml','_speaker.yaml')
        if not os.path.exists(out_file):
            p = parseXML(filename)
            p.parse_xml()
            p.output_lines(out_path='./')
        else:
            print('INFO: file exists. skipping...')

if __name__ == "__main__":
    filename = sys.argv[1]
    main(filename)
