import sys
import os
from scraperwiki import pdftoxml

inDir = 'pdfs'
outDir = 'xmls'

for f in os.listdir(inDir):
    fout_path = os.path.join(outDir,f.replace('.pdf','.xml'))
    if not os.path.exists(fout_path):
        with open(os.path.join(inDir,f),'rb') as fin:
            try:
                print(f)
                xml_out = pdftoxml(fin.read())
            except Exception as e:
                print(e)
                print('%s failed'%f)
                xml_out = ''
            if xml_out:
                with open(fout_path,'w') as fout:
                    fout.write(xml_out)
