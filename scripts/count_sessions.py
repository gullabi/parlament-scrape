import os
import yaml

count = 0
total = 0
for d in os.listdir('sessions'):
    total += 1
    session_text = os.path.join('sessions',d,'text')
    for f in os.listdir(session_text):
        filepath = os.path.join(session_text,f)
        with open(filepath) as read:
            y = yaml.load(read)
            if len(y['urls']) > 1:
                print(filepath, len(y['urls']))
                count += 1
print('%i out of %i files have more than one media url'%(count, total))
