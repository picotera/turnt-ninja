import json

hotwords_file = 'hotwords.ini'
weights_file = 'weights.json'

hotwords = open(hotwords_file).read().split(',')

score = 1
d = {}
for word in hotwords:
    d[word] = score
    
open(weights_file, 'w').write(json.dumps(d, indent=4))