
if __name__ == '__main__':
    pass

import storedData
from os.path import abspath, join , dirname
import json 
path = abspath(join(dirname(__file__), 'test.json'))
file = open(path, 'w+')
o = []
print (json.dumps(o))
print (json.JSONEncoder().encode(o))
file.write(json.JSONEncoder().encode(o))
file.close()
tree = storedData.StoredData(20,'test.json')
for i in range(0, 100):
    tree.insert(i, i+1)
tree.dump()