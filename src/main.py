
if __name__ == '__main__':
    pass

import storedData
from os.path import abspath, join , dirname
import json 

tree = storedData.StoredData(20,'test.json')
for i in range(20,30):
    tree.insert(i, ['Luis', 617277997, 'Hacienda Del Rey'])
tree.dump()
