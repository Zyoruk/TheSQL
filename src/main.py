
if __name__ == '__main__':
    pass

import storedData
tree = storedData.StoredData(20,'test.json')
for i in range(0,10):
    tree.insert(i, [10101])
print tree.getAll()
tree.updateSingle(0, 0, 0)
print tree.getAll()

tree.dump()