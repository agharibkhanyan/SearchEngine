import redis
import hashlib
import os
import json

## connect redis to server on a specific port
r = redis.Redis(host="localhost",port=6379,db=0, decode_responses=True)

HASH_SAME = "hashSame"
UNIQUE_URL = "uniqueURL"
DUPLICATE_URL = "duplicateURL"

# EC is page content/html content the same, samehash check
def isHashSame(varTemp) -> bool:
    hashOut = hashlib.md5(varTemp.encode('utf-8')).hexdigest()

    if r.sismember(HASH_SAME, hashOut):
        return True

    r.sadd(HASH_SAME, hashOut)  # add hash of text output to redis set
    return False

def addUniqueURL(docID, url) -> None:
    url = _removeFragment(url)
    r.hset(UNIQUE_URL, docID, url)

def addDuplicateURL(docID, url) -> None:
    r.hset(DUPLICATE_URL, docID, url)

def _removeFragment(str):
    str = str.split('#')[0]
    return str

#writes all urls taken from .json files to a single file
def _writeUrlsToDisk() -> None:
    content = r.hgetall(UNIQUE_URL)
    # for hash in r.hscan_iter('UNIQUE_URL'):
    #     print(hash)  # for all child keys
    with open(os.path.join("index", "hashurls.txt"), "w+") as hash:
        hash.write(json.dumps(content))

if __name__ == '__main__':
    _writeUrlsToDisk()