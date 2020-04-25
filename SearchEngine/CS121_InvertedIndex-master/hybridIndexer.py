#from multiprocessing import Pool, Queue, Lock
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string
import nltk

#nltk.download('punkt')
#nltk.download('wordnet')
#nltk.download('averaged_perceptron_tagger')
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import math
import utils as util
import hashlib

from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk import pos_tag
from collections import defaultdict

tag_map = defaultdict(lambda: wn.NOUN)
tag_map['J'] = wn.ADJ
tag_map['V'] = wn.VERB
tag_map['R'] = wn.ADV

# Data Structures

# 'Posting' objects store the info we later put into each token.json file in our index
# Initially, each token and Posting we find in each document goes into index.txt
# Postings contain docID (where token was found), RAW frequency (in the document of docID), and tag (from HTML content)
class Posting:
    def __init__(self, docID, frequency, tag):
        self.docID = int(docID)
        self.frequency = int(frequency)
        self.tag = str(tag)

    def incFreq(self):
        self.frequency += 1

    def showData(self):
        return [self.docID, self.frequency, self.tag]


# Main Functions (aka functions called in __main__)

# Creates subdirectories for each integer and letter.
def createPartialIndexes() -> None:
    if not Path("index").exists():
        Path("index").mkdir()
    for char in list(string.ascii_lowercase):
        pathFull = Path("index") / char
        if not pathFull.exists():
            pathFull.mkdir()
    for num in list("0123456789"):
        pathFull = Path("index") / num
        if not pathFull.exists():
            pathFull.mkdir()


# Uses multithreading, tokenizes every document in the "DEV" corpus
def parseJSONFiles(directoryPath: str) -> None:
    filePathsList = getAllFilePaths(directoryPath)  # 55K+ json files to process

    # Make the Pool of workers
    pool = ThreadPool(processes=20)
    # Each worker get a directory from list above, and begin tokenizing all json files inside
    pool.map(tokenize, filePathsList)
    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()

# creates hashurlt.txt that is used for search to diplay url results
def urlHashTableBuilder(directoryPath) -> None:
    uniqueURLDict = dict()  # holds docID : url
    dupeURLDict = dict()
    hashSets = set()

    filePathsList = getAllFilePaths(directoryPath)  # 55K+ json files to process

    for fileObj in filePathsList:
        filePath = fileObj[1]
        docID = int(fileObj[0])

        with open(filePath, 'r') as content_file:
            textContent = content_file.read()
            jsonOBJ = json.loads(textContent)
            htmlContent = jsonOBJ["content"]
            urlContent = jsonOBJ["url"]

            # initialize BeautifulSoup object and pass in html content
            soup = BeautifulSoup(htmlContent, 'html.parser')

            # return if html text has identical hash
            # add all tokens found from html response with tags removed
            varTemp = soup.get_text()
            hashOut = hashlib.md5(varTemp.encode('utf-8')).hexdigest()
            if hashOut in hashSets:
                dupeURLDict[docID] = urlContent
                continue

            # add unique url to redis
            uniqueURLDict[docID] = urlContent

            # add hash to set
            hashSets.add(hashOut)

    with open(os.path.join("C:\\Anaconda3\\envs\\Projects\\developer", "hashurls.txt"), "w") as hash:
        hash.write(json.dumps(uniqueURLDict))
    with open(os.path.join("C:\\Anaconda3\\envs\\Projects\\developer", "sameurls.txt"), "w") as hash:
        hash.write(json.dumps(dupeURLDict))


# Reads index.txt line by line, then sums the frequencies of each token.
# Next, it collects a list of DocIDs into a list for each token.
# Finally, it adds the tag the token had originally
def mergeTokens():
    indexTxt = open(os.path.join("index", "index.txt"), 'r')

    for line in indexTxt:
        # Convert raw text in this line into Posting data
        posting = line.split(" : ")
        token = str(posting[0].replace("'", ""))
        postingList = posting[1].strip("][\n").split(", ")

        # Change types of items in postingList appropriately, store into variables
        newDocID = int(postingList[0])
        newFreq = int(postingList[1])
        newTag = str(postingList[2].strip("'"))

        # Create a new filename and filepath for this token
        firstLetter = token[0:1]
        filePathFull = Path("index") / firstLetter / (token + ".json")

        # If file already exists, then we read it and update it
        if filePathFull.is_file():
            with open(filePathFull, "r+") as tokenJSON:
                # Add to the existing data and save updated values back to json
                data = tokenJSON.read()
                jsonObj = json.loads(data)
                jsonObj["docList"].append([newDocID, newFreq, newTag])
                jsonObj["docList"] = sorted(jsonObj["docList"])
                tokenJSON.seek(0)  # reset to beginning of file to overwrite
                tokenJSON.write(json.dumps(jsonObj))

        else:
            # Otherwise, write it from scratch
            jsonObj = {"docList": [[newDocID, newFreq, newTag]]}
            with open(filePathFull, "w+") as posting:
                posting.write(json.dumps(jsonObj))



### Helper Functions (aka functions called by other functions) ###

# Gets all filepaths
def getAllFilePaths(directoryPath: str) -> list:
    listFilePaths = list()
    hashTableIDToUrl = dict()

    # create list of all subdirectories that we need to process
    pathParent = Path(directoryPath)
    directory_list = [(pathParent / dI) for dI in os.listdir(directoryPath) if
                      Path(Path(directoryPath).joinpath(dI)).is_dir()]

    iDocID = 0
    # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
    # also creates a hashtable that maps docID => filepath urls
    for directory in directory_list:
        for files in Path(directory).iterdir():
            if files.is_file():
                fullFilePath = directory / files.name
                listFilePaths.append([int(iDocID), str(fullFilePath)])
                hashTableIDToUrl[int(iDocID)] = str(fullFilePath)
                iDocID += 1

    # Writes "hashtable" file that maps the docIDs to filepaths of those documents.
    with open(os.path.join("index", "hashtable.txt"), "w+") as hash:
        hash.write(json.dumps(hashTableIDToUrl))

    return listFilePaths


# Tokenizes and collects data from one json file from the "DEV" corpus
# creates a dictionary that is cached, lemmatized words are saved in here
# saves up to 50,000 lemmatized words in memory
def tokenize(fileItem: list) -> None:
    ps = PorterStemmer().stem
    wnl = WordNetLemmatizer()
    lem = wnl.lemmatize
    lemmaCache = dict()

    tokenDict = dict()
    filePath = fileItem[1]
    docID = int(fileItem[0])

    with open(filePath, 'r') as content_file:
        textContent = content_file.read()
        jsonOBJ = json.loads(textContent)
        htmlContent = jsonOBJ["content"]

        # initialize BeautifulSoup object and pass in html content
        soup = BeautifulSoup(htmlContent, 'html.parser')

        # Deletes HTML comments, javascript, and css from text
        for tag in soup(text=lambda text: isinstance(text, Comment)):
            tag.extract()
        for element in soup.findAll(['script', 'style']):
            element.extract()

        # Collect all words found from html response WITH TAGS IN A TUPLE WITH EACH WORD ('word', 'tag')
        # Tags below are in order of importance/weight
        tagNamesList = ['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'a', 'p', 'span', 'div']
        tagsTextList = []
        for tag in tagNamesList:
            tagsTextList.append(soup.find_all(tag))

        ##### REDIS ONLY START #####
        # urlContent = jsonOBJ["url"]

        # # return if html text has identical hash
        # # Add all tokens found from html response with tags removed
        # varTemp = soup.get_text()
        # if util.isHashSame(varTemp):
        #    util.addDuplicateURL(docID, urlContent)
        #    return

        # # Add unique url to redis
        # util.addUniqueURL(docID, urlContent)
        ##### REDIS ONLY END #####

        taggedTextDict = dict()
        for i, tagSubList in enumerate(tagsTextList):
            taggedTextDict[tagNamesList[i]] = list()
            for phrase in tagSubList:
                for word in re.split(r"[^a-z0-9']+", phrase.get_text().lower()):
                    taggedTextDict.get(tagNamesList[i]).append(word)

        # Store words as tokens in tokenDict, ignore words that are bad
        for tag, wordList in taggedTextDict.items():
            for word in wordList:
                if (len(word) == 0):  # ignore empty strings
                    continue
                if (len(word) > 30 and tag != 'a'):  # ignore words like ivborw0kggoaaaansuheugaaabaaaaaqcamaaaaolq9taaaaw1bmveuaaaacagiahb0bhb0bhr0ahb4chh8dhx8eicifisiukt4djzankywplcwhltkfpl8nn0clpvm9qumvvxu8wnvbrezesepkyxvwzxbpbnjqb3jtcxruc3vvdxhzdnhyehtefjvdf5xtjkv
                    continue  # But accept any URLs that may be large
                if (word[0] == "'"):  # ignore words that start with '
                    continue
                if (len(word) == 1 and word.isalpha()):  # ignore single characters
                    continue

                # will not change numbers/digits
                # lemmatized things that are 3 letter or greater
                if not any(char.isdigit() for char in word) and len(word) > 2 and word not in lemmaCache:
                    # Lemmatization of a word with a number is usually itself.
                    # lemmatization of in, on, as, is usually itself.
                    # Checking for the above and if word is not already cached saves time.
                    # gets the part of speech or a word, to make lemmatization more accurate
                    pos = tag_map[pos_tag((word,))[0][1][0]]
                    lemWord = lem(word, pos) # lemmatized word

                    #catches words that lemmatization misses and porter stemmer in its place
                    if word[-2:] == "ly" or word[-4:] == "ness" or word[-3:] == "ish":  # Catches any ly, ness, or ish that lemmatize doesnt catch. Words are less accurate, but cuts off extraneous words.
                        lemWord = ps(word)
                    lemmaCache[word] = lemWord
                else:
                    lemmaCache[word] = word  # the lemma of the word is itself

                if lemmaCache[word] in tokenDict:
                    tokenDict.get(lemmaCache[word]).incFreq()
                else:
                    tokenDict[lemmaCache[word]] = Posting(docID, 1, tag)

                if len(lemmaCache) > 5000000:  # Save up to 5million tokens, and then clear to prevent too much memory error
                    lemmaCache.clear()

        # Write tokens and their Postings to a text file ("store on disk")
        buildIndex(tokenDict)


def buildIndex(tokenDict: dict) -> None:
    # Write index.txt to store indexing data for merging later
    with open(os.path.join("index", "index.txt"), "a") as partialIndex:
        for key in sorted(tokenDict):
            partialIndex.write(key + " : " + str(tokenDict.get(key).showData()) + '\n')



if __name__ == '__main__':
    # Aljon
    folderPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\DEV"

    # # William
    # folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\DEV"
    # urlHashTableBuilder(folderPath)

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art - windows
    # folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # Art - linux
    # folderPath = "/home/anon/Downloads/DEV"


    # First Pass: Term frequency and Getting HTML tags
    print("Creating partial index folders...")
    createPartialIndexes()

    print("Parsing 'DEV' JSON files, building index.txt...")
    parseJSONFiles(folderPath)

    print("Merging tokens from index.txt, storing token.JSON files into index...")
    mergeTokens()

    print("-----DONE!-----")
