from multiprocessing import Pool, Queue, Lock
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup, Comment
import string
import math



# Main Functions (aka functions called in __main__)

# Creates final_index folder, and subdirectories for each integer and letter within that
def createFinalIndex() -> None:
    indexName = "final_index"
    if not Path(indexName).exists():
        Path(indexName).mkdir()
    for char in list(string.ascii_lowercase):
        pathFull = Path(indexName) / char
        if not pathFull.exists():
            pathFull.mkdir()
    for num in list("0123456789"):
        pathFull = Path(indexName) / num
        if not pathFull.exists():
            pathFull.mkdir()


# Uses multithreading, collects data from every token.json in the "old" index
def rewriteTokenFiles(indexPath: str) -> None:
    filePathsList = getAllFilePaths(indexPath)  # 1.3M+ small json files to process
    
    # For testing only
    for filepath in filePathsList:
        calculateTFIDF(filepath)


### Helper Functions (aka functions called by other functions) ###

# Gets all subdirectories (0-9, a-z) in index, then gets all token.json filepaths into a list
def getAllFilePaths(indexPath: str) -> list:
    # create list of all subdirectories that we need to process
    filePathsList = list()
    pathParent = Path(indexPath)
    subdirList = [(pathParent / p) for p in os.listdir(indexPath) if
                      Path(Path(indexPath).joinpath(p)).is_dir()]

    # Gets all the token.json filepaths and adds them to a list to be processed in rewriteTokenFiles()
    for subdir in subdirList:
        for tokenFile in Path(subdir).iterdir():
            if tokenFile.is_file():
                tokenFilePath = subdir / tokenFile.name
                filePathsList.append(str(tokenFilePath))

    return filePathsList


# Calculate TF-IDF scores for each document within each token.json file
# Rewrites those scores into each token.json file, with new format {"docDict": {docID : TFIDF_Score}}.
def calculateTFIDF(tokenFilePath : str) -> None:
    finalIndexPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\final_index"
    N = 13518180 #Total number of tokens / token.json files

    # Read token.json file, store its data as a variable
    tokenFile = open(tokenFilePath, 'r')
    tokenFileText = tokenFile.read()
    tokenJSON = json.loads(tokenFileText)
    docList = tokenJSON["docList"]
    tokenFile.close()
        
    # Use token.json data to calculate TF-IDF score for each document
    # Example of each docList per token: [[24494, 'a'], [26066, 'p'], [47854, 'div']]
    # Save at the end as {docID : TFIDF_Score}
    newData = dict()
    newData["docList"] = dict()
    for doc in docList:
        # Get name of the token.json file to rewrite it in finalIndex
        basename = os.path.basename(tokenFilePath)
        filename = os.path.splitext(basename)[0]

        # Calculate TF-IDF score for current document
        # doc[0] = DocID, doc[1] = RawDocTF, doc[2] = Tag
        boostPercent = makeBoostPercent(doc[2])
        newData["docList"][int(doc[0])] = (1 + math.log(doc[1])) * math.log(N / len(docList)) * boostPercent

        with open(os.path.join(finalIndexPath, f"{filename[0]}", f"{filename}.json"), 'w+') as newFile:
            json.dump(newData, newFile)


# Returns float, which is the % to boost a TF-IDF score based on its tag 
def makeBoostPercent(tag : str) -> float:
    # ['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'a', 'p', 'span', 'div']
    if (tag == "title"):
        return 2.0
    elif (tag == "h1"):
        return 1.9
    elif (tag == "h2"):
        return 1.8
    elif (tag == "h3"):
        return 1.7
    elif (tag == "h4"):
        return 1.6
    elif (tag == "h5"):
        return 1.5
    elif (tag == "h6"):
        return 1.4
    elif (tag == "strong"):
        return 1.3
    elif (tag == "b"):
        return 1.2
    elif (tag == "a"):
        return 1.1
    elif (tag in ["p", "div", "span"]):
        return 1.0



if __name__ == '__main__':
    # Aljon
    index = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\index"

    # William
    #folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\DEV"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art - windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # Art - linux
    #folderPath = "/home/anon/Downloads/DEV"


    # Second Pass: TF-IDF Scores using previous index with tags
    print("Creating final index folders...")
    createFinalIndex()

    print("Parsing token.json files, calculating TF-IDF scores...")
    rewriteTokenFiles(index)

    print("-----DONE!-----")
