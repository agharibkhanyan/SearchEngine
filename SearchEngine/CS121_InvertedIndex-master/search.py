from multiprocessing import Pool
from pathlib import Path
import os
import re
import json
import string
import math
import GLOBALS

stopWords = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
             "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
             "can't",
             "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
             "during",
             "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
             "he", "he'd",
             "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
             "i", "i'd", "i'll",
             "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more",
             "most", "mustn't", "my",
             "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
             "ourselves", "out", "over",
             "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some",
             "such", "than", "that", "that's",
             "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd",
             "they'll", "they're", "they've",
             "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
             "we'll", "we're", "we've", "were", "weren't",
             "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
             "why", "why's", "with", "won't", "would", "wouldn't",
             "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"}



# Main Functions (aka functions called in __main__)

# Takes in query as str. Returns list of docs that match the OR query (inclusive)
def search(query, finalIndexPath):
    listOfDicts = list()
    queryList = set()   # We use set() to remove duplicate terms, and we won't have to open a file twice
    tempList = query.strip().lower().replace("'", "").split(" ")

    for word in tempList:
        if word not in stopWords:
            queryList.add(word)

    print("Cleaned query tokens:")
    print(queryList, "\n") # query tokens with stopwords removed and replacing apostrohe and lower()

    #convert set to list to enumerate
    queryList = list(queryList)

    for word in queryList:
        charPath = word[0] #Get 1st char of current word, use to find subdir

        # Get the file path of the final_indexed token.json file
        jsonFilePath = str(Path(finalIndexPath) / charPath / word) + ".json"

        try:
            with open(jsonFilePath, "r") as file:
                data = file.read()
                jsonObj = json.loads(data)
                docsDict = jsonObj["docList"]
                listOfDicts.append(docsDict)
        except:
            pass

    return intersectDicts(listOfDicts)


def getDocURLs(intersectedDocs, indexPath, cacheURLs):
    listUrls = list()  # holds unique file paths of .json files
    #
    # hashTablePath = Path(indexPath) / "hashurls.txt"
    # with open(hashTablePath, "r") as file:
    #     data = file.read()
    #     hashSet = json.loads(data)

    for docID in intersectedDocs:
        if(docID in cacheURLs):
            fileUrl = cacheURLs[docID]
            listUrls.append( (fileUrl, intersectedDocs[docID]) )

    return listUrls



# Helper Functions (aka functions called by other functions)

# Returns unique dict of file urls from hashurl.txt (or hasthtable.txt)
def intersectDicts(listOfDicts):
    if len(listOfDicts) == 1:
        return listOfDicts[0]

    intersection = {}
    for dictItem in listOfDicts:
        for doc in dictItem:
            if doc not in intersection:
                intersection[doc] = dictItem[doc] #
            else:
                intersection[doc] += dictItem[doc] #adding tfidf weights
    print("intersection = ", intersection)
    return intersection


def flaskBackendQuery(queryUser, cacheURLs):
    indexPath = GLOBALS.FINAL_INDEX

    if (queryUser.strip() == ""):
        print("Query needs to be at least one character")

    unsortedDocs = search(queryUser, indexPath) #list of dictionaries

    # Change filepaths to website URLs for displaying
    unsortedURLs = getDocURLs(unsortedDocs, indexPath, cacheURLs)

    # Sort docs by the TF-IDF score
    sortedURLs = sorted(unsortedURLs, key=lambda x: x[1], reverse=True) #highest scores shown first

    return sortedURLs[0:10] #return 10 results


if __name__ == '__main__':
    #####
    # Aljon
    # finalIndexPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\final_index"
    # indexPath = "C:\\Users\\aljon\\Documents\\CS_121\\Assignment_3\\CS121_InvertedIndex\\index"

    # William
    # folderPath = "C:\\1_Repos\\developer\\partial_indexes"
    # folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\partial_indexes"
    indexPath = "C:\\1_Repos\\developer"
    finalIndexPath = "C:\\1_Repos\\developer"

    # Jerome
    #folderPath = "C:\\Users\\arkse\\Desktop\\CS121_InvertedIndex\\DEV"

    # Art
    # windows
    #folderPath = "C:\\Users\\aghar\\Downloads\\DEV"
    # linux
    #folderPath = "/home/anon/Downloads/DEV"
    #####


    # Get query from user
    query = input("Enter a search query: ")
    if(query.strip() == ""):
        print("Query needs to be at least one character")
    # Fetch all results of query, intersect them to follow Bool-AND logic
    unsortedDocs = search(query, finalIndexPath)

    # Change filepaths to website URLs for displaying
    unsortedURLs = getDocURLs(unsortedDocs, indexPath)

    # Sort docs by the TF-IDF score
    sortedURLs = sorted(unsortedURLs, key=lambda x: x[1], reverse=True)
    
    # Print top 5 ranked file-urls for given query
    print(f"\n------------ Top 5 Docs for '{query}' ------------\n")
    for i, doc in enumerate(sortedURLs):
        if (i > 5):
            break
        print(doc[0], " = ", doc[1])

    print("\n------------ DONE! ------------\n")
