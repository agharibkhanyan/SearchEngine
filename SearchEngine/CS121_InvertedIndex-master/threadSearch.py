from pathlib import Path
import os
import requests
from time import time as timer, perf_counter
import json
import memoryIndex
from nltk.stem import PorterStemmer
import sys
from importlib import reload



def fetch_url(entry):
    path, uri = entry
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
    return path

urls = [
    ("/tmp/1.html", "https://markhneedham.com/blog/2018/07/10/neo4j-grouping-datetimes/"),
    ("/tmp/2.html", "https://markhneedham.com/blog/2018/07/09/neo4j-text-cannot-be-parsed-to-duration/"),
    ("/tmp/3.html", "https://markhneedham.com/blog/2018/06/15/neo4j-querying-strava-graph-py2neo/"),
    ("/tmp/4.html", "https://markhneedham.com/blog/2018/06/12/neo4j-building-strava-graph/"),
    ("/tmp/5.html", "https://markhneedham.com/blog/2018/06/05/neo4j-apoc-loading-data-strava-paginated-json-api/"),
    ("/tmp/6.html", "https://markhneedham.com/blog/2018/06/03/neo4j-3.4-gotchas-working-with-durations/"),
    ("/tmp/7.html", "https://markhneedham.com/blog/2018/06/03/neo4j-3.4-formatting-instances-durations-dates/"),
    ("/tmp/8.html", "https://markhneedham.com/blog/2018/06/02/neo4j-3.4-comparing-durations/"),
    ("/tmp/9.html", "https://markhneedham.com/blog/2018/05/19/interpreting-word2vec-glove-embeddings-sklearn-neo4j-graph-algorithms/"),
    ("/tmp/10.html", "https://markhneedham.com/blog/2018/05/11/node2vec-tensorflow/")
]


# #single threaded
# start = timer()
# for entry in urls:
#     fetch_url(entry)
# print(f"Elapsed Time: {timer() - start}")
# # Elapsed Time: 1.436131477355957


# # multi-thread ThreadPool
# start = timer()
# from multiprocessing.pool import ThreadPool
# results = ThreadPool(10).imap_unordered(fetch_url, urls)
# for path in results:
#     print(path)
#
# print(f"Elapsed Time: {timer() - start}")
# # Elapsed Time: 0.321636438369751


# from multiprocessing import Pool
# start = timer()
# def runMultithreadDownload():
#     pool = Pool(processes=10)
#     # Each worker get a directory from list above, and begin tokenizing all json files inside
#     pool.map(fetch_url, urls)
#     # Close the pool and wait for the work to finish
#     pool.close()
#     pool.join()
#
#     print(f"Elapsed Time: {timer() - start}")
#     # Elapsed Time: 0.8964707851409912

# if __name__ == '__main__':
#     runMultithreadDownload()


# import queue
# from threading import Thread
#
# def foo(bar):
#     return 'foo ' + bar
#
# start = timer()
# my_queue = queue.Queue()
#
# threads_list = list()
#
# t = Thread(target=lambda q, arg1: q.put(foo(arg1)), args=(my_queue, 'world!'))
# t.start()
# threads_list.append(t)
#
# t2 = Thread(target=lambda q, arg1: q.put(foo(arg1)), args=(my_queue, 'test2'))
# t2.start()
# # Add more threads here
# threads_list.append(t2)
#
# # Join all the threads
# for t in threads_list:
#     t.join()
#
# # Check thread's return value
# while not my_queue.empty():
#     result = my_queue.get()
#     print(result)
#
# print(f"Elapsed Time: {timer() - start}")


import queue
from threading import Thread

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


# Returns unique set of file urls from hasthtable.txt
def reduceResult():
    listUrls = list()

    tempSet = None
    while not my_queue.empty():
        #we initialize our tempset if first iteration of loop to first set in list
        if tempSet == None:
            tempSet = my_queue.get()
            continue

        #print(tempSet)
        set2 = my_queue.get()
        tempSet = tempSet.intersection(set2)

    for docID in tempSet:
        fileUrl = part1Cache[docID]
        listUrls.append(fileUrl)

    return listUrls


def simpleBoolAnd(word):
    if word in stopWords:
        return
    #
    # #convert set to list to enumerate
    # queryList = list(queryList)

    charPath = word[0] #Get 1st char of current word, use to find subdir

    # get the full file path of the indexed .json file
    jsonFilePath = str(Path(folderPath) / charPath / word) + ".json"

    try:
        with open(jsonFilePath, "r") as file:
            data = file.read()
            jsonObj = json.loads(data)

            listDocs = jsonObj["listDocIDs"]
            tempSet = set(listDocs)

            my_queue.put(tempSet)
    except:
        pass

folderPath = "C:\\Anaconda3\\envs\\Projects\\developer\\partial_indexes"
part1Cache = None
my_queue = queue.Queue()



if __name__ == "__main__":
    while True:
        if not part1Cache:
            part1Cache = memoryIndex.readHashTable(folderPath)

        threads_list = list()

        # ps = PorterStemmer()
        query = input("Please enter a query: ")

        #start_time = time.time()
        t1_start = perf_counter()
        start = timer()  # timer 2

        tempList = query.strip().lower().replace("'", "").split(" ")

        for word in tempList:
            t = Thread(target=lambda q, arg1: simpleBoolAnd(arg1), args=(my_queue, word))
            t.start()
            threads_list.append(t)

        # Join all the threads
        for t in threads_list:
            t.join()

        # Check thread's return value
        print(reduceResult())

        #print("--- %.8f seconds ---" % (time.time() - start_time))
        t1_stop = perf_counter()
        print("--- %.8f seconds ---" % (t1_stop - t1_start))

        print(f"Elapsed Time: {timer() - start}") # timer 2

        print("Press enter to re-run the script, CTRL-C to exit")

        sys.stdin.readline()
        reload(memoryIndex)