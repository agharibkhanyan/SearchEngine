from flask import Flask,render_template,flash,request,url_for,redirect
import search as util
from pathlib import Path
import json
from time import time as timer, perf_counter
import GLOBALS

app = Flask(__name__)

# home page, with search bar at this url http://localhost:5000/
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/query", methods=["POST"])
def results():
    queryUser=request.form.get("query") # get keywords typed by user

    ### warm the cache with urls ###
    indexPath = GLOBALS.REG_INDEX

    #read hashurls.txt into memory, to read out urls from docID's
    hashTablePath = Path(indexPath) / "hashurls.txt"
    with open(hashTablePath, "r") as file:
        data = file.read()
        cacheURLs = json.loads(data)

    t1_start = perf_counter()
    start = timer()  # timer 2

    urlss = util.flaskBackendQuery(queryUser, cacheURLs)

    t1_stop = perf_counter()
    print("--- %.8f seconds ---" % (t1_stop - t1_start))
    print(f"Elapsed Time: {timer() - start}")  # timer 2

    listUrlOnly = list()
    for doc in urlss:
        listUrlOnly.append(doc[0])  # tuple stucture (url, score), just want to display the url

    # urlss=["https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/"]

    return render_template('results.html', urls=listUrlOnly, query=queryUser) # query=queryUser is used to show what user typed on page results.html


if __name__ == "__main__":
    app.run(debug=True)
