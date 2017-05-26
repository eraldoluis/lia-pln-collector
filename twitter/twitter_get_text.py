import sys
from codecs import open
from sys import stdout

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def getTextFromES():
    es = Elasticsearch("localhost:9200")
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "start": "2017-05-24T21:16:27.396400-04:00"
                        }
                    }
                ]
            }
        }
    }

    for doc in scan(es, index="ctrls", doc_type="twitter", query=query):
        try:
            text = doc["_source"]["tweet"]["text"]
            if text is not None:
                yield text
            else:
                stdout.write("x")
        except:
            stdout.write("X")


def main(outFileName):
    outFile = open(outFileName, "wt", encoding="utf8")

    numTweets = 0
    for tweet in getTextFromES():
        tweet = " ".join(tweet.split())
        outFile.write(tweet + "\n")
        numTweets += 1
        if numTweets % 10000 == 0:
            if numTweets % 100000 == 0:
                stdout.write("%d" % ((numTweets / 100000) % 10))
                stdout.flush()
            else:
                stdout.write(".")
                stdout.flush()

    outFile.close()

    stdout.write(" done!\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Missing argument: <output file>"
        exit(1)
    main(sys.argv[1])
