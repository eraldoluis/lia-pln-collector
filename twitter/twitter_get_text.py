import sys
from codecs import open
from sys import stdout

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def getTextFromES(tweets):
    es = Elasticsearch("localhost:9200")
    query = {
        "query": {
            "function_score": {
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
                },
                "random_score": {},
                "boost_mode": "replace"
            }
        }
    }

    count = 0
    for doc in scan(es, index="ctrls", doc_type="twitter", query=query):
        try:
            id = doc["_id"]
            text = doc["_source"]["tweet"]["text"]
            if count >= tweets and tweets != -1:
                break
            if text is not None:
                count += 1
                yield (id, text)
            else:
                stdout.write("x")
                stdout.flush()
        except:
            stdout.write("X")
            stdout.flush()


def main(outFileName, twts):
    outFile = open(outFileName, "wt", encoding="utf8")

    numTweets = 0
    for (id, tweet) in getTextFromES(twts):
        tweet = " ".join(tweet.split())
        outFile.write(id + "\t" + tweet + "\n")
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
    if len(sys.argv) != 3 and len(sys.argv) != 2:
        print "Missing argument: <output file> 'number of tweets'"
        exit(1)
    elif len(sys.argv) == 3:
        tweets = int(sys.argv[2])
    else:
        tweets = -1
    main(sys.argv[1], tweets)
