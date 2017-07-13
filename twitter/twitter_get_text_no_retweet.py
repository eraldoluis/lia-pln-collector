#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import traceback
from sys import stdout

import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from tweepy import OAuthHandler
from tweepy import TweepError
from tweepy.api import API
from codecs import open

with open('twitter_api_config.json') as f:
    apiConfig = json.load(f)
ckey = apiConfig['ckey']
consumer_secret = apiConfig['consumer_secret']
access_token_key = apiConfig['access_token_key']
access_token_secret = apiConfig['access_token_secret']

# Twitter authentication.
auth = OAuthHandler(ckey, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)

api = API(auth_handler=auth)


def reindex(es, newIndex,count,outFile):
    query = {
        "query": {
          "function_score":{
            "query":{
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
           "random_score":{},
           "boost_mode":"replace"
             }
           }
        }
    ids = []
    docs = {}
    num = 0
    inter = 0
    for doc in helpers.scan(es, index="ctrls", doc_type="twitter", query=query):
        ids.append(doc["_source"]["tweet"]["id_str"])
        docs[ids[-1]] = doc

        if len(ids) == 100:
            try:
                res = api.statuses_lookup(ids)
                for status in res:
                    tweet = status._json
                    if 'retweeted_status' not in tweet:
                        if num % 3 == 0: ##Manual
                           for update in processTweets(tweet, docs, newIndex):
                               yield update
                        else:
                            texto = tweet.get("text")
                            id = docs[tweet["id_str"]]["_id"]
                            texto = " ".join(texto.split())
                            outFile.write(id + "\t" + texto + "\n")
                        num += 1
                        if (num >= count and count != -1):
                            break
            except TweepError:
                traceback.print_exc()            

            stdout.write('.')
            stdout.flush()
            del ids[:]
            docs.clear()
        if (num >=  count and count!= -1): break    
    stdout.write('\n')
    stdout.flush()


def processTweets(tweet, docs, newIndex):
   
    if 'retweeted_status' not in tweet:
        doc = docs[tweet["id_str"]]
        action = {
            '_op_type': 'index',
            '_index': newIndex,
            '_type': 'twitter',
            '_id': doc['_id'],
            '_source': doc['_source']
        }
        yield action
    


def main():
    if len(sys.argv) != 2:
	## -1 all tweets
        print 'Missing argument: <num of tweets>'
        sys.exit(1)
    outFile = open("Automatic.txt", "wt", encoding="utf8")    
    es = Elasticsearch('localhost:9200')
    res = helpers.bulk(es, reindex(es, "manual", int(sys.argv[1]),outFile))
    print res
    print 'Finished!'
    outFile.close()

if __name__ == '__main__':
    main()
