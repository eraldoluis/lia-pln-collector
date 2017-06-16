#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from sys import stdout

import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from tweepy import OAuthHandler
from tweepy.api import API

with open('../collector/twitter_api_config.json') as f:
    apiConfig = json.load(f)
ckey = apiConfig['ckey']
consumer_secret = apiConfig['consumer_secret']
access_token_key = apiConfig['access_token_key']
access_token_secret = apiConfig['access_token_secret']

# Twitter authentication.
auth = OAuthHandler(ckey, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)

api = API(auth_handler=auth)


def reindex(es, newIndex, start):
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "start": start
                            # "start": "2017-02-20T16:33:30.542448-04:00"  # FUTEBOL
                            # "start": "2017-02-20T16:33:25.093458-04:00"  # SUPERNATURAL
                        }
                    }
                ]
            }
        }
    }

    print query
    sys.exit(0)

    ids = []
    docs = {}
    for doc in helpers.scan(es, index="ctrls", doc_type="twitter", query=query):
        ids.append(doc["_source"]["tweet"]["id_str"])
        docs[ids[-1]] = doc

        if len(ids) == 100:
            for update in processTweets(ids, docs, newIndex):
                yield update
            stdout.write('.')
            stdout.flush()
            del ids[:]
            docs.clear()

    stdout.write('\n')
    stdout.flush()


def processTweets(ids, docs, newIndex):
    res = api.statuses_lookup(ids)
    for status in res:
        tweet = status._json
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
        print 'Missing argument: <start>'
        sys.exit(1)

    es = Elasticsearch('localhost:9200')
    res = helpers.bulk(es, reindex(es, "ctrls_no_retweet", sys.argv[1]))
    print res
    print 'Finished!'


if __name__ == '__main__':
    main()
