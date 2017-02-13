#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
from datetime import datetime
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch

import logging
import logging.config


class Listener(StreamListener):
    def __init__(self, terms=None):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.start = datetime.now()
        self.terms = terms
        self.count = 0

    def on_data(self, data):
        try:
            # Parse JSON data.
            tweet = json.loads(data)

            # Filter some fields of interest.
            user = tweet.get('user', {})
            tweet = {
                'id': tweet.get('id'),
                'id_str': tweet.get('id_str'),
                'user_id': user.get('id'),
                'user_id_str': user.get('id_str'),
                'user_screen_name': user.get('screen_name'),
                'user_name': user.get('name'),
                'user_location': user.get('location'),
                'user_created_at': user.get('created_at'),
                'user_verified': user.get('verified'),
                'text': tweet.get('text'),
                'created_at': tweet.get('created_at'),
                'coordinates': tweet.get('coordinates'),
                'lang': tweet.get('lang')
            }

            # print json.dumps(tweet, indent=2)

            self.count += 1
            if (self.count % 100) == 0:
                logging.info('%d tweets' % self.count)

            # Save tweet to ElasticSearch.
            self.es.index(index="ctrls",
                          doc_type="twitter",
                          body={
                              "start": self.start,
                              "terms": self.terms,
                              "count": self.count,
                              "tweet": tweet
                          })

            return True

        except Exception as e:
            logging.error(e)
            return True

    def on_error(self, status):
        logging.error(status)
        return True


def decodeUtf8(txt):
    if type(txt) != unicode:
        txt = txt.decode('utf-8')
    return txt


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.stderr.write('\n')
        sys.stderr.write('Syntax error! Expected argument: <list of terms>\n')
        sys.stderr.write('\n')
        sys.stderr.write('The list of terms is a space-separated list of strings. ')
        sys.stderr.write('Each string can be composed by more than one term if it ')
        sys.stderr.write('is surrounded by double quote marks (").\n')
        sys.stderr.write('For example: "sao paulo" santos.\n')
        sys.stderr.write('\n')
        sys.exit(1)

    # Logging.
    logging.config.fileConfig('logging.conf')

    logging.info('Starting...')

    # Criar uma nova app e obter estes dados em https://apps.twitter.com
    # Salvar os dados no arquivo JSON abaixo.
    with open('twitter_api_config.json') as f:
        apiConfig = json.load(f)
    ckey = apiConfig['ckey']
    consumer_secret = apiConfig['consumer_secret']
    access_token_key = apiConfig['access_token_key']
    access_token_secret = apiConfig['access_token_secret']

    keyword_list = [decodeUtf8(s) for s in sys.argv[1:]]

    logging.info("Search terms: " + str(keyword_list))

    # Twitter authentication.
    auth = OAuthHandler(ckey, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)

    # Create the stream object.
    twitterStream = Stream(auth, Listener(terms=keyword_list))

    while True:
        # Start tracking.
        twitterStream.filter(track=keyword_list, languages=['pt'])
        # If disconnected, sleep for 5 minutes and try again.
        time.sleep(5 * 60)
