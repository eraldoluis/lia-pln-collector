#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import traceback
from datetime import datetime
from dateutil.parser import parse
from dateutil import tz

import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch

import logging
import logging.config


class ResilientListener(StreamListener):
    def __init__(self, terms, es, auth):
        super(ResilientListener, self).__init__()
        self.terms = terms
        self.es = es
        self.auth = auth
        self.count = 0
        self.startTime = None
        self.sleepTime = None

    def start(self):
        # Start time (this can be used as an ID).
        self.startTime = datetime.now(tz.tzlocal())

        logging.info("Start: " + str(self.startTime))

        # Create the stream object.
        twitterStream = Stream(self.auth, self)

        # When an error occurs, sleep for 1 second and try again. If the connection fails, double this time.
        # If the connection goes through and some tweets are sent, restart this time interval.
        self.sleepTime = None

        while True:
            try:
                # Start tracking.
                twitterStream.filter(track=self.terms, languages=['pt'])
            except Exception as e:
                logging.error(e)
                print traceback.format_exc()

            if self.sleepTime is None:
                self.sleepTime = 1
            else:
                self.sleepTime *= 2

            # If disconnected, sleep for a while.
            logging.info('Sleeping for %d seconds...' % self.sleepTime)
            time.sleep(self.sleepTime)

    def on_data(self, data):
        try:
            # Parse JSON data.
            tweet = json.loads(data)

            # print json.dumps(tweet, indent=2)

            # Filter some fields of interest.
            user = tweet.get('user', {})
            tweet = {
                'id': tweet.get('id'),
                'id_str': tweet.get('id_str'),
                'text': tweet.get('text'),
                'created_at': parse(tweet.get('created_at')) if 'created_at' in tweet else None,
                'coordinates': tweet.get('coordinates'),
                'lang': tweet.get('lang'),
                'user': {
                    'id': user.get('id'),
                    'id_str': user.get('id_str'),
                    'screen_name': user.get('screen_name'),
                    'name': user.get('name'),
                    'location': user.get('location'),
                    'created_at': parse(user.get('created_at')) if 'created_at' in user else None,
                    'verified': user.get('verified')
                }
            }

            self.count += 1
            if (self.count % 100) == 0:
                logging.info('%d tweets' % self.count)

            # Save tweet to ElasticSearch.
            self.es.index(index="ctrls",
                          doc_type="twitter",
                          body={
                              "start": self.startTime,
                              "terms": self.terms,
                              "count": self.count,
                              "tweet": tweet
                          })

            # Reset sleep time due to error.
            self.sleepTime = None

            return True

        except Exception as e:
            logging.error(e)
            print traceback.format_exc()
            return True

    def on_error(self, status):
        logging.error(status)
        return False


def decodeUtf8(txt):
    """
    Make sure the given string is encoded as UTF8.

    :param txt: input string
    :return: input string encoded as UTF8
    """
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

    # Make sure input terms are encoded as UTF8.
    keyword_list = [decodeUtf8(s) for s in sys.argv[1:]]

    logging.info("Search terms: " + str(keyword_list))

    # Twitter authentication.
    auth = OAuthHandler(ckey, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)

    # Create the stream object.
    listener = ResilientListener(terms=keyword_list,
                                 es=Elasticsearch([{'host': 'localhost', 'port': 9200}]),
                                 auth=auth)
    listener.start()
