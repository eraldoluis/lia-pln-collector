# Import the necessary package to process data in JSON format
import sys
import time
import traceback
from datetime import datetime
from dateutil.parser import parse
from dateutil import tz

import logging
import logging.config

try:
    import json
except ImportError:
    import simplejson as json

# Import the necessary methods from "twitter" library
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

# Variables that contains the user credentials to access Twitter API 
with open('twitter_api_config.json') as f:
	apiConfig = json.load(f)
ACCESS_TOKEN = apiConfig["access_token_key"]
ACCESS_SECRET = apiConfig["access_token_secret"]
CONSUMER_KEY = apiConfig["ckey"]
CONSUMER_SECRET = apiConfig["consumer_secret"]

logging.config.fileConfig('logging.conf')
sys.stderr.write('Start Tracking\n')

oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

# Initiate the connection to Twitter Streaming API
twitter_stream = TwitterStream(auth=oauth)
sleepTime = 0
while True:
	try:
		# Get a sample of the public data following through Twitter
		iterator = twitter_stream.statuses.sample(language = "pt")
		for tweet in iterator:

    			json.dumps(tweet)
			user = tweet.get('user', {})
			#Filtrar campos do tweet  	
    			 tweet = {
                		'id': tweet.get('id'),
                		'id_str': tweet.get('id_str'),
                		'text': tweet.get('text'),
                		'created_at': parse(tweet.get('created_at')) if 'created_at' in tweet else None,
                		'coordinates': tweet.get('coordinates'),
                		'lang': tweet.get('lang'),
                '		user': {
                    		'id': user.get('id'),
                    		'id_str': user.get('id_str'),
                    		'screen_name': user.get('screen_name'),
                    		'name': user.get('name'),
                    		'location': user.get('location'),
                    		'created_at': parse(user.get('created_at')) if 'created_at' in user else None,
                    		'verified': user.get('verified')
                		}
            		}
    			# Twitter Python Tool wraps the data returned by Twitter 
    			# as a TwitterDictResponse object.
    			# We convert it back to the JSON format to print/score
    			print tweet
   			 # The command below will do pretty printing for JSON data, try it out
   			 #print json.dumps(tweet, indent=4)
		sleepTime = 0
        except Exception as e:
        	sys.stderr.write(str(e)+ '\n')
		if sleepTime is 0:
                	sleepTime = 1
            	else:
                	sleepTime *= 2

	# If disconnected, sleep for a while.
        sys.stderr.write('Sleeping for %d seconds...\n' % sleepTime)
        time.sleep(sleepTime)
