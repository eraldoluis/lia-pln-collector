#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.parser import parse
from dateutil import tz

from elasticsearch import Elasticsearch
from elasticsearch import helpers

import logging.config


def reindexTwitter(es, new_index):
    for d in helpers.scan(es, index="ctrls", doc_type="twitter"):
        src = d['_source']
        action = {
            '_op_type': 'index',
            '_index': new_index,
            '_type': 'twitter',
            '_id': d['_id'],
            '_source': {
                'start': parse(src['start'], default=datetime.now(tz=tz.tzlocal())),
                'terms': src['terms'],
                'count': src['count'],
                'tweet': {
                    'id': src['tweet']['id'],
                    'id_str': src['tweet']['id_str'],
                    'text': src['tweet']['text'],
                    'created_at': parse(src['tweet']['created_at']) if src['tweet']['created_at'] is not None else None,
                    'coordinates': src['tweet']['coordinates'],
                    'lang': src['tweet']['lang'],
                    'user': {
                        'id': src['tweet']['user_id'],
                        'id_str': src['tweet']['user_id_str'],
                        'screen_name': src['tweet']['user_screen_name'],
                        'name': src['tweet']['user_name'],
                        'location': src['tweet']['user_location'],
                        'created_at': parse(src['tweet']['user_created_at']) if src['tweet']['user_created_at'] is not None else None,
                        'verified': src['tweet']['user_verified']
                    }
                }
            }
        }
        yield action


if __name__ == "__main__":
    # Logging.
    logging.config.fileConfig('logging.conf')
    logging.info('Starting...')

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    res = helpers.bulk(es, reindexTwitter(es, "ctrls_001"))
    print res
    logging.info('Finished!')
