from sys import stdin, stdout
import csv
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def getTextFromCsv():
    reader = csv.reader(open("/home/eraldo/lia/src/lia-pln-datasets-models/lucas_sa_twitter/eleicoes_cg_labeled.csv"))
    # reader = csv.reader(stdin)
    header = reader.next()
    idxText = header.index("text")
    idxPolarity = header.index("polarity")
    for line in reader:
        yield (" ".join(line[idxText].split()), line[idxPolarity])


def getTextFromES():
    es = Elasticsearch("localhost:9200")
    query = {
        "size": 10,
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "start": [
                                "2017-02-20T16:33:25.093458-04:00",
                                "2017-02-20T16:33:30.542448-04:00"
                            ]
                        }
                    }
                ]
            }
        }
    }

    for doc in scan(es, query=query):
        try:
            text = doc["_source"]["tweet"]["text"]
            if text is not None:
                yield text
            else:
                stdout.write("x")
        except:
            stdout.write("X")


def main():
    numLines = 0
    for (text, polarity) in getTextFromCsv():
        print "%s\t%s" % (text, polarity)
        numLines += 1


if __name__ == "__main__":
    main()
