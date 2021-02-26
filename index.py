#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The web server to handle requests from the clients to search queries 
and handle requests for crawling new web pages. The server runs on the
python web-server framework called Flask. Incoming client requests are
handled and tasks are added to Redis task queues for processing.
"""

__author__ = "Anthony Sigogne"
__copyright__ = "Copyright 2017, Byprog"
__email__ = "anthony@byprog.com"
__license__ = "MIT"
__version__ = "1.0"

import re
import os
import url
import crawler
import requests
import json
import query
from flask import Flask, request, jsonify
from language import languages
from redis import Redis
from rq import Queue
from multiprocessing import Process
from multiprocessing import Queue as Q
from twisted.internet import reactor
from rq.decorators import job
from scrapy.crawler import CrawlerRunner
from urllib.parse import urlparse
from datetime import datetime
from elasticsearch import Elasticsearch
from flask_rq2 import RQ
import logging
from twisted.internet import reactor


#Initialize the flask application
app = Flask(__name__)
with app.app_context():
    from helper import *

"""
__author__      : Bijin Benny
__email__       : bijin@ualberta.ca
__license__     : MIT
__version__     : 1.0
Modification    : The native Redis library used in the original reference is 
                  outdated and is modified to use the new redis library specific
                  to Flask apps
                  
Configure and intialize Redis task queue    
"""
app.config['RQ_REDIS_URL']='redis://localhost:6379/0'
redis_conn = RQ(app)

"""
__author__      : Bijin Benny
__email__       : bijin@ualberta.ca
__license__     : MIT
__version__     : 1.0
Modification    : The deprecated elasticsearch library elasticsearch_dsl is 
                  removed and replaced with the new elasticsearch library for 
                  ES clients

Load environment variables and create elastic search DB client 
"""
host = os.getenv("HOST")
user = os.getenv("USERNAME")
pwd  = os.getenv("PASSWORD")
port = os.getenv("PORT")
es = Elasticsearch(hosts="http://"+user+":"+pwd+"@"+host+":"+port+"/")

"""
__author__      : Bijin Benny
__email__       : bijin@ualberta.ca
__license__     : MIT
__version__     : 1.0
Modification    : Logging framework is added to the code to enable better debugging
                  through logs

Set logging information
"""
logging.basicConfig(filename=datetime.now().strftime('server_%d_%m_%Y.log'),
level=logging.DEBUG,format='%(asctime)s %(levelname)-8s %(message)s')


logging.info(es.info())

"""
__author__      : Bijin Benny
__email__       : bijin@ualberta.ca
__license__     : MIT
__version__     : 1.0
Modification    : The DB mapping/schema used in the orginal code is specific to 
                  the application the code was used for and needs to be modified
                  to store information specific to the project experiment

Database schema used to create the DB index if the server is running for
the first time. Ignores the schema if the index already exists.
"""
settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
            "properties": {
                "url": {
                    "type": "keyword"
                },
                "domain":{
                    "type": "keyword"
                },
                "title":{
                    "type": "text",
                    "analyzer": "english"
                },
                "description":{
                    "type": "text",
                    "analyzer": "english"
                },
                "body":{
                    "type": "text",
                    "analyzer": "english"
                },
                "weight":{
                    "type": "long"
                },
                "bert_vector":{
                    "type": "dense_vector",
                    "dims": 768
                },
                "laser_vector":{
                    "type": "dense_vector",
                    "dims": 1024
                }
            }
    }
    
}
es.indices.create(index='web-en',ignore=400,body=settings)


"""
Server endpoint for crawl requests. Crawl requests with list of urls
to crawl is handled by this handler
URL : /explore
Method : HTTP POST
POST Data : url - list of urls to explore
Returns success or error message depending on the task being processed
in the Redis queue.
"""
@app.route("/explore", methods=['POST'])
def explore():
 
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data :
        raise InvalidUsage('No url specified in POST data')

    logging.info("launch exploration job")
    job = explore_job.queue(data["url"])
    job.perform()

    return "Exploration started"

@redis_conn.job('low')
def explore_job(link) :
    """
    Explore a website and index all urls (redis-rq process).
    """
    logging.info("explore website at : %s"%link)

    try :
        link = url.crawl(link).url
    except :
        return 0

    def f(q):
        try:
            """
            __author__      : Bijin Benny
            __email__       : bijin@ualberta.ca
            __license__     : MIT
            __version__     : 1.0
            Modification    : The original code used CrawlerProcess class from
            scrapy library to crawl web pages. However, CrawlerProcess class could
            not run parallely in Redis tasks threads. CrawlerProcess was replaced by
            CrawlerRunner class that could run parallely in multiple Redis tasks
            """
            runner = CrawlerRunner({
                'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
                'DOWNLOAD_TIMEOUT':100,
                'DOWNLOAD_DELAY':0.25,
                'ROBOTSTXT_OBEY':True,
                'HTTPCACHE_ENABLED':False,
                'REDIRECT_ENABLED':False,
                'SPIDER_MIDDLEWARES' : {
                'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware':True,
                'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware':True,
                'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware':True,
                'scrapy.extensions.closespider.CloseSpider':True
                },
                'CLOSESPIDER_PAGECOUNT':500 #only for debug
            })
            runner.crawl(crawler.Crawler, allowed_domains=[urlparse(link).netloc],
            start_urls = [link,], es_client=es, redis_conn=redis_conn)
            d = runner.join()
            d.addBoth(lambda _: reactor.stop())
            reactor.run()
            q.put(None)
        except Exception as e:
            q.put(e)
    q = Q()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result        
    return 1


"""
Server endpoint to handle search queries from the web-client.
Forwards the query to the Elasticsearch DB and return the top
relevant results.
URL : /search
Method : HTTP POST
POST Data : query - The search query
            hits  - The number of results to be returned
            start - Start number for the hits (for pagination purpose)
"""
@app.route("/search", methods=['POST'])
def search():
    def format_result(hit, highlight) :
        #Highlight title and description
        title = hit["title"]
        description = hit["description"]
        if highlight :
            if "description" in highlight :
                description = highlight["description"][0]+"..."
            elif "body" in highlight :
                description = highlight["body"][0]+"..."
            

        #Create false title and description for better user experience
        if not title :
            title = hit["domain"]
        if not description :
            description = url.create_description(hit["body"])+"..."

        return {
            "title":title,
            "description":description,
            "url":hit["url"],
            "thumbnail":hit.get("thumbnail", None)
        }

     #Analyze and validate the user query
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    logging.info("[search request data : "+ str(data))
    if "query" not in data :
        raise InvalidUsage('No query specified in POST data')
    start = int(data.get("start", "0"))
    hits = int(data.get("hits", "10"))
    if start < 0 or hits < 0 :
        raise InvalidUsage('Start or hits cannot be negative numbers')
    groups = re.search("(site:(?P<domain>[^ ]+))?( ?(?P<query>.*))?",
        data["query"]).groupdict()
    logging.info("Expression query : " + str(groups["query"]))

    """
             __author__     : Bijin Benny
            __email__       : bijin@ualberta.ca
            __license__     : MIT
            __version__     : 1.0
            Modification    : The referenced code included searching web pages
    based on their domains as well as search queries. Domain search was irrelevant
    to the experiment use case and the code is modified to perform only query search
    Send search request to Elastic search DB with the user query
    """
    response = es.search(index="web-en",body=query.expression_query(groups["query"]))
    logging.info("Raw response" + str(response))
    results = []

    #Process, sort and return the results back to the user
    for domain_bucket in response['aggregations']['per_domain']['buckets']:
        for hit in domain_bucket["top_results"]["hits"]["hits"] :
            results.append((format_result(hit["_source"],
            hit.get("highlight", None)),hit["_score"]))

    logging.info("Before Sort Results :" + str(results))        
    results = [result[0] for result in -
    sorted(results, key=lambda result: result[1], reverse=True)]
    logging.info("After Sort Results :" + str(results))
    
    total = len(results)
    results = results[start:start+hits]
    logging.info("Total results : "+ str(total))

    return jsonify(total=total, results=results)

