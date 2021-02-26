#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import os
import url
import scrapy
import base64
import io
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
from language import languages
from collections import Counter
from rq.decorators import job
from rq import Queue
from elasticsearch import Elasticsearch

host = os.getenv("HOST")
user = os.getenv("USERNAME")
pwd  = os.getenv("PASSWORD")
port = os.getenv("PORT")
client = Elasticsearch(hosts="http://"+user+":"+pwd+"@"+host+":"+port+"/")


class Crawler(scrapy.spiders.CrawlSpider):
    """
    Explore a website and index all urls.
    """

    name = 'crawler'
    handle_httpstatus_list = [301, 302, 303] # redirection allowed
    rules = (
        # Extract all inner domain links with state "follow"
        Rule(LinkExtractor(), callback='parse_items', follow=True, process_links='links_processor'),
    )

    def parse(self):
        pass
    
    def links_processor(self,links):
        """
        A hook into the links processing from an existing page, done in order to not follow "nofollow" links
        """
        ret_links = list()
        if links:
            for link in links:
                if not link.nofollow:
                    ret_links.append(link)
        return ret_links

    def parse_items(self, response):
        """
        Parse and analyze one url of website.
        """
        yield pipeline(response, self)

def pipeline(response, spider) :
    """
    Index a page.
    """
    # skip rss or atom urls
    if not response.css("html").extract_first() :
        return

    # get domain
    domain = url.domain(response.url)

    # extract title
    title = response.css('title::text').extract_first()
    title = title.strip() if title else ""

    # extract description
    description = response.css("meta[name=description]::attr(content)").extract_first()
    description = description.strip() if description else ""

    # get main language of page, and main content of page
    lang = url.detect_language(response.body)
    if lang not in languages :
        raise InvalidUsage('Language not supported')
    body, boilerplate = url.extract_content(response.body, languages.get(lang))

    # weight of page
    weight = 3
    if not title and not description :
        weight = 0
    elif not title :
        weight = 1
    elif not description :
        weight = 2
    if body.count(" ") < boilerplate.count(" ") or not url.create_description(body) :
        # probably bad content quality
        weight -= 1

    res = spider.es_client.index(index="web-%s"%lang, id=response.url, body={
        "url":response.url,
        "domain":domain,
        "title":title,
        "description":description,
        "body":body,
        "weight":weight
    })


    if response.status in spider.handle_httpstatus_list and 'Location' in response.headers:
        newurl = response.headers['Location']
        meta = {'dont_redirect': True, "handle_httpstatus_list" : spider.handle_httpstatus_list}
        meta.update(response.request.meta)
        return Request(url = newurl.decode("utf8"), meta = meta, callback=spider.parse)


