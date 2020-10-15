#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 11:35:05 2020

test the GmaxFeed functions

@author: George
"""

from PostraceFeeds import GmaxFeed, RaceMetadata
from datetime import datetime, timezone
from loguru import logger

logger.add('test.log', level='DEBUG', format="{time} {level} {message}")

# set all paths here or make sure environ variable correctly set
gmax_feed = GmaxFeed()

def test_get_range(start_date = datetime(2020,10,1, tzinfo = timezone.utc), end_date = datetime(2020,10,7, tzinfo = timezone.utc), new:bool = False):
   sharecodes = gmax_feed.get_racelist_range(start_date = start_date, end_date = end_date, new = new)
   return sharecodes

def test_get_data(date:datetime = datetime(2020,10,2, tzinfo = timezone.utc), request:set = {'sectionals', 'sectionals-history', 'points', 'obstacles'}, new:bool = False, filter:RaceMetadata = None):
    sharecodes = gmax_feed.get_racelist(date = date, new = new)
    data = gmax_feed.get_data(sharecodes = sharecodes, request = request, new = new, filter = filter)
    return data

def test_filter(filter:RaceMetadata = None) -> filter:
    if filter is None:
        filter = RaceMetadata()
        filter.set_filter(countries = {'GB', 'CA'}, start_date = datetime(2020,10,1, tzinfo = timezone.utc), end_date = datetime(2020,10,7, tzinfo = timezone.utc), published = True)
    gmax_feed.update(start_date = '2020-09-25', end_date = '2020-10-02', request = {'sectionals'}, filter = filter)
    return filter

if __name__ == '__main__':
    try:
        # should return dict of racelist feed, sharecode->metadata
        racelist_range = test_get_range()
    except Exception:
        logger.exception('Error in racelist range generation')
        
    try:
        # should return a RaceMetadata object, if filter with some restrictions are applied then len(filter._data) != len(filter._list)
        filter = test_filter()
        print(len(filter._data), len(filter._list))
    except Exception:
        logger.exception('Error in racelist range generation')
        
    try:
        # returns data for all the given request types, if available and permitted
        daily_data = test_get_data()
    except Exception:
        logger.exception('Error in racelist range generation')
    