#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 12:18:11 2020

Run to update files for some given daterange. Should only be run as main with
this as home directory else imports wont work.

To schedule updates in another program, use

"from gmaxfeed.feeds.postrace_feeds import update; update()"

To get daily updates automatically, can use linux crontab, or windows task
scheduler to run this (don't forget to set env variables).
a simple package like scheduler is also useful to place within other existing
applications, for a basic example-

from gmaxfeed.feeds.postrace_feeds import update
import schedule, threading, time
schedule.every().day.at("06:30").do(update)

def fun():
    while True:
        schedule.run_pending()
        time.sleep(300)

thr = threading.Thread(target = fun)
thr.start()


@author: tpd
"""

if __name__ == '__main__':
    from gmaxfeed.feeds.postrace_feeds import update
    update()
    