#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 11:23:17 2021

perpetual background process to schedule daily updates of the racelist, 
sectionals, points, etc.

similar logic would easily go into a bash+crontab job, but easier to assess activity,
control the environment vars and working directory for a supervisord job.

must be run as main, or run with supervisord.

@author: George
"""

import os
import sys
import threading
from datetime import datetime, timedelta
import time as systime
from schedule import Scheduler

from gmaxfeed.feeds import get_logger
from gmaxfeed.feeds.postrace_feeds import GmaxFeed


logger = get_logger(name = "main_background_update")

GMAX_FEED = GmaxFeed()


class BackgroundGmaxUpdater(Scheduler):

    def __init__(self,
                 requests: list = ["sectionals", "points"],
                 ) -> None:
        super().__init__()
        logger.info("Initiating BackgroundGmaxUpdater - {0}".format(self))
        self.active = True
        self.requests = requests
        
        # list background jobs here
        self.every(2).hours.do(self._updater)
        
        # thread to handle checking of jobs queue
        self.thr = threading.Thread(target = self.driver, daemon = True)
        self.thr.start()

    def __repr__(self) -> str:
        return "< BackgroundGmaxUpdater >"

    def _updater(self):
        """
        call GMAX_FEED.update() with new start and end date params, as well as 
        self.requests.
        """
        logger.info("fetching gmax updates...")
        start_date = datetime.utcnow() - timedelta(days = 10)
        end_date = datetime.utcnow()
        _ = GMAX_FEED.update(
            start_date = start_date,
            end_date = end_date,
            request = self.requests
            )

    def driver(self):
        # driver thread for the background scheduled tasks
        while self.active:
            self.run_pending()
            systime.sleep(30)

    def terminate(self) -> None:
        # clears jobs from Scheduler
        self.active = False
        self.clear()


if __name__ == "__main__":
    requests = [
        x for x in [
            "sectionals",
            "points",
            "sectionals-raw",
            "sectionals-history",
            "performance",
            "obstacles"
            ]
        if x in sys.argv or x in os.environ
        ]
    requests = requests or ["sectionals", "points"]
    logger.info("running main_background_update.py with args: {0}".format(requests))
    background_driver = BackgroundGmaxUpdater(requests = requests)
    while True:
        systime.sleep(3600)
