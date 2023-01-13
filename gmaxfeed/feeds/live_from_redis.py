#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 23:26:32 2021

Python3 DGRAM seems to not propagate the socket REUSEPORT flags correctly,
and even worse the port gets indefinitely blocked when you restart the program
a few times, even after restarting the computer (on a cloud VM). This is a 
difficult problem to detect and I can never be certain if an error is on my
side or in the Gmax servers.

If you can't be bothered writing loads of low level C or Rust code to direct the 
packet to the correct place, it's a good solution to just use the executable
to add packets to a redis queue (or RabbitMQ) and then python3 can pop
those off at leisure without worry of missing packets.

Using a MQ like redis is also benficial in systems where many programs may want
to use the datafeed simultaneously, since many queues can be used

@author: tpd
"""

import os
import json
from datetime import datetime
from redis.client import Redis

from . import get_logger

logger = get_logger(name = __name__)
logger.info("booting recorder at {0}".format(datetime.utcnow()))

REDIS_CLIENT = Redis(password = os.environ.get("REDIS_PASSWD"))

_main_dir = os.environ.get("MAIN_DIR") or '..'


def add_directory(raceid:str, path:str) -> None:
    logger.info("adding new folder to test_store - {0}".format(raceid))
    os.mkdir(path)
    os.mkdir(os.path.join(path, 'points'))
    os.mkdir(os.path.join(path, 'progress'))
    os.mkdir(os.path.join(path, 'probs'))


def deal_with_datagram(data:str) -> None:
    data = json.loads(data)
    if data['K'] == 0: # points
        # raceid at least for now should always be first 14 chars
        raceid = data['I'][:14]
        par_fol = os.path.join(_main_dir, 'test_store', raceid)
        if not os.path.exists(par_fol):
            add_directory(raceid = raceid, path = par_fol)
        if data['V'] > 23.5:
            data['V'] = 18.
        string = '{0},{1},{2},{3},{4},{5}\n'.format(
            data['T'][:10] + ' ' + data['T'][11:-1],
            data['X'],
            data['Y'],
            data['V'],
            data['SF'],
            data['P']
            )
        with open(os.path.join(par_fol, 'points', data['I']+'.txt'), 'a') as f:
            f.write(string)
    elif data['K'] == 5: # progress
        raceid = data['I']
        par_fol = os.path.join(_main_dir, 'test_store', raceid)
        if not os.path.exists(par_fol):
            add_directory(raceid = raceid, path = par_fol)
        data['T'] = data['T'][:10] + ' ' + data['T'][11:-1] # for benefit of plotly in client side, but it's horrible that plotly doesn't recognise ISO
        string = json.dumps(data) + '\n'
        with open(os.path.join(par_fol, 'progress', data['I']+'.txt'), 'a') as f:
            f.write(string)
    elif data['K'] == 6: # probabilities
        raceid = data['I'][:14]
        par_fol = os.path.join(_main_dir, 'test_store', raceid)
        if not os.path.exists(par_fol):
            add_directory(raceid = raceid, path = par_fol)
        string = json.dumps(data) + '\n'
        with open(os.path.join(par_fol, 'probs', data['I']+'.txt'), 'a') as f:
            f.write(string)

def main() -> None:
    while True:
        d = REDIS_CLIENT.brpop('test_queue')  # get data from front of queue, wait indefinitely
        if d and d[1]:
            d = d[1]
            if d == b'terminate':
                break
            try:
                data = d.decode('ascii')
            except Exception: # any exception will only be from decode if some unexpected data is received to port
                logger.exception('error decoding packet from redis queue')
                continue
            if data:
                deal_with_datagram(data = data)


if __name__ == "__main__":
    main()
    
