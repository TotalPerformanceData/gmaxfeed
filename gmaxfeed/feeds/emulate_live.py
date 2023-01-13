#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 14:51:57 2021

emulate a live feed from recordings/post-race points by streaming via UDP 
to some given IP and Port, useful for testing recorder code and GUIs.

Note, lots of IDEs use asyncio to run which doesn't work well running other async
programs from the IDE console so safest option is to run from a plain python
console prompt.

@author: tpd
"""

import socket
import json
import asyncio
import dateutil
import threading
from queue import Queue, Empty
from datetime import datetime, timedelta

from .. import get_logger

logger = get_logger(name = __name__)


async def wait_until(dt: datetime, dt_now: datetime = None) -> None:
    """
    pause a coroutine execution until dt.
    Pass dt_now to give a beggining reference timestamp
    """
    dt_now = dt_now or datetime.utcnow().replace(tzinfo = dt.tzinfo)
    if dt > dt_now:
        await asyncio.sleep((dt - dt_now).total_seconds())


async def execute_at(dt: datetime,
                     coro: asyncio.coroutine,
                     dt_now: datetime = None
                     ) -> None:
    await wait_until(dt = dt, dt_now = dt_now)
    await coro


async def put(queue: Queue, data: list) -> None:
    queue.put(data)
    

def bind_and_push(HOST: str,
                  PORT: int,
                  dst: tuple,
                  queue: Queue
                  ) -> None:
    """
    wait for data (list of packets) to appear in queue for at most 5 seconds
    and push to dst, break when timeout exceeded
    """
    if type(dst) is list:
        dst = tuple(dst)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
        server.bind((HOST,PORT))
        while True:
            try:
                data = queue.get(block = True, timeout = 5)
            except Empty:
                print("No data received to send, exiting")
                break
            for row in data:
                if type(row) is dict:
                    row = json.dumps(row).encode('ascii')
                elif type(row) is str:
                    row = row.encode('ascii')
                logger.info(
                    "Sending packet to {0}: {1}".format(dst, row.decode())
                    )
                server.sendto(row, dst)
    

def prepare_emulate_data(data: dict = {'points':{}, 'progress':{}}) -> dict:
    """
    prepare the data recordings (or post-race mockups) of progress and points
    feeds into a dict for the stream to cycle and emit.
    """
    off_time = None
    end_time = None
    points = data.get('points')
    progress = data.get('progress')
    # get off time and finish time to make a shorter boundary
    if progress:
        for row in progress:
            if row['R'] > 0 and off_time is None:
                if 'T' not in row['T']: # plotly date format - "%Y-%m-%d %H:%M:%S.%f"
                    off_time = (
                        datetime.strptime(row['T'], '%Y-%m-%d %H:%M:%S.%f') - \
                        timedelta(seconds = row['R'])
                        ).replace(tzinfo = dateutil.tz.UTC)
                else: # ISO date format - "%Y-%m-%dT%H:%M:%S.%fZ"
                    off_time = dateutil.parser.parse(row['T']) - timedelta(seconds=row['R'])
            elif row['R'] == 0 and off_time is not None: # If false start, reset to None
                off_time = None
        if off_time is not None:
            end_time = off_time + timedelta(seconds = max([row['R'] for row in progress]))
    output = {}
    off_time = off_time or datetime(2000, 1, 1, tzinfo = dateutil.tz.UTC) - timedelta(seconds = 10) # default start boundary
    end_time = end_time or datetime.utcnow().replace(tzinfo = dateutil.tz.UTC) + timedelta(seconds = 20) # default end boundary
    if points:
        if type(points) is dict:
            # format will either be runnerid -> timestamps -> record
            # or timestamps -> runnerid -> record, either will be fine through this
            for k1 in points.values():
                if type(k1) is list:
                    for row in k1:
                        if off_time < dateutil.parser.parse(row['T']) < end_time:
                            if row['T'] not in output:
                                output[row['T']] = []
                            output[row['T']].append(row)
                elif type(k1) is dict:
                    for row in k1.values():
                        if off_time < dateutil.parser.parse(row['T']) < end_time:
                            if row['T'] not in output:
                                output[row['T']] = []
                            output[row['T']].append(row)
        elif type(points) is list:
            # assume records format
            for row in points:
                if off_time < dateutil.parser.parse(row['T']) < end_time:
                    if row['T'] not in output:
                        output[row['T']] = []
                    output[row['T']].append(row)
    if progress:
        for row in progress:
            if off_time < dateutil.parser.parse(row['T']) < end_time:
                if row['T'] not in output:
                    output[row['T']] = []
                output[row['T']].append(row)
    return output


def begin_stream(data: dict = {},
                 HOST: str = '0.0.0.0',
                 PORT: int = 0,
                 to_addr: tuple = None
                 ) -> None:
    """
    cycle and stream data of {
        timestamp1: [packet1, packet2, packet3, ...],
        timestamp2 :[packet1, packet2, packet3,...],
        ...
        }
    HOST and PORT are for the server sock.
    to_addr: tuple of (str, int) for the destination IP address and PORT
    """
    logger.info("Beggining stream to {0}".format(to_addr))
    try:
        timestamps = {ts:dateutil.parser.parse(ts) for ts in data}
        earliest_ts = min([v for v in timestamps.values()]) - timedelta(seconds = 2)
        latest_ts = max([v for v in timestamps.values()]) + timedelta(seconds = 2)
        queue = Queue()
        #loop = asyncio.get_event_loop() # if running in standlone process can use this,
        loop = asyncio.new_event_loop() # if spawned by something else (like in a dedicated process from celery task queue) previous line doesn't work but this does
        asyncio.set_event_loop(loop)
        for ts, dt in timestamps.items():
            logger.info(
                "creating task for ts {0} in {1}".format(
                    ts, (dt - earliest_ts).total_seconds()
                    )
                )
            loop.create_task(
                execute_at(
                    dt = dt,
                    coro = put(queue = queue, data = data[ts]),
                    dt_now = earliest_ts
                    )
                )
        final_future = asyncio.Task(
            wait_until(dt = latest_ts, dt_now = earliest_ts)
            )
        thr = threading.Thread(
            target = loop.run_until_complete,
            args = (final_future,),
            daemon = True
            )
        thr.start()
        bind_and_push(HOST = HOST, PORT = PORT, dst = to_addr, queue = queue)
        logger.info("Finished sending stream")
    except Exception:
        logger.exception("Error occured in begin stream")
    