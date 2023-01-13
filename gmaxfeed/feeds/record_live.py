#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 09:29:47 2019

Basic example of how to listen for packets and avoid missing packets during periods of high congestion.

Suggestion for use:
    We often cover 4 meetings simultaneously, at this much traffic a python program using threads 
    to save the packet to the appropriate place will likely start to miss packets whilst the process is 
    busy working on the thread fileio stuff and a packet arriving at the socket isn't read in time before the next.
    
    To avoid this, 1 process can be dedicated to listening for packets all the time enqueueing the packets
    for another process to handle the logic to decide where the packet should be saved.
    
    Use 2 processes, 1 to listen to updates and 1 to save files.
    in P1, use 2 threads, 1 for UserTerminate class and 1 for socket.listen() to add packet to queue.
    in P2, dequeue and save packets
    
    To avoid the port blocking upon exiting the program the UserTerminate class can be used to break the listener loop,
    and can be improved with the GracefulExit SIGTERM interceptor from utils.

Issues in Practice:
    I've used the above description as the foundation for my recorders for the last year or so and it's never missed a 
    packet, however there have been oddities that are hard to fathom. The flags REUSEPORT and REUSEADDR don't seem to 
    perform the expected behaviour when passing with the python socket api, eg you should be able to multicast from a port
    for two separate processes when both pass the REUSEPORT flag but this isn't the case (2021-02-03, ubuntu and osx tests), 
    the most recent process to bind to the port just hijacks the port. Strangely, if the second process then releases the 
    port the first process begins to receive packets again.
    This isn't ideal but not the end of the world, the bigger problem is when the programs aren't gracefully shutdown the
    port remains blocked for a couple of minutes after and if you restart the program without allowing it to unblock
    the port will not become unblocked at all after any amount of time resulting in loss of all data packets and probably no
    warning. 
    This behaviour is the same regardless of whether I pass the REUSEPORT flag or not. Even stranger still, this behaviour
    persists through a computer reboot (Digital Ocean shared instance - Ubuntu)
    Finally, this behaviour also presents challenges in using the data for multiple applications. The Redis method in "rust-listener"
    can solve this by simply using as many redis queues as there are applications (or as many queues as there are data preparation processes).
    redis is a low latency in memory database which can be used easily as a message queue and lends itself very well to these applications.
    
    I'm not confident then in using the python socket api for a life-or-death deployment, even if using duplicate
    redundancy feeds directed to different ports from multiple Gmax sources.
    
    As such I've written a small but functional listener in Rust to handle it instead. This is included in the repo under directory "rust-listener"
    and includes options to handle the packets into a file structure itself or add the packets to a redis queue for other processes to get.

@author: George Swindells
@email: george.swindells@totalperformancedata.com

"""
import socket, threading, json, os
import multiprocessing as mp
from datetime import datetime, timedelta

_dir = os.path.abspath(os.path.dirname(__file__))
DIREC = os.path.join(_dir, "TPDLiveRecording")
if not os.path.exists(DIREC):
    os.mkdir(DIREC)

_par_dir, _ = os.path.split(_dir)

from .. import get_logger
logger = get_logger(name = __name__)


# to terminate user can input 't' for a more graceful exit
class UserTerminate:
    
    def userTerminate(self):
        while True:
            inp = input()
            if inp == "t":
                self.term = True
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as serverSocket:
                    serverSocket.bind(('127.0.0.1',60000))
                    data = b'terminate activated'
                    serverSocket.sendto(data, ('127.0.0.1', 4629))
                break
        
    def __init__(self):
        self.term = False


def file_management(q:mp.Queue) -> None: # function for secondary file management process, input of queue
    
    def file_save(data:str, tstamp:str, sc:str) -> None:
        with open(os.path.join(DIREC, sc), 'a', newline = '\r\n') as wfile:
            wfile.write(tstamp + ';' + data)
    
    def deal_with_datagram(data:str, address:str, ts) -> None:
        try:
            data2 = json.loads(data)
        except Exception:
            logger.exception("Encountered json.loads() error: {0} - {1} - {2} ".format(data, address, ts))
            return
        file_save(data = data, tstamp = str(ts), sc = data2['I'])
            
    while True:
        d = q.get() # get data from front of queue, wait indefinitely
        if d[0] == b'terminate activated':
            break # if userTerminate activated on concurrent process, put 'terminate' in queue to instruct this process to exit also
        try:
            tReceived = datetime.utcnow()
            print(repr(d[0]))
            data = d[0].decode('ascii')
        except Exception: # any exception will only be from decode if some unexpected data is received to port
            logger.exception(' {0} - {1}'.format(str(d), str(tReceived)))
            continue
        
        deal_with_datagram(data, d[1], tReceived)
        

if __name__ == '__main__':
    q = mp.Queue()
    p = mp.Process(target = file_management, args = (q,))
    p.start()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        ut = UserTerminate()
        x = threading.Thread(target = ut.userTerminate)
        x.start()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('',4629)) #(HOST='', PORT=4629)
        while True: # could use GracefullExit class from utils here to try to avoid port blocking on shutdown
            # wait for data received...
            data, addr = s.recvfrom(4096)
            q.put((data, addr))
            if ut.term:
                print("user terminated...")
                break
    
