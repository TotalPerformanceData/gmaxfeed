#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 09:29:47 2019
Use 2 processes, 1 to listen to updates and 1 to save files.
in P1, use 2 threads, 1 for userTerminate class and 1 for socket.listen() to add packet to queue.
in P2, dequeue and save packets
@author: GSwindells
"""
import socket, threading, json, os
import multiprocessing as mp
from datetime import datetime, timedelta

_dir = os.path.abspath(os.path.dirname(__file__))
DIREC = os.path.join(_dir, "TPDLiveRecording")
if not os.path.exists(DIREC):
    os.mkdir(DIREC)

_par_dir, _ = os.path.split(_dir)
from loguru import logger
logger.add(os.path.join(_par_dir, 'logs', 'live_recording.log'), level='INFO', format="{time} {level} {message}")


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


def fileManagement(q): # function for secondary file management process, input of queue
    
    def fileSave(data, tstamp, sc):
        with open(os.path.join(DIREC, sc), 'a', newline = '\r\n') as wfile:
            wfile.write(tstamp + ';' + data)
    
    def dealWithDatagram(data, address, ts):
        try:
            data2 = json.loads(data)
        except Exception:
            logger.exception("Encountered json.loads() error: {0} - {1} - {2} ".format(data, address, ts))
            return
        fileSave(data, str(ts), data2['I'])
            
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
        
        dealWithDatagram(data, d[1], tReceived)
        

if __name__ == '__main__':
    q = mp.Queue()
    p = mp.Process(target=fileManagement, args=(q,))
    p.start()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        ut = UserTerminate()
        x = threading.Thread(target = ut.userTerminate)
        x.start()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('',4629)) #(HOST='', PORT=4629)
        while True:
            ### wait for data received...
            data, addr = s.recvfrom(4096)
            q.put((data, addr))
            if ut.term:
                print("user terminated...")
                break
    
