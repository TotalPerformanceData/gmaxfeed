# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 09:29:47 2019
Use 2 processes, 1 to listen to updates and 1 to save files.
in P1, use 2 threads, 1 for userTerminate class and 1 for socket.listen() to add packet to queue.
in P2, dequeue and save packets
@author: GSwindells
"""
import socket, threading, csv, json, sys, logging, os
import multiprocessing as mp
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

if not os.path.exists("TPDLiveRecording"):
    os.mkdir("TPDLiveRecording")

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
        with open(os.path.join("TPDLiveRecording", sc), 'a', newline = '\r\n') as wfile:
            wfile.write(tstamp + ';' + data)
    
    def dealWithDatagram(data, address, ts):
        try:
            data2 = json.loads(data)
        except Exception:
            logger.exception("Encountered json.loads() error: %s - %s - %s ", data, address, ts)
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
            logger.exception(' %s - %s', str(d), str(tReceived))
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
    
