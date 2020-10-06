# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 11:48:33 2019

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

from Utils import listdir2, readUrl, logger, export_sectionals_to_xls
import urllib, json, os, time, dateutil
from datetime import datetime, timedelta


def getDaysRaces(licenceKey, date = False, courses = None, country = None, published = None):
    
    dictData = dict()
    if not date:
        date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
        
    if type(date) is datetime:
        date = date.strftime('%Y-%m-%d')
    
    response = False
    idx = 0
    while True:
        response = readUrl('http://www.gmaxequine.com/TPD/client/racelist.ashx?DateLocal=' + date + '&k=' + licenceKey)
        if response: # if readUrl successful break and return the response
            break
        else:
            time.sleep(1)
            idx += 1
            if idx == 5:
                break
    
    try:
        data = json.load(response)
        for k in data:
            dictData[k['I']] = k
    except Exception:
        logger.exception('json load Error: '+ date)
        
    if published is not None:
        for s in list(dictData.keys()):
            if dictData[s]['Published'] != published:
                dictData.pop(s)
                continue
    
    if courses is not None:
        if type(courses) is str:
            courses = [courses]
        for s in list(dictData.keys()):
            for c in courses:
                if c.lower() in dictData[s]['Racecourse'].lower():
                    break
                if c == courses[-1]:
                    dictData.pop(s)
    
    if country is not None:
        if type(country) is str:
            country = [country]
        for s in list(dictData.keys()):
            for c in country:
                if dictData[s]['Country'].lower() == c.lower():
                    break
                if c == country[-1]: # if c gets to end of list and not broken loop then condition not satisfied and s is popped
                    dictData.pop(s)
        
    return dictData



def update_racelist(licence_key:str, start_date:datetime, end_date=None, direc='./racelist', force_overwrite=False):
    # update the contents of the racelist folder if the modification time of the file is from the same date as the fname
    
    files = set(listdir2(direc))
    
    if end_date is None or end_date < start_date:
        end_date = datetime.utcnow() - timedelta(days=1)
    
    while start_date < end_date: # post race points aren't available until a couple days after race usually
        date = start_date.strftime('%Y-%m-%d')
        path = os.path.join(direc, date)
        if date not in files:
            temp = getDaysRaces(licence_key, date=date, published = None)
            with open(path, 'w') as f:
                json.dump(temp, f)
        else:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if force_overwrite or mtime < start_date + timedelta(days=7): # file was created on the same week, might have missed later races or publish change so get a replacement
                temp = getDaysRaces(licence_key, date=date, published = None)
                with open(path, 'w') as f:
                    json.dump(temp, f)
        start_date += timedelta(days=1)
        
        
def get_racelist_range(licence_key:str=None, start_date:datetime=None, end_date=None, direc='./racelist', force_overwrite=False):
    
    if licence_key is not None:
        update_racelist(licence_key, start_date, end_date=end_date, direc=direc, force_overwrite=force_overwrite)
    
    racelist_contents = set(listdir2(direc))
    
    if end_date is None:
        end_date = dateutil.parser.parse(max(racelist_contents))
    if start_date is None:
        start_date = dateutil.parser.parse(min(racelist_contents))
        
    sharecodes = {}
    dt = start_date
    while dt <= end_date:
        date = dt.strftime('%Y-%m-%d')
        if date not in racelist_contents:
            if licence_key is not None:
                temp = getDaysRaces(licence_key, date=date, published = None)
                with open(os.path.join(direc, date), 'w') as f:
                    json.dump(temp, f)
        else:
            with open(os.path.join(direc, date), 'r') as f:
                temp = json.load(f)
        sharecodes.update(temp)
        dt += timedelta(days=1)
    
    return sharecodes


def read_json(path:str) -> list:
    data = None
    if os.path.exists(path):
        with open(path, 'r') as file:
            data = json.load(file)
    return data


def reformat_sectionals_list(data:list) -> dict:
    # reformat list of dictionaries into dictionary of runners:{timestamps}
    runners = sorted(list(set([row['I'] for row in data])))
    d = {runner:{} for runner in runners}
    for runner in runners:
        d[runner] = {row['G']:row for row in data if row['I']==runner}
    return d


def getSectionals(licenceKey, sharecode):
    
    response = False
    idx = 0
    while True:
        response = readUrl('https://www.gmaxequine.com/TPD/client/sectionals.ashx?Sharecode=' + sharecode + '&k=' + licenceKey)
        if response:
            break
        else:
            time.sleep(1)
            idx += 1
            if idx == 5:
                break
    
    if response:
        return json.load(response)
    else:
        return False


def getGPSData(licence, sharecode):
    
    data = list()
    response = False
    idx = 0
    while True:
        response = readUrl('http://www.gmaxequine.com/TPD/client/points.ashx?Sharecode=' + sharecode + '&k=' + licence)
        if response:
            break
        else:
            time.sleep(1)
            idx += 1
            if idx == 5:
                break
            
    if response:
        lines = response.readlines()
        data = [json.loads(line) for line in lines]
    
    return data


def getTrackTimeHistory(licence, sharecode):
    
    data = list()
    with urllib.request.urlopen('http://www.gmaxequine.com/TPD/client/sectionals-history.ashx?Sharecode=' + sharecode + '&k=' + licence) as response:
        if response:
            lines = response.readlines()
            data = [json.loads(line) for line in lines]
    
    return data


def getObstacleLocations(licence, sharecode):
    
    data = list()
    with urllib.request.urlopen('http://www.gmaxequine.com/TPD/client/jumps.ashx?Sharecode=' + sharecode + '&k=' + licence) as response:
        if response:
            lines = response.readlines()
            data = [json.loads(line) for line in lines]
    
    return data


def getRouteFile(licence, cc):
    data = False
    with urllib.request.urlopen('http://www.gmaxequine.com/TPD/client/routes.ashx?Racecourse=' + cc + '&k=' + licence) as response:
        data = response.read().decode('utf-8')
    return data


def updateRouteFiles(licence, us = False):
    # cycle course codes and download/save route files from gmax if available
    route_folder = 'RouteFiles'
    if not os.path.exists(route_folder):
        os.mkdir(route_folder)
    for cc in [1,3,4,6,11,12,14,17,19,23,24,30,35,37,40,43,46,47,53,57,58,59,61,64,71,72,73,74,75,76,77,78,79,80,81,82,90,91]:
        if cc > 70 and not us:
            break
        cc = str(cc).zfill(2)
        data = getRouteFile(licence, cc)
        if data and data != 'Permission Denied' and data != '[]':
            with open(os.path.join(os.path.abspath(route_folder), 'Racecourse-{}.kml'.format(cc)), 'w') as f:
                f.write(data)


def load_all_sectionals(start_date:datetime=None, end_date:datetime=None, direc:str='sectionals') -> dict:
    sharecodes = get_racelist_range(start_date=start_date, end_date=end_date)
    for sc in sharecodes:
        data = read_json(os.path.join(direc, sc))
        if data is not None:
            data = reformat_sectionals_list(data)
            sharecodes[sc]['sectionals'] = data
    data = export_sectionals_to_xls(sharecodes)
    return data

# when run, checks the previous week for published races, and checks/downloads GPS points feed and sectionals
# if licence key is only activated for one of the above feeds then comment out or remove lines for unauthorsied feed or will have folder full of 'Permission Denied' text files
if __name__ == '__main__':
    
    licence_key = '' # paste gmax licence key in here
    
    gpsPath = 'gpsData'
    if not os.path.exists(gpsPath):
        os.mkdir(gpsPath)
    gpsContents = set(listdir2(gpsPath))
    
    secPath = 'sectionals'
    if not os.path.exists(secPath):
        os.mkdir(secPath)
    secContents = set(listdir2(secPath))

    racelistPath = 'racelist'
    if not os.path.exists(racelistPath):
        os.mkdir(racelistPath)
    
    sharecodes = set()
    
    start_date = datetime(2019,9,1) # if running for first time and want all data put datetimes in here
    end_date = datetime(2019,9,8)
    #start_date = datetime.today() - timedelta(days=round(7)) # if updating data for last week or so
    #end_date = datetime.today() - timedelta(days=1)
    
    update_racelist(licence_key, start_date = start_date, end_date = end_date)
    sharecodes = get_racelist_range(licence_key=licence_key, start_date=start_date, end_date=end_date, direc='./racelist', force_overwrite=False)
    racelistContents = set(listdir2(racelistPath))
    
    # for multiprocessing/threading to download faster;
    def save_file(row):
        licence_key, sc = row
        if sc not in gpsContents or os.stat(os.path.join(gpsPath, sc)).st_size < 100: # or len(set([row['P'] for row in read_json(os.path.join(gpsPath, sc))])) < 30: 
            dataGps = getGPSData(licence_key, sc)
            if dataGps:
                with open(os.path.join(gpsPath, sc), 'w') as f:
                    json.dump(dataGps, f)
        if sc not in secContents:
            dataSec = getSectionals(licence_key, sc)
            if dataSec:
                with open(os.path.join(secPath, sc), 'w') as f:
                    json.dump(dataSec, f)
                    
    #import multiprocessing as mp
    from multiprocessing.pool import ThreadPool
    #with mp.Pool() as pool:
    # ThreadPool is a nifty tool which is similar to mp.Pool but uses threads in one process. Not well documented and strange that it's in mp but useful here
    with ThreadPool(4) as pool:
        pool.map(save_file, [(licence_key, sc) for sc in sharecodes])
    # gmax server imposes fair use limits so making 4x more requests per second could make server block the request, 
    # and then would have to hope the time.sleep(1) retry... picks it up and doesn't cause an error after 5 retries.
    # it'll take an hour or so to download the lot using multithreaded process, probably longer for 10Hz. best something to leave downloading overnight
    r"""
    # single process;
    for sc in sharecodes:
        if sc not in gpsContents or os.stat(os.path.join(gpsPath, sc)).st_size < 100 or len(set([row['P'] for row in read_json(os.path.join(gpsPath, sc))])) < 30: # last condition for server error where P field doesn't change through race sometimes, only occaisional now for jumps races
            dataGps = getGPSData(licence_key, sc)
            if dataGps:
                with open(os.path.join(gpsPath, sc), 'w') as f:
                    json.dump(dataGps, f)
        if sc not in secContents:
            dataSec = getSectionals(licence_key, sc)
            if dataSec:
                with open(os.path.join(secPath, sc), 'w') as f:
                    json.dump(dataSec, f)
    """
    #data = load_all_sectionals() # if you want to export everything to an xls file can do so here, subset by daterange start_date and end_date available. might add similar to split S Sl and SF onto different sheets but the context can get lost without the times.
