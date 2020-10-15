# -*- coding: utf-8 -*-
"""
Created on Fri Oct 6 2020

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

import os, dateutil, requests, time, json, concurrent
import numpy as np
import pandas as pd
from loguru import logger

MAX_THREADS = 6

logger.add('utils.log', level='INFO', format="{time} {level} {message}")

HEADERS_ = {
        '1': ['Finish', '1f', '2f', '3f', '4f', '5f', '6f', '7f', '8f', '9f', '10f', '11f', '12f', '13f', '14f', '15f', '16f', '17f', '18f', '19f', '20f', '21f', '22f', '23f', '24f', '25f', '26f', '27f', '28f', '29f', '30f', '31f', '32f', '33f', '34f']
        }

def listdir2(fol:str) -> list:
    return [f for f in os.listdir(fol) if not f.startswith('.')]

def _gate_num(x:str) -> float:
    return float(x.replace('f','').replace('Finish','0').replace('F',''))

def read_json(path:str) -> list:
    data = None
    if os.path.exists(path):
        with open(path, 'r') as file:
            data = json.load(file)
    return data

def reformat_sectionals_list(data:list) -> dict:
    # reformat list of dictionaries into dictionary of runners:{gate:{data}, date2:{data},...}
    runners = sorted(list(set([row['I'] for row in data])))
    d = {runner:{} for runner in runners}
    for runner in runners:
        d[runner] = {row['G']:row for row in data if row['I']==runner}
    return d

def reformat_gps_list(data:list, by:str='T') -> dict:
    # by either 'T' or 'I'
    # reformat list of dictionaries into dictionary of {runner1:{timestamp1:{data}, timestamp2:{data}, ...}, ...} or {timestamp1:{runner1:{data}, runner2:{data}, ...}, timestamp2:{...}, ...}
    a = 'I' if by == 'T' else 'T'
    heads = sorted(list(set([row[by] for row in data])))
    d = {key:{} for key in heads}
    for key in heads:
        d[key] = {row[a]:row for row in data if row[by]==key}
    return d

def load_file(direc:str, fname:str) -> dict or None:
    path = os.path.join(direc, fname)
    if os.path.exists(path):
        data = read_json(path)
    else:
        data = None
    return data

def dump_file(data:dict, direc:str, fname:str) -> None:
    path = os.path.join(direc, fname)
    with open(path, 'w') as f:
        json.dump(data, f)

def process_url_response(url:str, direc:str, fname:str, version:int = 1) -> dict:
    # little helper to cut down on repeated code. version 1 stores as is, version 2 reformats to make I the index, version 3 for converting invalid json format to valid json list of dicts for storage.
    txt = read_url(url)
    if txt:
        if version == 1:
            data = json.loads(txt)
        elif version == 2:
            data = {row['I']:row for row in json.loads(txt)}
        elif version == 3:
            data = [json.loads(row) for row in txt if len(row) > 5]
        else:
            data = txt
    else:
        data = {}
    dump_file(data = data, direc = direc, fname = fname)
    return data

def read_url(url:str = False, try_limit:int = 3) -> str or False:
    if not url:
        return False
    txt = False
    idx = 0
    while idx != try_limit:
        try:
            with requests.get(url, timeout=5) as response:
                txt = response.text
                if txt == "Permission Denied":
                    txt = False
            break
        except Exception:
            logger.exception('url error - {0}'.format(url))
            time.sleep(2)
            idx += 1
    return txt

def apply_thread_pool(func, iterable, new=False) -> list:
    threads = min([MAX_THREADS, len(iterable)])
    if threads > 1:
        with concurrent.futures.ThreadPoolExecutor(threads) as pool:
            results = [pool.submit(func, x, new) for x in iterable]
        results = [res.result() for res in results]
    elif iterable:
        results = [func(x, new) for x in iterable]
    else:
        results = []
    return results

def _compute_derivatives(data:dict, race_length:float) -> dict:
    average_sl = np.sum([gate['D'] for gate in data.values()]) / np.sum([gate['N'] for gate in data.values() if 'N' in gate and gate['D'] > 0])
    average_sf = np.sum([gate['N'] for gate in data.values() if 'N' in gate]) / data['Finish']['R']
    fin_speed = np.sum([gate['D'] for gate in data.values() if _gate_num(gate['G']) < 1.75]) / np.sum([gate['S'] for gate in data.values() if _gate_num(gate['G']) < 1.75])
    av_speed = race_length / data['Finish']['R'] # some issues with this, can't use actual data['D'] because of opening distance occasionally being 0, and race-length often underestimates the distance like at Fontwell.
    fin_perc = 100 * fin_speed / av_speed
    sections = {gate['G']:gate for gate in data.values()}
    return {
              'finish_time':data['Finish']['R'],
              'average_sl':average_sl,
              'average_sf':average_sf,
              'finish_perc':fin_perc,
              'sections':sections
           }

def export_sectionals_to_xls(sharecodes:dict) -> None:
    # make pandas dataframe and then write it to xls file
    data = {}
    for sc in sorted(list(sharecodes.keys()), key = lambda x: dateutil.parser.parse(sharecodes[x]['PostTime']), reverse=True):
        if 'sectionals' in sharecodes[sc]:
            for rnum in sharecodes[sc]['sectionals']:
                derivs = _compute_derivatives(sharecodes[sc]['sectionals'][rnum], race_length = sharecodes[sc]['RaceLength'])
                data[rnum + '_S'] = {
                        'Date':dateutil.parser.parse(sharecodes[sc]['PostTime']).strftime('%Y-%m-%d %H:%M:%S'),
                        'Sharecode':rnum,
                        'Metric':'Time',
                        'RaceType':sharecodes[sc]['RaceType'],
                        'RaceLength':sharecodes[sc]['RaceLength'],
                        'Finish Speed Percentage':np.round(derivs['finish_perc'], 2),
                        'Overall':derivs['finish_time'],
                        }
                data[rnum + '_SL'] = {
                        'Date':dateutil.parser.parse(sharecodes[sc]['PostTime']).strftime('%Y-%m-%d %H:%M:%S'),
                        'Sharecode':rnum,
                        'Metric':'Stride Length',
                        'RaceType':sharecodes[sc]['RaceType'],
                        'RaceLength':sharecodes[sc]['RaceLength'],
                        'Finish Speed Percentage':None,
                        'Overall':np.round(derivs['average_sl'], 2),
                        }
                data[rnum + '_SF'] = {
                        'Date':dateutil.parser.parse(sharecodes[sc]['PostTime']).strftime('%Y-%m-%d %H:%M:%S'),
                        'Sharecode':rnum,
                        'Metric':'Stride Frequency',
                        'RaceType':sharecodes[sc]['RaceType'],
                        'RaceLength':sharecodes[sc]['RaceLength'],
                        'Finish Speed Percentage':None,
                        'Overall':np.round(derivs['average_sf'], 2),
                        }
                for h in HEADERS_['1']:
                    if h in sharecodes[sc]['sectionals'][rnum]:
                        data[rnum + '_S'][h] = sharecodes[sc]['sectionals'][rnum][h]['S']
                    else:
                        data[rnum + '_S'][h] = None
                        data[rnum + '_SL'][h] = None
                        data[rnum + '_SF'][h] = None
                        continue
                    if 'N' in sharecodes[sc]['sectionals'][rnum][h] and sharecodes[sc]['sectionals'][rnum][h]['S'] > 1:
                        data[rnum + '_SL'][h] = np.round(sharecodes[sc]['sectionals'][rnum][h]['D'] / sharecodes[sc]['sectionals'][rnum][h]['N'], 2)
                        data[rnum + '_SF'][h] = np.round(sharecodes[sc]['sectionals'][rnum][h]['N'] / sharecodes[sc]['sectionals'][rnum][h]['S'], 2)
                    else:
                        data[rnum + '_SL'][h] = None
                        data[rnum + '_SF'][h] = None
                    
    df = pd.DataFrame.from_dict(data, 'index')
    df.to_excel('tpd_sectionals.xlsx')
    return data

def haversine(x1:np.ndarray, x2:np.ndarray, y1:np.ndarray, y2:np.ndarray) -> np.ndarray:
    # input in degrees, arrays or numbers. Compute haversine distance between coords (x1, y1) and (x2, y2)
    x1 = np.deg2rad(x1)
    x2 = np.deg2rad(x2)
    y1 = np.deg2rad(y1)
    y2 = np.deg2rad(y2)
    return 12730000*np.arcsin(((np.sin((y2-y1)*0.5)**2) + np.cos(y1)*np.cos(y2)*np.sin((x2-x1)*0.5)**2)**0.5)

# bearing returned is clockwise angle between north and direction of travel
def compute_bearing(coords1:(float, float), coords2:(float, float)) -> float: 
    # return angle in radians to Meridean line for line between given coords in degrees
    lon1, lat1 = np.deg2rad(coords1)
    lon2, lat2 = np.deg2rad(coords2)
    return np.arctan2(np.sin(lon2-lon1)*np.cos(lat2) , np.cos(lat1)*np.sin(lat2)-np.sin(lat1)*np.cos(lat2)*np.cos(lon2-lon1))

def compute_bearing_difference(b1:np.ndarray, b2:np.ndarray) -> np.ndarray:
    return ((b2 - b1 + 0.5*np.pi) % np.pi) - 0.5*np.pi

def compute_new_coords(X1:np.ndarray, Y1:np.ndarray, D:np.ndarray, B:np.ndarray) -> (np.ndarray, np.ndarray):
    # input in degrees, output in degrees, X=longitude, Y=latitude, input bearing in radians clockwise from North (as output from compute_bearing)
    X1 = np.deg2rad(X1)
    Y1 = np.deg2rad(Y1)
    d = D/6378100.
    Y2 = np.arcsin(np.sin(Y1)*np.cos(d) + np.cos(Y1)*np.sin(d)*np.cos(B))
    X2 = X1 + np.arctan2(np.sin(B)*np.sin(d)*np.cos(Y1), np.cos(d)-np.sin(Y1)*np.sin(Y2))
    return np.rad2deg(X2), np.rad2deg(Y2)

def compute_mean_bearing(bearings:np.ndarray) -> np.float:
    # https://stackoverflow.com/questions/5189241/how-to-find-the-average-of-a-set-of-bearings
    x = np.nanmean(np.cos(bearings)) or 0.00000001
    y = np.nanmean(np.sin(bearings))
    return np.arctan2(y, x)

def compute_back_bearing(bearings:np.ndarray) -> np.ndarray:
    # return back bearing, input in radians
    bool_ = bearings <= 0.
    return bool_*(bearings + np.pi) + (1-bool_)*(bearings - np.pi)

