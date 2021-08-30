#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 6 2020

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

import os
import json
import time
import requests
import dateutil
import concurrent
import concurrent.futures
import numpy as np
import pandas as pd
from datetime import datetime, date
from bs4 import BeautifulSoup

MAX_THREADS = 4

from . import get_logger

logger = get_logger(name = __name__)

HEADERS_ = {
        '1': ['Finish', '1f', '2f', '3f', '4f', '5f', '6f', '7f', '8f', '9f', '10f', '11f', '12f', '13f', '14f', '15f', '16f', '17f', '18f', '19f', '20f', '21f', '22f', '23f', '24f', '25f', '26f', '27f', '28f', '29f', '30f', '31f', '32f', '33f', '34f']
        }

def listdir2(fol: str) -> list:
    """
    listdir, but ignore all hidden files, .DS_Store, ., ..

    Parameters
    ----------
    fol : str
        directory to list.

    Returns
    -------
    list
        list of file names.
    """
    return [f for f in os.listdir(fol) if not f.startswith('.')]

def _gate_num(x: str) -> float:
    """
    convert Gmax gate label to a float in furlongs from finish, and Finish -> 0.

    Parameters
    ----------
    x : str
        Gmax gate label.

    Returns
    -------
    float
        furlongs from finish.
    """
    return float(x.replace('f','').replace('Finish','0').replace('F',''))

def to_datetime(d: datetime or int or float or str = None, tz = None):
    """
    check the format of the given datetime and return as naive UTC datetime 

    Parameters
    ----------
    d : datetime or int or float or str, optional
        datetime object or timestamp (seconds, or millis).
        The default is None.
    tz : tzinfo, optional
        timezone to set for output. The default is None.

    Returns
    -------
    d : datetime
        UTC, tz-naive.
    """
    if type(d) is str:
        d = dateutil.parser.parse(d)
    elif type(d) is date:
        d = datetime.combine(date, datetime.min.time())
    elif type(d) in [float, int]:
        if d > 2051222400: # probably given in milliseconds
            d = d / 1000
        else: # probably given in seconds, do nothing
            pass
        d = datetime.utcfromtimestamp(d)
    if type(d) is datetime:
        if tz is not None:
            d = d.astimezone(tz)
        else:
            d = d.astimezone(dateutil.tz.UTC).replace(tzinfo = None)
        return d

def read_json(path: str) -> dict or list:
    """
    read a json file into python dict/list

    Parameters
    ----------
    path : str
        path to json encoded file.

    Returns
    -------
    dict or list
        python json object.
    """
    data = None
    if os.path.exists(path):
        with open(path, 'r') as file:
            data = json.load(file)
    return data

def reformat_sectionals_list(data: list) -> dict:
    """
    reformat list of dictionaries into dictionary of runners, like
    { 
     runner1: {gate: {...}, gate2: {...}, ...},
     runner2: ...
    }

    Parameters
    ----------
    data : list
        list of gmax sectionals.

    Returns
    -------
    dict
        dict, formatted by runnerid -> gate -> data.
    """
    runners = sorted(list(set([row['I'] for row in data])))
    d = {runner:{} for runner in runners}
    for runner in runners:
        d[runner] = {row['G']:row for row in data if row['I']==runner}
    return d

def reformat_gps_list(data: list, by: str = 'T') -> dict:
    """
    reformat list of dictionaries into dictionary like,
    {
     runner1: {timestamp1: {data}, timestamp2: {data}, ...}, ...},
    ...
    }
    or
    {
     timestamp1: {runner1: {data}, runner2: {data}, ...},
     timestamp2: {...},
     ...
    }
    
    by either 'T' or 'I'

    Parameters
    ----------
    data : list
        DESCRIPTION.
    by : 'T' or 'I', optional
        key to format list by. The default is 'T'.

    Returns
    -------
    dict
        reformatted dict.
    """
    a = 'I' if by == 'T' else 'T'
    heads = sorted(list(set([row[by] for row in data])))
    d = {key:{} for key in heads}
    for key in heads:
        d[key] = {row[a]:row for row in data if row[by]==key}
    return d

def load_file(direc: str, fname: str) -> dict or None:
    """
    intermediary function for loading file 'fname' from directory 'direc'.
    fname must be a json encoded file.

    Parameters
    ----------
    direc : str
        directory from which to load file.
    fname : str
        fname to load from file.

    Returns
    -------
    dict or None
    """
    path = os.path.join(direc, fname)
    return read_json(path)

def dump_file(data: dict or str or bytes,
              direc: str, 
              fname: str
              ) -> None:
    """
    dump json encoded data or raw string into os.path.join(direc, fname).

    Parameters
    ----------
    data : dict or str or bytes
        json data or string to dump to file.
    direc : str
        directory to use.
    fname : str
        fname to use within given directory.
    """
    path = os.path.join(direc, fname)
    with open(path, 'w') as f:
        if type(data) in [list, dict]:
            json.dump(data, f)
        else:
            if type(data) is bytes:
                data = data.decode("ascii")
            f.write(data)

def process_url_response(url: str,
                         direc: str,
                         fname: str,
                         version: int = 1
                         ) -> dict:
    """
    little helper function to cut down on repeated code.
    
    version 1 stores as is,
    version 2 reformats to make I the index,
    version 3 for converting rows of json string to list of json strings

    Parameters
    ----------
    url : str
        full URL to get.
    direc : str
        directory into which to store file.
    fname : str
        filename under which to store file.
    version : int, optional
        type of data processing to format the string. The default is 1.

    Returns
    -------
    dict
    """
    txt = read_url(url)
    if txt:
        if version == 1:
            data = json.loads(txt)
        elif version == 2:
            data = {row['I']:row for row in json.loads(txt)}
        elif version == 3:
            data = [json.loads(row) for row in txt.splitlines() if len(row) > 5]
        else:
            data = txt
    else:
        data = {}
    dump_file(data = data, direc = direc, fname = fname)
    return data

def read_url(url: str = False, try_limit: int = 3) -> str or False:
    """
    simple read url with GET request.

    Parameters
    ----------
    url : str, optional
        URL to GET. The default is False.
    try_limit : int, optional
        number of attempts to make before giving up. The default is 3.

    Returns
    -------
    str or False
    """
    if not url:
        return False
    txt = False
    idx = 0
    while idx < try_limit:
        try:
            with requests.Session() as s:
                response = s.get(url, timeout = 8)
                txt = response.text
                if txt == "Permission Denied":
                    txt = False
            break
        except Exception:
            logger.exception('url error - {0}'.format(url))
            time.sleep(1)
            idx += 1
    return txt

def apply_thread_pool(func,
                      iterable,
                      new: bool = False,
                      offline: bool = False
                      ) -> list:
    """
    apply a thread pool to the given func and iterable.

    Parameters
    ----------
    func : function
        function to apply to each element of the given iterable.
    iterable : list or dict or set
        iterable of inputs for the given function.
    new : bool, optional
        whether to fetch a new copy of the data. The default is False.
    offline : bool, optional
        whether to not use internet for this request. The default is False.

    Returns
    -------
    list
    """
    threads = min([MAX_THREADS, len(iterable)])
    if threads > 1:
        with concurrent.futures.ThreadPoolExecutor(threads) as pool:
            results = [pool.submit(func, x, new, offline) for x in iterable]
        results = [res.result() for res in results]
    elif iterable:
        results = [func(x, new, offline) for x in iterable]
    else:
        results = []
    return results

def validate_sectionals(data: list, handle_dups: bool = True) -> list:
    """
    check the sectionals for duplicates and missing sections.
    if a duplicate is found, usually if a horse's gate is split in two, and
    handle_dups if True then the if the two records are obviously from a split
    section they're added together.

    Parameters
    ----------
    data : list
        list of sectional data for some race
    handle_dups : bool, optional
        whether to add duplicated sections together. The default is True.

    Returns
    -------
    list
        list of validated sectional data for some race.
    """
    if data:
        unique_tuples = set([(row["I"], row["G"]) for row in data])
        if len(unique_tuples) != len(data):
            logger.warning("Duplicate runner section warning, checking to see which runner and gate...")
            runners = set([row['I'] for row in data])
            new_data = {}
            for runner in runners:
                for row in data:
                    if row["I"] == runner:
                        key = (row["I"], row["G"])
                        if key in new_data:
                            logger.warning("duplicate gate found: runner: {0} gate: {1}. Attempting to fix by addition...".format(row["I"], row["G"]))
                            if False: #new_data[key]["S"] < 2. or row["S"] < 2.:
                                logger.warning("fixing by addition: {0} + {1}".format(new_data[key], row))
                                for k, v in row.items():
                                    if k in {'S', 'R', 'D', 'N'}:
                                        new_data[key][k] += v
                        else:
                            new_data[key] = row
            data = [row for row in new_data.values()]
    return data

def _compute_derivatives(data: dict, race_length: float) -> dict:
    """
    compute some overview metrics for the given sectionals for some runner

    Parameters
    ----------
    data : dict
        DESCRIPTION.
    race_length : float
        DESCRIPTION.

    Returns
    -------
    dict
        DESCRIPTION.
    """
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

def export_sectionals_to_xls(sharecodes: dict) -> None:
    """
    make pandas dataframe of TPD sectionals and then write it to an xls file.
    
    Parameters
    ----------
    sharecodes : dict
        dictionary merge of racelist and sectionals. like:
            sc1: {"PostTime": ...,
                  "RaceType": ...,
                  ...,
                  "sectionals": {
                      rnum1: {
                          gate1: {...},
                          gate2: {...},
                          ...
                          },
                      rnum2: {
                          ...
                          },
                      }
                  }
    """
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

def route_xml_to_json(x: str or bytes) -> list:
    """
    convert the Gmax racecourse survey KML file into a nested json dict.

    Parameters
    ----------
    x : str or bytes
        KML encoded text, bytes, file pointer, or filepath.

    Returns
    -------
    list of nested dictionaries,
        a single nested dictionary is of format,
        {
        
        }
    """
    coords_id = 1
    
    def _handle_linestrings(line_strings: list) -> list:
        """
        handle a list of LineString tags as bs4 form, convert to json format

        Parameters
        ----------
        line_strings : list
            list of LineString tags

        Returns
        -------
        list of coord trios from LineString tag
            LineString converted to JSON format, such as format,
        """
        nonlocal coords_id
        coords_output = []
        for idx, line_string in enumerate(line_strings, start = 1):
            if line_string.coordinates is not None:
                coords = line_string.coordinates.text.strip().split(" ")
                # coords exist as 3d trio including elevation (which is usually 0 or unusably inaccurate)
                for coord_trio in coords:
                    X, Y, Z = coord_trio.split(",")
                    coords_output.append({
                        "X": float(X),  # longitude
                        "Y": float(Y),  # latitude
                        "Z": float(Z),  # elevation
                        "course_coordinates_id": coords_id,  # course-level unique ID
                        "line_string_id": idx  # line-string level unique ID
                        })
                    coords_id += 1
        return coords_output
    
    if type(x) in [str, bytes]:
        if len(x) > 256: # probably given the whole file contents
            txt = x
        else: # probably given a filepath
            with open(x, 'r') as f:
                txt = f.read()
    else: # probably given a file points for some reason
        txt = x.read()
    soup = BeautifulSoup(txt, features = "xml")
    output = []
    course_name = soup.find("name").text
    # each track type is stored under a different "Folder" tag
    folders = soup.find_all("Folder")
    for folder in folders:
        track_type_name = folder.find("name").text
        track_type_output = {
            "course_name": course_name,
            "track_type": track_type_name,
            "winning_line": [],
            "running_line": [],
            "jumps": []
            }
        # WINNING_LINE and RUNNING_LINE are stored in separate Placemark tags
        placemarks = folder.find_all("Placemark")
        for placemark in placemarks:
            placemark_name = placemark.find("name").text
            # each coordinate line is stored in a LineString tag
            line_strings = placemark.find_all("LineString")
            coord_trios = _handle_linestrings(line_strings = line_strings)
            if placemark_name == "WINNING_LINE":
                track_type_output["winning_line"].extend(coord_trios)
            elif placemark_name == "RUNNING_LINE":
                track_type_output["running_line"].extend(coord_trios)
            elif "JUMP" in placemark_name:
                track_type_output["jumps"].extend(coord_trios)
            else:
                logger.warning("unexpected placemark name in course: {0}".format(course_name))
        output.append(track_type_output)
    return output

def haversine(x1: np.ndarray,
              x2: np.ndarray,
              y1: np.ndarray,
              y2: np.ndarray
              ) -> np.ndarray:
    """
    input in degrees, arrays or numbers.
    
    compute haversine distance between coords (x1, y1) and (x2, y2)

    Parameters
    ----------
    x1 : np.ndarray
        X/longitude in degrees for coords pair 1
    x2 : np.ndarray
        Y/latitude in degrees for coords pair 1.
    y1 : np.ndarray
        X/longitude in degrees for coords pair 2.
    y2 : np.ndarray
        Y/latitude in degrees for coords pair 2.

    Returns
    -------
    np.ndarray or float
        haversine distance (meters) between the two given points. 
    """
    x1 = np.deg2rad(x1)
    x2 = np.deg2rad(x2)
    y1 = np.deg2rad(y1)
    y2 = np.deg2rad(y2)
    return 12730000*np.arcsin(((np.sin((y2-y1)*0.5)**2) + np.cos(y1)*np.cos(y2)*np.sin((x2-x1)*0.5)**2)**0.5)

def compute_bearing(coords1: (float, float),
                    coords2: (float, float)
                    ) -> float: 
    """
    compute the bearing between the two given coordinate pairs.

    Parameters
    ----------
    coords1 : (float, float)
        (longutiude, latitude) of start point.
    coords2 : (float, float)
        (longutiude, latitude) of finish point.

    Returns
    -------
    float
        bearing, clockwise angle in radians from North and direction of travel.
    """
    lon1, lat1 = np.deg2rad(coords1)
    lon2, lat2 = np.deg2rad(coords2)
    return np.arctan2(np.sin(lon2-lon1)*np.cos(lat2) , np.cos(lat1)*np.sin(lat2)-np.sin(lat1)*np.cos(lat2)*np.cos(lon2-lon1))

def compute_bearing_difference(b1: np.ndarray,
                               b2: np.ndarray
                               ) -> np.ndarray:
    """
    compute the geometric difference (radians) between two bearings, 
    or arrays thereof.

    Parameters
    ----------
    b1 : np.ndarray
        bearing 1.
    b2 : np.ndarray
        bearing 2.

    Returns
    -------
    np.ndarray
    """
    return ((b2 - b1 + 0.5*np.pi) % np.pi) - 0.5*np.pi

def compute_new_coords(X1: np.ndarray,
                       Y1: np.ndarray,
                       D: np.ndarray,
                       B: np.ndarray
                       ) -> (np.ndarray, np.ndarray):
    """
    compute new coordinates for a particle that moves a great circle distance D
    on the bearing B starting from coordinates (X1, Y1), degrees.
    
    Parameters
    ----------
    X1 : np.ndarray,
        initial X/longitude in degrees
    Y1 : np.ndarray,
        initial Y/latitiude in degrees
    D : np.ndarray,
        great circle distance (meters)
    B : np.ndarray
        bearing (radians)

    Returns
    -------
    (np.ndarray, np.ndarray) or (float, float)
        new particle coordinates in degrees
    """
    X1 = np.deg2rad(X1)
    Y1 = np.deg2rad(Y1)
    d = D/6378100.
    Y2 = np.arcsin(np.sin(Y1)*np.cos(d) + np.cos(Y1)*np.sin(d)*np.cos(B))
    X2 = X1 + np.arctan2(np.sin(B)*np.sin(d)*np.cos(Y1), np.cos(d)-np.sin(Y1)*np.sin(Y2))
    return np.rad2deg(X2), np.rad2deg(Y2)

def compute_mean_bearing(bearings: np.ndarray) -> np.float:
    """
    compute mean bearing for the given bearings, given in radians.
    
    https://stackoverflow.com/questions/5189241/how-to-find-the-average-of-a-set-of-bearings

    Parameters
    ----------
    bearings : np.ndarray

    Returns
    -------
    np.float
    """
    x = np.nanmean(np.cos(bearings)) or 0.00000001
    y = np.nanmean(np.sin(bearings))
    return np.arctan2(y, x)

def compute_back_bearing(bearings: np.ndarray) -> np.ndarray:
    """
    reverse a bearing or array thereof, radians

    Parameters
    ----------
    bearings : np.ndarray

    Returns
    -------
    np.ndarray
    """
    bool_ = bearings <= 0.
    return bool_*(bearings + np.pi) + (1-bool_)*(bearings - np.pi)

