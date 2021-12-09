#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 6 2020

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

import os
import re
import json
import time
import requests
import dateutil
import concurrent
import concurrent.futures
import numpy as np
import pandas as pd
from copy import deepcopy
from datetime import datetime, date
import bs4
from bs4 import BeautifulSoup

MAX_THREADS = 6

from . import get_logger

logger = get_logger(name = __name__)

HEADERS_ = {
        '1': ['Finish', '1f', '2f', '3f', '4f', '5f', '6f', '7f', '8f', '9f', '10f', '11f', '12f', '13f', '14f', '15f', '16f', '17f', '18f', '19f', '20f', '21f', '22f', '23f', '24f', '25f', '26f', '27f', '28f', '29f', '30f', '31f', '32f', '33f', '34f']
        }

TURF_COURSES = [
    "Ascot",
    "Bangor",
    "Bath",
    "Brighton",
    "Chepstow",
    "Chester",
    "Doncaster",
    "Fakenham",
    "Ffos Las",
    "Fontwell",
    "Hereford",
    "Hexham",
    "Kentucky Downs",
    "Newton Abbot",
    "Plumpton",
    "Ripon",
    "Sedgefield",
    "Uttoxeter",
    "Windsor",
    "Worcester",
    "Yarmouth"
    ]
AW_COURSES = [
    "Wolverhampton",
    ]
DIRT_COURSES = []
SPECIFIC_COURSES = {
    "Southwell 1M7f182y NH Flat": "Turf",
    "Southwell 1M7f153y NH Flat": "Turf",
    "Canterbury 250y": "Dirt",
    "Canterbury 300y": "Dirt",
    "Canterbury 330y": "Dirt",
    "Canterbury 350y": "Dirt",
    "Canterbury 400y": "Dirt",
    "Canterbury 440y": "Dirt",
    "Canterbury 870y": "Dirt",
    "Newcastle 1M6f66y NH Flat": "Turf",
    "Newcastle 2M46y NH Flat": "Turf",
    "Newcastle 2M4f NH Flat": "AW",
    "Newcastle 2M56y NH Flat": "AW",
    "Newcastle 2M98y NH Flat": "Turf"
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

def reduce_racetype(racetype: str) -> str:
    """
    from a Gmax RaceType in the racelist packet, remove specific lanes and
    run-up information. eg,
    
    Del Mar 1M Lane 4 (+30ft) OLD -> Del Mar 1M
    
    useful for grouping races on seldom run distances to get more observations
    for sectional averages.
    
    # TODO, regex cuts out (AW) from 'Newcastle 2M4f NH Flat (AW)', would rather it didn't
    
    Parameters
    ----------
    racetype : str
        Gmax RaceType label, from the racelist feed.

    Returns
    -------
    str
        a more concise racetype, if too much detail given
    """
    racetype = re.sub("( Lane (\d+(?:\.\d+)?))|\((.*?)\)|(Legacy|legacy|OLD|old)",
                      "",
                      racetype)
    return racetype.strip()

def get_race_details(racetype: str, racecourse: str = None) -> dict:
    """
    get estimated race details from the Gmax RaceType string, including surface, 
    obstacle and ROUND or STRAIGHT course.

    Parameters
    ----------
    racetype : str
        Gmax RaceType string.
    racecourse : str
        Gmax Racecourse string.

    Returns
    -------
    dict
    """
    lower_racetype = racetype.lower()
    surface = None
    if "Turf" in racetype or any([x in lower_racetype for x in ["hurdle", "chase"]]):
        surface = "Turf"
    elif any([x in lower_racetype for x in ["allweather", "all weather", "all-weather", "polytrack", "tapeta", "fibresand"]]) or \
         any([x in racetype for x in ["AW", "PT", "FS"]]):
        surface = "AW"
    elif "Dirt" in racetype:
        surface = "Dirt"
    else:
        # use list of courses which are definitely only one type
        racecourse = racecourse or racetype
        if any([x in racecourse for x in TURF_COURSES]):
            surface = "Turf"
        elif any([x in racecourse for x in AW_COURSES]):
            surface = "AW"
        elif any([x in racecourse for x in DIRT_COURSES]):
            surface = "Dirt"
        else:
            surface = SPECIFIC_COURSES.get(racetype)
    
    if "chase" in lower_racetype:
        obstacle = "Fence"
    elif "hurdle" in lower_racetype:
        obstacle = "Hurdle"
    elif "nh flat" in lower_racetype:
        obstacle = "NH Flat"
    else:
        obstacle = "Flat"
    
    if any([x in racetype for x in ["Straight", "Ascot 1M Flat"]]):
        detail = "STRAIGHT"
    elif any([x in racetype for x in ["Round", "Doncaster 7f213y", "Ascot 7f213y Flat"]]):
        detail = "ROUND"
    else:
        detail = None
    
    return {
        "surface": surface,
        "obstacle": obstacle,
        "detail": detail
        }

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
        d = datetime.combine(d, datetime.min.time())
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

def check_file_exists(direc: str, fname: str) -> True or None:
    """
    check if file exists, return True if exists.
    
    used in conjuction with no_return flag, to stop existing files being
    loaded and returned and also to not waste time on file IO.

    returns None if False to work with current logic.

    Parameters
    ----------
    direc : str
        directory to search for file
    fname : str
        file name.

    Returns
    -------
    True or None
    """
    return os.path.exists(os.path.join(direc, fname)) or None

def read_file(path: str, is_json: bool = True) -> dict or list:
    """
    read a json file into python dict/list

    Parameters
    ----------
    path : str
        path to json encoded file.
    is_json : bool
        whether the file is json encoded or not. Default is True

    Returns
    -------
    dict or list
        python json object.
    """
    data = None
    if os.path.exists(path):
        with open(path, 'r') as f:
            if is_json:
                data = json.load(f)
            else:
                data = f.read()
    return data

def load_file(direc: str, fname: str, is_json: bool = True) -> dict or None:
    """
    intermediary function for loading file 'fname' from directory 'direc'.
    fname must be a json encoded file.

    Parameters
    ----------
    direc : str
        directory from which to load file.
    fname : str
        fname to load from file.
    is_json : bool
        whether the file is json encoded or not. Default is True.

    Returns
    -------
    dict or None
    """
    path = os.path.join(direc, fname)
    return read_file(path, is_json = is_json)

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

def process_url_response(url: str,
                         direc: str,
                         fname: str,
                         version: int = 1
                         ) -> dict:
    """
    little helper function to cut down on repeated code.
    
    version 1 given valid json string so stores as is,
    version 2 reformats to make I the index from valid json string,
    version 3 for converting rows of r"\r\n" delimited json string to list of dicts
    version 4 for saving route file as given, XML encoded text

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
    data = {}
    txt = read_url(url)
    if txt:
        if version == 1:
            data = json.loads(txt)
            dump_file(data = data, direc = direc, fname = fname)
        elif version == 2:
            data = {row['I']:row for row in json.loads(txt)}
            dump_file(data = data, direc = direc, fname = fname)
        elif version == 3:
            data = [json.loads(row) for row in txt.splitlines() if len(row) > 5]
            dump_file(data = data, direc = direc, fname = fname)
        elif version == 4:
            if txt not in ["File not available - please contact us.", "Permission Denied", "{}"]:
                data = txt
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
                      **kwargs,
                      ) -> list:
    """
    apply a thread pool to the given func and iterable.

    Parameters
    ----------
    func : function
        function to apply to each element of the given iterable.
    iterable : list or dict or set
        iterable of inputs for the given function.
        
    **params, passed onto func
    new : bool, optional
        whether to fetch a new copy of the data. The default is False.
    offline : bool, optional
        whether to not use internet for this request. The default is False.
    no_return : bool, optional
        whether to ignore returns for the func to save memory for file system
        updates.
        The default is False.

    Returns
    -------
    list
    """
    threads = min([MAX_THREADS, len(iterable)])
    if threads > 1:
        with concurrent.futures.ThreadPoolExecutor(threads) as pool:
            results = [pool.submit(func, x, **kwargs) for x in iterable]
        results = [res.result() for res in results]
    elif iterable:
        results = [func(x, **kwargs) for x in iterable]
    else:
        results = []
    return results

def convert_sectionals_to_1f(sectionals: list) -> list:
    """
    convert a list of sectional records into the 1f interval UK format.
    used for bring the USA format into more consistent format until a fix can
    be applied at source to allow the user to specify the interval.

    note, only 0.5f interval is supported yet # TODO

    Parameters
    ----------
    sectionals : list
        list of gmax sectionals with intervals not equal to 1f.

    Returns
    -------
    list
        new sectionals with interval equal to 1f, and the remainder placed
        at the start of the race.
    """
    new_sects = []
    runners = set([row["I"] for row in sectionals])
    all_gates = list(set([row["G"] for row in sectionals]))
    fur_gates = sorted([g for g in all_gates if int(_gate_num(g)) == _gate_num(g)],
                        key = _gate_num,
                        reverse = True)
    for gate in fur_gates:
        for runner in runners:
            runner_sects = [row for row in sectionals if row["I"] == runner]
            runner_gates = set([row["G"] for row in runner_sects])
            if len(runner_gates) != len(all_gates):
                continue # remove runners where end is cut off (usually tailed off)
            gate_number = _gate_num(gate)
            upper_gate_number = int(gate_number) + 1
            sects = [row for row in runner_sects if
                     gate_number <= _gate_num(row["G"]) < upper_gate_number]
            if sects:
                new_sects.append({
                    "I": sects[0]["I"],
                    "G": min([row["G"] for row in sects], key = _gate_num),
                    "L": min([row["L"] for row in sects]),
                    "S": sum([row["S"] for row in sects]),
                    "R": max([row["R"] for row in sects]),
                    "B": min(sects, key = lambda row: row["L"])["B"],
                    "D": sum([row["D"] for row in sects]),
                    "N": sum([row.get("N") or 0 for row in sects])
                    })
    return new_sects

def add_proportions(sectionals: list, inplace: bool = True) -> list:
    """
    add proportion of time/strides that each runner spends in each section, 
    compared to that runner's final time and number of strides.
    
    sectionals should be validated as complete for each runner, and no 
    repeated sections.
    
    sectionals are actually edited in place, unless specified inplace = False.

    Parameters
    ----------
    sectionals : list
        list of all gmax runner sectionals for some race, not nested, as records.
    inplace : bool
        if true, sectionals are altered inplace, else copy is returned.

    Returns
    -------
    list
        sectionals as given, with proportions fields 'prop_S' and 'prop_N' added.
    """
    runners = {}
    runner_final_times = {}
    runner_final_strides = {}
    if not inplace:
        sectionals = deepcopy(sectionals)
    for row in sectionals:
        if row["I"] not in runners:
            runners[row["I"]] = []
            runner_final_strides[row["I"]] = 0
        runners[row["I"]].append(row)
        runner_final_strides[row["I"]] += row.get("N", 0)
        if row["G"] == "Finish":
            runner_final_times[row["I"]] = row["R"]
        if row.get("D") and row.get("S"):
            row["V"] = row["D"] / row["S"]
            if row.get("N"):
                row["SF"] = row["N"] / row["S"]
                row["SL"] = row["D"] / row["N"]
    for runner, sections in runners.items():
        if runner not in runner_final_times:
            continue
        if 0 < sum(["N" in s for s in sections]) < len(sections):
            continue
        if any([s["D"] == 0 for s in sections]):
            continue
        for section in sections:
            section["prop_S"] = section["S"] / runner_final_times[runner]
            if runner_final_strides[runner]:
                section["prop_N"] = section["N"] / runner_final_strides[runner]
    return sectionals

def validate_sectionals(data: list,
                        handle_dups: bool = True,
                        remove_dups: bool = True,
                        remove_incomplete: bool = True,
                        ) -> list:
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
    remove_dups : bool, optional
        whether to remove horses which have duplicate sections.
        The default is True.
    remove_incomplete : bool, optional
        whether to remove horses which have incomplete sections.
        The default is True.

    Returns
    -------
    list
        list of validated sectional data for some race.
    """
    if data:
        if False: #handle_dups:
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
        if remove_dups:
            runners = {row["I"]: set() for row in data}
            remove_runners = set()
            for row in data:
                if row["G"] not in runners[row["I"]]:
                    runners[row["I"]].add(row["G"])
                else:
                    logger.warning("runner {0} found with duplicate gates, removing from sectionals".format(row["I"]))
                    remove_runners.add(row["I"])
            if remove_runners:
                data = [row for row in data if row["I"] not in remove_runners]
        if remove_incomplete:
            runners = {row["I"]: [] for row in data}
            for row in data:
                runners[row["I"]].append(row)
            expected_records = np.median([len(r) for r in runners.values()])
            for runner, records in runners.items():
                if len(records) != expected_records:
                    logger.warning("runner {0} found with missing gates, removing from sectionals".format(runner))
                    data = [row for row in data if row["I"] != runner]
    return data

def compute_overall_race_metrics(sectionals: list,
                                 race_length: float = None,
                                 ignore_first: bool = True,
                                 func = None
                                 ) -> list:
    """
    use gmax sectionals to compute the overall race metrics, such as overall time,
    overall average stride length and stride frequency, ignoring the opening gate
    if ignore_first.

    Parameters
    ----------
    sectionals : list
        list of Gmax/TPD sectional records.
    race_length : float, optional
        distance of the race.
        The default is None, and is taken as max P remaining from sectionals.
    ignore_first : bool
        whether to ignore the opening section when computing the stride data.
        The default is True.
    func : function
        optional additional custom smoothing/processing function to apply to 
        sectionals. not inplace.

    Returns
    -------
    list
    """
    sectionals = validate_sectionals(data = sectionals)
    add_proportions(sectionals, inplace = True)
    if func is not None:
        sectionals = func(sectionals)
    if not sectionals:
        return []
    metrics = []
    runners = set([row["I"] for row in sectionals])
    max_gate = max([row["G"] for row in sectionals],
                   key = _gate_num)
    min_time = min([row["R"] for row in sectionals if row["G"] == "Finish"])
    for runner in runners:
        data = [row for row in sectionals if row["I"] == runner]
        if ignore_first:
            distance_ran = sum([row.get("D", 0) for row in data if row["G"] != max_gate])
            number_strides = sum([row.get("N", 0) for row in data if row["G"] != max_gate])
            time = sum([row.get("S", 0) for row in data if row["G"] != max_gate])
        else:
            distance_ran = sum([row.get("D", 0) for row in data])
            number_strides = sum([row.get("N", 0) for row in data])
            time = sum([row.get("S", 0) for row in data])
        finish_time = sum([row.get("R", 0) for row in data if row["G"] == "Finish"]) or None
        final_2f_time = sum([row.get("S", 0) for row in data if _gate_num(row["G"]) <= 2.])
        final_2f_distance = sum([row.get("D", 0) for row in data if _gate_num(row["G"]) <= 2.])
        finish_speed_perc = (final_2f_distance / final_2f_time) / (distance_ran / time)
        metrics.append({
            "runner_sharecode": runner,
            "distance_ran": distance_ran,
            "number_strides": number_strides if number_strides > 0 else None,
            "time": time if time else None,
            "stride_length": distance_ran / number_strides if number_strides > 0 else None,
            "stride_frequency": number_strides / time if number_strides > 0 and time > 0 else None,
            "finish_time": finish_time,
            "final_2f_distance": final_2f_distance,
            "final_2f_time": final_2f_time,
            "finish_speed_percentage": finish_speed_perc,
            "time_behind": finish_time - min_time if finish_time else 1000.
            })
    return metrics

def list_broken_progress_field(sharecodes: list, gmax_feed) -> list:
    """
    list sharecodes where the points have P field which does decrease through race

    Parameters
    ----------
    sharecodes : list
        list of sharecodes to check
    gmax_feed : TYPE
        GmaxFeed instance

    Returns
    -------
    list
        list of broekn sharecodes
    """
    broken = []
    for sc in sharecodes:
        points = gmax_feed.get_points(sc, offline = True).get('data')
        if points:
            if len(set([row["P"] for row in points])) < 20:
                broken.append(sc)
    return broken

def _compute_derivatives(data: dict, race_length: float) -> dict:
    """
    compute some overview metrics for the given sectionals for some runner

    Parameters
    ----------
    data : dict
        sectionals data for a runner, in dict form mapping gate to data.
    race_length : float
        total race length in meters.

    Returns
    -------
    dict
    """
    average_sl = np.sum([gate['D'] for gate in data.values()]) / np.sum([gate['N'] for gate in data.values() if 'N' in gate and gate['D'] > 0])
    average_sf = np.sum([gate['N'] for gate in data.values() if 'N' in gate]) / data['Finish']['R']
    fin_speed = np.sum([gate['D'] for gate in data.values() if _gate_num(gate['G']) < 1.75]) / np.sum([gate['S'] for gate in data.values() if _gate_num(gate['G']) < 1.75])
    av_speed = race_length / data['Finish']['R'] # some issues with this, can't use actual data['D'] because of opening distance occasionally being 0, and race-length often underestimates the distance like at Fontwell.
    fin_perc = 100 * fin_speed / av_speed
    sections = {gate['G']:gate for gate in data.values()}
    return {
        'finish_time': data['Finish']['R'],
        'average_sl': average_sl,
        'average_sf': average_sf,
        'finish_perc': fin_perc,
        'sections': sections
        }

def export_sectionals_to_csv(sectionals: dict or list,
                             fname: str = None,
                             compression: str = None
                             ) -> None:
    """
    export the given dictionary/list of sharecodes to csv format
    
    Parameters
    ----------
    sectionals : dict
        dictionary or list of all sectionals to export as given by gmax API.
    fname : str
        name of the file to save under
    compression: str
        as per options for df.to_csv(compression = compression)
    """
    if type(sectionals) is dict:
        sectionals = [row for row in sectionals.values()]
    df = pd.DataFrame.from_records(sectionals)
    df.to_csv(fname or 'tpd_sectionals.csv', compression = compression)

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
        "course_name": str,  # name of course file
        "track_type": str,  # name of track type
        "coordinates": {
            "#JUMP": [],  # coords of obstacles
            "#RUNNING_LINE": []  # coords of running line
            "#WINNING_LINE": [{  # coords of finish line
                "line_string_id": int,
                "placemark_name": str,
                "style_name": str,  # one of "#JUMP" "#WINNING_LINE" or "#RUNNING_LINE"
                "coordinates": [{
                    "course_coordinates_id": int,
                    "X": double,  # lon
                    "Y": double,  # lat
                    "Z": double,  # elevation (meters above sea level)
                    },
                    {...},
                    ],
                }],
            }
        }
    """
    if not x:
        return None
    coords_id = 1
    
    def _handle_linestrings(placemark: bs4.element.Tag) -> list:
        """
        handle a placemark tag of LineString tags as bs4 form, convert to json format

        Parameters
        ----------
        placemark : Tag

        Returns
        -------
        list of coord trios from LineString tag
            LineString converted to JSON format, such as format,
        """
        nonlocal coords_id
        coords_output = []
        # each coordinate line is stored in a LineString tag
        line_strings = placemark.find_all("LineString")
        style_name = placemark.find("styleUrl").text
        if "#" not in style_name:
            style_name = "#" + style_name
        placemark_name = placemark.find("name").text
        for idx, line_string in enumerate(line_strings, start = 1):
            line_string_dict = {
                "line_string_id": idx,  # line-string level unique ID
                "placemark_name": placemark_name,
                "style_name": style_name,  # will be useful for filtering coordinate types when have multiple running lines (lanes in USA/CA)
                "coordinates": []
                }
            if line_string.coordinates is not None:
                coords = line_string.coordinates.text.strip().split()
                # coords exist as 3d trio including elevation (which is usually 0 or unusably inaccurate)
                for coord_trio in coords:
                    X, Y, Z = coord_trio.split(",")
                    line_string_dict["coordinates"].append({
                        "X": float(X),  # longitude
                        "Y": float(Y),  # latitude
                        "Z": float(Z),  # elevation
                        "course_coordinates_id": coords_id  # course wide unique ID
                        })
                    coords_id += 1
            coords_output.append(line_string_dict)
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
            "coordinates": {
                "#RUNNING_LINE": [],
                "#WINNING_LINE": [],
                "#JUMP": []
                }
            }
        # WINNING_LINE and RUNNING_LINE are stored in separate Placemark tags
        placemarks = folder.find_all("Placemark")
        for placemark in placemarks:
            style_name = placemark.find("styleUrl").text
            if "#" not in style_name:
                style_name = "#" + style_name
            track_type_output["coordinates"][style_name].append(_handle_linestrings(placemark = placemark))
        output.append(track_type_output)
    return output

def last_tracker_use(data: list, fname: str = None) -> pd.DataFrame:
    """
    get date of latest usage of each tracker from performance feed

    Parameters
    ----------
    data : list
        list of records from performance feed.
    fname : str, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    pd.DataFrame
    """
    trackers = {}
    for row in data:
        date = row["I"][2:14]
        t1 = row.get("ID1")
        t2 = row.get("ID2")
        for t in [t1, t2]:
            if t and "Unknown" not in t:
                if t not in trackers:
                    trackers[t] = {"date": date, "code": t[-3:], "sharecode": row["I"]}
                else:
                    if date > trackers[t]["date"]:
                       trackers[t] = {"date": date, "code": t[-3:], "sharecode": row["I"]}
    for t in trackers:
        trackers[t]["date"] = datetime.strptime(trackers[t]["date"], "%Y%m%d%H%M")
    return pd.DataFrame.from_dict(trackers, orient = 'index')
    #df.to_excel(fname or 'latest_tracker_uses.xlsx')

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

