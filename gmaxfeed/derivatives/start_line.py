#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 23 15:45:23 2023

functions to create, save, and load estimated start line locations, estimated
from the post race GPS points data by finding the timestamp at which the
sum of the squared velocities of all the runners is minimum.

over many races, these coordinates can be overlayed on top of each other
and a robust linear regression algorithm can quite accurately fit the
exact location of the starting stalls.

note that this only works for flat races where a starting stall is used.
for jumps racing, an approach would have to simply find the timestamp
and coordinates at which the P field begins to decrease for each runner.

@author: George Swindells
"""

import os
import json
import dateutil
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.linear_model import RANSACRegressor

from ..feeds.utils import (
    compute_bearing,
    compute_mean_bearing,
    compute_new_coords,
    reduce_racetype,
    put_datetime
    )
from ..feeds.postrace_feeds import GmaxFeed
from .. import get_logger
logger = get_logger(name = __name__)


os.environ["STARTLINE_COORDS_DIRECTORY"] = (
    os.environ.get("STARTLINE_COORDS_DIRECTORY")
    or os.path.join(os.environ["LOGS_DIR"], "startline_coords")
    )

if not os.path.exists(os.environ["STARTLINE_COORDS_DIRECTORY"]):
    os.mkdir(os.environ["STARTLINE_COORDS_DIRECTORY"])


def save_start_line(data: dict,
                    race_type: str,
                    directory: str = os.environ.get("STARTLINE_COORDS_DIRECTORY")
                    ) -> None:
    """
    save a startline dictionary into the filesystem under the race type label
    which should be reduced.

    Parameters
    ----------
    data : dict
        dict of the startline coords
    race_type : str
        gmax race type label, reduced using reduce_racetype (checked).
    directory : str, optional
        directory into which to save the data.
        The default is os.environ.get("STARTLINE_COORDS_DIRECTORY").
    """
    race_type = reduce_racetype(race_type).lower().replace("/", "_")
    path = os.path.join(directory, race_type + ".json")
    with open(path, "w") as f:
        json.dump(data, f)


def load_start_line(race_type: str,
                    directory: str = os.environ.get("STARTLINE_COORDS_DIRECTORY")
                    ) -> dict or None:
    """
    load start line from the local start line coords directory

    Parameters
    ----------
    race_type : str
        gmax RaceType to load from the start line coords directory
    directory : str, optional
        directory from which to load the start line coords.
        The default is os.environ.get("STARTLINE_COORDS_DIRECTORY")
    """
    race_type = reduce_racetype(race_type).lower().replace("/", "_")
    path = os.path.join(directory, race_type + ".json")
    start_line = None
    if os.path.exists(path):
        with open(path, "r") as f:
            start_line = json.load(f)
    return start_line


def create_start_lines(gmax_feed: GmaxFeed,
                       lower_date: datetime = datetime(2018,1,1),
                       upper_date: datetime = datetime.today(),
                       race_types: list = None,
                       racecourses: list = None,
                       offline: bool = True,
                       off_times: dict = {},
                       sample_size: int = 50
                       ) -> dict:
    """
    for the given daterange and specific race types/courses, compute
    new start line geometry from the post race GPS points and save them
    into the start line directory in environment variable STARTLINE_COORDS_DIRECTORY.

    Parameters
    ----------
    gmax_feed : GmaxFeed,
        GmaxFeed instance to use to fetch the racelist and the points
        from the local or remote sources.
    lower_date : datetime, optional
        lower date boundary for racelist query.
        The default is datetime(2018,1,1).
    upper_date : datetime, optional
        upper date boundary for racelist query.
        The default is datetime.today()
    race_types : list, optional
        list of specific race types to recreate.
        The default is None, and all race types are recreated.
    racecourses : list, optional
        list of specific racecourse to recreate.
        The default is None, and all records are recreated.
    offline : bool, optional
        parameter for GmaxFeed instance, if True no requests are sent to
        the Gmax APIs and only the local file system cache is used.
        The default is True.
    off_times : dict, optional
        dictionary of known off times for each race, as dict of sharecode
        to a datetime object of naive UTC.
        If given, this can be used to identify the starting stall GPS
        points instead of estimating the start point from the minimum
        velocities, and may be more accurate.
        The default is {}
    """
    racelist = gmax_feed.get_racelist_range(
        start_date = lower_date,
        end_date = upper_date,
        offline = offline
        )
    # apply further filter conditions
    if racecourses:
        racecourses = set(racecourses)
        racelist = {
            sc: v for sc, v in racelist.items()
            if v["Racecourse"] in racecourses
            }
    # group sharecodes into race types, and apply filter conditions
    race_type_groups = {
        reduce_racetype(row["RaceType"]).lower(): []
        for row in racelist.values() if row["Published"]
        }
    if race_types:
        race_types = set([reduce_racetype(rt).lower() for rt in race_types])
        race_type_groups = {
            k: v for k, v in race_type_groups.items()
            if k in race_types
            }
    for row in racelist.values():
        rt = reduce_racetype(row["RaceType"]).lower()
        if rt in race_type_groups and row["Published"]:
            race_type_groups[rt].append(row["I"])
    # for each race type, fetch the points data (or a good sample size of it)
    for race_type, sharecodes in race_type_groups.items():
        # possible for some flat races to be flag start when stalls are broken,
        # but it's so rare that it should get buried by the stalls data
        is_jumps = any([
            x in race_type
            for x in ["chase", "hurdle", "nh_flat", "nhflat", "nh flat"]
            ])
        if len(sharecodes) > sample_size:
            sharecodes = np.random.choice(
                sharecodes,
                size = sample_size,
                replace = False
                )
            sharecodes = sharecodes.tolist()
        points = gmax_feed.get_data(
            sharecodes = sharecodes,
            request = {"points"},
            offline = offline
            ).get("points")
        # find timestamp at which all runners are in the starting stalls.
        # for jumps races, due to the run-up, the logic will work a bit
        # differently, ensure the LOBF is parallel to the direction of
        # travel by taking subsequent timestamp around the point at which P
        # begins to decrease, then take the negative reciprocal of the gradient
        # which gives the gradient of the perpendicular line.
        # apply this to the average point at which P begins to decrease and
        # use that as the result.
        if is_jumps:
            all_coordinates = pd.DataFrame()
            all_bearings = []
            for sc, sc_points in points.items():
                if not sc_points:
                    continue
                runners = list(set([row["I"] for row in sc_points]))
                df = pd.DataFrame.from_records(sc_points)
                # find the timestamp at which runner P fields begin to decrease
                # and take runner coordinates at t-1 and t+1 to sample the init
                # bearing for each race, then cache the coords near the start line.
                # take negative reciprocal of the mean bearing for startline bearing
                # and use with average x,y to create a polynomial to go into the record.
                max_p = df.P.max()
                df2 = df[df.P < max_p]
                runner_start_timestamps = df2[["I", "T"]].groupby("I").min()
                runner_start_timestamps.loc[
                    runner_start_timestamps.index,
                    "T"
                    ] = runner_start_timestamps["T"].apply(
                    dateutil.parser.parse
                    )
                ts1 = [
                    put_datetime(x)
                    for x in
                    (runner_start_timestamps - timedelta(seconds = 1))["T"].to_list()
                    ]
                ts2 = [
                    put_datetime(x)
                    for x in
                    (runner_start_timestamps + timedelta(seconds = 1))["T"].to_list()
                    ]
                df3 = df.set_index(["I", "T"])
                for t1, t2, runner_sc in zip(ts1, ts2, runner_start_timestamps.index):
                    try:
                        init_coords = df3.loc[(runner_sc, t1), ["X", "Y"]]
                        final_coords = df3.loc[(runner_sc, t2), ["X", "Y"]]
                    except Exception:
                        logger.exception(
                            "Error occurred accessing runner_sc timestamp for coords: {0}".format(
                                    (runner_sc, t1, t2)
                                )
                            )
                        continue
                    b = compute_bearing(
                        coords1 = (init_coords["X"], init_coords["Y"]),
                        coords2 = (final_coords["X"], final_coords["Y"])
                        )
                    x = np.mean([init_coords["X"], final_coords["X"]])
                    y = np.mean([init_coords["Y"], final_coords["Y"]])
                    all_bearings.append(b)
                    all_coordinates = pd.concat(
                        (all_coordinates, pd.DataFrame({"X": [x], "Y": [y]}))
                        )
            if all_bearings:
                all_bearings = np.array(all_bearings)
                mean_bearing = compute_mean_bearing(
                    all_bearings[np.isfinite(all_bearings)]
                    )
                if np.isnan(mean_bearing):
                    continue
                perpendicular_bearing = -1 / mean_bearing
                xy1 = (
                    np.mean(all_coordinates.X),
                    np.mean(all_coordinates.Y)
                    )
                xy2 = compute_new_coords(
                    X1 = xy1[0],
                    Y1 = xy1[1],
                    D = 5,
                    B = perpendicular_bearing
                    )
                xy3 = compute_new_coords(
                    X1 = xy1[0],
                    Y1 = xy1[1],
                    D = -5,
                    B = perpendicular_bearing
                    )
                # fit linear regressor to get gradient and intercept
                poly = np.polyfit(
                    x = [xy1[0], xy2[0], xy3[0]],
                    y = [xy1[1], xy2[1], xy3[1]],
                    deg = 1
                    )
                start_line_coords = {
                    "poly": [
                        poly[0],
                        poly[1]
                        ],
                    "xy1": list(xy2),
                    "xy2": list(xy3)
                    }
                # save the startline coords for this racetype in the filesystem
                save_start_line(
                    data = start_line_coords, 
                    race_type = race_type
                    )
            else:
                continue
        else:
            all_coordinates = pd.DataFrame()
            for sc, sc_points in points.items():
                if not sc_points:
                    continue
                # identify timestamp in the stalls and extract coords
                runners = list(set([row["I"] for row in sc_points]))
                start_timestamp = off_times.get(sc)
                if not start_timestamp:
                    df = pd.DataFrame.from_records(sc_points)
                    runner_count = df[["T", "I"]].groupby("T").count().reset_index()
                    valid_timestamps = runner_count[runner_count.I == len(runners)]["T"]
                    tempdf = df.loc[
                        df["T"].isin(valid_timestamps),
                        ["T", "V"]
                        ]
                    tempdf = tempdf.assign(
                        V2 = np.power(tempdf.V, 2)
                        )
                    sum_sq_v = tempdf.groupby("T").sum()
                    min_sum_sq_v = sum_sq_v[sum_sq_v["V2"] == sum_sq_v["V2"].min()]
                    start_timestamp = min_sum_sq_v.index[0]
                else:
                    start_timestamp -= timedelta(seconds = 1.5)
                    start_timestamp = (
                        start_timestamp - timedelta(
                            microseconds = start_timestamp.microsecond
                            )
                        )
                    start_timestamp = put_datetime(start_timestamp)
                start_coords = df.loc[
                    df["T"] == start_timestamp,
                    ["T", "X", "Y"]
                    ]
                all_coordinates = pd.concat((all_coordinates, start_coords))
            # once all coords have been extracted, find average X and Y.
            av_x = all_coordinates.X.mean()
            av_y = all_coordinates.Y.mean()
            # translate each race onto the average X and Y for the racetype, to
            # eliminate small placement errors in the starting stall location
            group_differences_x = (
                all_coordinates[["T", "X"]].groupby("T").mean() - av_x
                ).reset_index()
            group_differences_y = (
                all_coordinates[["T", "Y"]].groupby("T").mean() - av_y
                ).reset_index()
            
            tempdf_x = group_differences_x.merge(
                all_coordinates[["T", "X"]],
                on = "T",
                suffixes = ("_left", "_right")
                )
            translated_x = tempdf_x["X_right"] - tempdf_x["X_left"]
            
            tempdf_y = group_differences_y.merge(
                all_coordinates[["T", "Y"]],
                on = "T",
                suffixes = ("_left", "_right")
                )
            translated_y = tempdf_y["Y_right"] - tempdf_y["Y_left"]
            # fit the initial LOBF on the translated data
            x = translated_x.to_numpy().reshape(-1, 1)
            y = translated_y.to_numpy().reshape(-1, 1)
            poly_rls = RANSACRegressor()
            poly_rls.fit(x, y)
            start_line_coords = {
                "poly": [
                    poly_rls.estimator_.coef_[0][0],
                    poly_rls.estimator_.intercept_[0]
                    ],
                "xy1": None,
                "xy2": None
                }
            poly = np.poly1d(start_line_coords["poly"])
            observations = len(x)
            lower_x = np.sort(x)[int(observations * 0.2)]
            upper_x = np.sort(x)[int(observations * 0.8)]
            start_line_coords["xy1"] = [float(lower_x), float(poly(np.min(lower_x)))]
            start_line_coords["xy2"] = [float(upper_x), float(poly(np.max(upper_x)))]
            # save the startline coords for this racetype in the filesystem
            save_start_line(
                data = start_line_coords, 
                race_type = race_type
                )
