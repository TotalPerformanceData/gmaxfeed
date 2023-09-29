#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 11:48:33 2019

Aesthetic class for requesting data from the gmax API.

RaceMetadata can be used for filtering the sharecodes

Keep up to date using update function, either by running gmaxfeed/main_update.py
or scheduling update() in other applications.

Don't run this as main as imports don't work then.

Gmax Licence can be hard coded in GmaxFeed in line 403, or more preferably set
as an environment variable either by inserting lines of definitions after the
imports as indicated on lines 66 to 76 (commented out) or more preferably still
by setting persistent environment variables.

setting env variables:
OSX:
    in the terminal, use the command
    $export GMAXLICENCE=my_licence
    permanently add to source by adding a line to ~/.bash_profile or ~/.profile:
    nano ~/.bash_profile
    then scroll to bottom and add line,
    export GMAXLICENCE=my_licence
    type "source ~/.bash_profile" to enable. When a new terminal session is loaded
    the deafult profile is loaded, this varies depending on which files are
    available and which shell script language is being used.
Linux:
    as above, but ~/.bash_profile is more liekly to be ~/.profile, or you could
    put them at the bottom of ~/.bashrc.
Windows:
    the command is "set" instead of "export", and to set persistently for future
    cmd sessions I think you just need to use
    setx GMAXLICENCE my_licence
    there's also a GUI which can be found by searching for "env" in the windows
    search tool.

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

import os
import dateutil
import json

from .utils import (listdir2,
                    to_datetime,
                    check_file_exists,
                    read_file,
                    reformat_sectionals_list,
                    export_sectionals_to_xls,
                    export_sectionals_to_csv,
                    read_url,
                    load_file,
                    alter_sectionals_gate_label,
                    process_url_response,
                    apply_thread_pool,
                    route_xml_to_json)
from datetime import datetime, timedelta, timezone
from datetime import date as date_

"""
# environment variables used
os.environ['FIXTURES_PATH'] = '/path/to/fixtures'
os.environ['RACELIST_PATH'] = '/path/to/racelist'
os.environ['SEC_PATH'] = '/path/to/sectionals'
os.environ['GPS_PATH'] = '/path/to/gpsData'
os.environ['SEC_HIST_PATH'] = '/path/to/sectionals-hist'
os.environ['SEC_RAW_PATH'] = '/path/to/sectionals-raw' #internal
os.environ['PERFORMANCE_PATH'] = '/path/to/tracker-performance' #internal
os.environ['ROUTE_PATH'] = '/path/to/routes'
os.environ['JUMPS_PATH'] = '/path/to/jumps'
os.environ['GMAXLICENCE'] = 'my_licence'
"""

from .. import get_logger
logger = get_logger(name = __name__)

# some courses use metric units for sectional "G" field and needs to be changed
# from "200m" to "1f" to pass through other sorts and parsers.
METRIC_GATES = {"65", "66", "67", "68", "31"}


class RaceMetadata:
    """
    group metadata about the races, and filter for given countries, courses, 
    published status etc if applicable.
    
    can define without data, and call set_filter() to apply to something later,
    such as in gmax_feed.get_data()
    """
    def __init__(self, data: dict = None, direc: str = None):
        self.clear()
        self.import_data(data = data, direc = direc)
        self._filter = {}
    
    def __iter__(self) -> str:
        """
        iterate the values which passed the filter parameters, returning tuple 
        of (key, value).
        Be careful not to make changes to the items in the dict as the keys in 
        self._list and self._data actually point to the same place 
        """
        yield from self._list.keys()
    
    def __len__(self) -> int:
        return len(self._list)
    
    def __repr__(self) -> str:
        return "< RaceMetadata - Races:{0} >".format(len(self._data))
    
    def set_filter(self,
                   countries: list or set = None,
                   courses: list or set = None,
                   course_codes: list or set = None,
                   published: bool = None,
                   start_date: datetime or str = None,
                   end_date: datetime or str = None,
                   race_types: list or set = None,
                   opts: dict = {}) -> None:
        """
        set the internal filter using the named values or a dict of named values

        Parameters
        ----------
        countries : list or set, optional
            country codes to be included, such as GB, US, CA, FR. 
            The default is None.
        courses : list or set, optional
            course names to be included, such as 'Wolverhampton', 'Newcastle'. 
            The default is None.
        course_codes : list or set, optional
            gmax courses codes to be included, such as '04', '14', '53'. 
            The default is None.
        published : bool, optional
            whether to included only published, or only unpublished races. 
            The default is None meaning no filter applied.
        start_date : datetime or str, optional
            lower date boundary. The default is None.
        end_date : datetime or str, optional
            upper date boundary. The default is None.
        race_types : list or set, optional
            Gmax RaceTypes to include in the filter. The default is None.
        """
        self._filter = {
            'countries': countries,
            'courses': courses,
            'course_codes': course_codes,
            'published': published,
            'start_date': start_date,
            'end_date': end_date,
            'race_types': race_types
            }
    
    def get(self, sharecode: str) -> dict or None:
        """
        get metadata for a sharecode from self._data.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD race identifier, knwon as sharecode.

        Returns
        -------
        dict or None
        """
        return self._data.get(sharecode)
    
    def clear(self) -> None:
        """
        clear race metadata from self
        """
        self._data = {}
        self._list = self._data
    
    def import_data(self,
                    data: list or dict = None,
                    direc: str = None
                    ) -> None:
        """
        add the races in data to self._data, takes either list of dicts, or 
        dict mapping each sharecode -> race_metadata if data is None and a 
        directory is passed instead (path to racelist folder) this contents of
        the folder are iterated and imported
        can be called multiple times, for instance if you run it in the morning
        to gather all metadata in one place and then want to add metadata for 
        new races that have appeared in the gmax racelist later that day.

        Parameters
        ----------
        data : list or dict, optional
            metadata records. The default is None.
        direc : str, optional
            directory from which to load records. The default is None.
        """
        if data is None:
            if direc is None:
                return
            files = listdir2(direc)
            for file in files:
                d = read_file(os.path.join(direc, file))
                for sc in d:
                    self._data[sc] = d[sc]
        else:
            if type(data) is list:
                for row in data:
                    self._data[row['I']] = row
            elif type(data) is dict:
                for row in data.values():
                    self._data[row['I']] = row
        self._list = self._data
    
    def get_set(self,
                countries: bool = True,
                courses: bool = True,
                course_codes: bool = True,
                race_types: bool = True
                ) -> dict:
        """
        get a set of all possible values within self._data for the given fields
        and return as dictionary of sets. Useful for passing "everything except" 
        conditions to filter, eg, for all courses except Ascot Newcastle and Bath,
        obj.filter(courses = obj.get_set().get('courses') - {'Ascot', 'Newcastle', 'Bath'})

        Parameters
        ----------
        countries : bool, optional
            include set of available countries in output.
            The default is True.
        courses : bool, optional
            include set of available courses in output.
            The default is True.
        course_codes : bool, optional
            include set of available course_codes in output.
            The default is True.
        race_types : bool, optional
            include set of available race_types in output.
            The default is True.

        Returns
        -------
        dict
        """
        output = {}
        if countries:
            output['countries'] = set([sc.get('Country') for sc in self._data.values()])
        if courses:
            output['courses'] = set([sc.get('Racecourse') for sc in self._data.values()])
        if course_codes:
            # assume first two chars are the course code, might change in later years
            output['course_codes'] = set([sc[:2] for sc in self._data])
        if race_types:
            output['race_types'] = set([sc.get('RaceType') for sc in self._data.values()])
        return output
    
    def apply_filter(self,
                     countries: list or set = None,
                     courses: list or set = None,
                     course_codes: list or set = None,
                     published: bool = None,
                     start_date: datetime or str = None,
                     end_date: datetime or str = None,
                     race_types: list or set = None,
                     data: list or dict = None
                     ) -> None:
        """
        filter the sharecodes within self._data by the given conditions
        passing sets will be much faster if giving long lists of options.
        courses = ['Ascot', 'Newcastle', 'Lingfield Park']
        course_codes = ['01', '35', '30']
        countries = ['US', 'GB']
        published = True # compare the race_data['Published'] field against the
            given published paramter and return if matches
            
        # must be timezone aware as compared to race_data['PostTime'] parsed field
        start_date = datetime(2020, 1, 1, tzinfo = timezone.utc)
        end_date = datetime(2020, 1, 1, tzinfo = timezone.utc)
        
        Parameters
        ----------
        countries : list or set, optional
            iterable of countries to include. The default is None.
        courses : list or set, optional
            iterable of courses to include. The default is None.
        course_codes : list or set, optional
            iterable of course_codes to include. The default is None.
        published : bool, optional
            include only published races, only unpublished races, or all.
            The default is None.
        start_date : datetime or str, optional
            lower bound datetime.
            must be timezone aware as is compared to race_data['PostTime'] parsed
            field.
            The default is None.
        end_date : datetime or str, optional
            upper bound datetime. 
            must be timezone aware as is compared to race_data['PostTime'] parsed
            field.
            The default is None.
        race_types : list or set, optional
            iterable of race_types to include. The default is None.
        data : list or dict, optional
            metadata records to import and filter. The default is None.
        """
        if data is not None:
            self.import_data(data = data)
        countries = countries or self._filter.get('countries')
        courses = courses or self._filter.get('courses')
        course_codes = course_codes or self._filter.get('course_codes')
        race_types = race_types or self._filter.get('race_types')
        published = published or self._filter.get('published')
        start_date = to_datetime(
            start_date or self._filter.get('start_date'),
            tz = dateutil.tz.UTC
            )
        end_date = to_datetime(
            end_date or self._filter.get('end_date'),
            tz = dateutil.tz.UTC
            )
        if all([x is None for x in [countries, courses, course_codes, published, start_date, end_date, race_types]]):
            self._list = self._data
        else:
            self._list = {}
            for sc, row in self._data.items():
                if race_types is not None:
                    if row['RaceType'] not in race_types:
                        continue
                if countries is not None:
                    if row['Country'] not in countries:
                        continue
                if courses is not None:
                    if row['Racecourse'] not in courses:
                        continue
                if course_codes is not None:
                    if sc[:2] not in course_codes:
                        continue
                if published is not None:
                    if row['Published'] != published:
                        continue
                parsed_date = dateutil.parser.parse(row['PostTime'])
                if start_date is not None:
                    if parsed_date < start_date:
                        continue
                if end_date is not None:
                    if parsed_date > end_date:
                        continue
                self._list[sc] = row


class GmaxFeed:
    """
    instantiate with licence key so don't have to keep placing into function 
    calls.
    
    if licence key not passed on instantiation checks for a licence key in env 
    called 'GMAXLICENCE'
    """
    def __init__(self, 
                 licence: str = None,
                 fixtures_path: str = None,
                 racelist_path: str = None, 
                 sectionals_path: str = None, 
                 gps_path: str = None, 
                 route_path: str = None,
                 sectionals_history_path: str = None,
                 sectionals_raw_path: str = None,
                 jumps_path: str = None,
                 performance_path: str = None) -> None:
        """
        instantiate GmaxFeed object to manage downloads and cached file fetching.

        Parameters
        ----------
        licence : str, optional
            Gmax/TPD licence key. The default is None.
        fixtures_path : str, optional
            path to fixtures directory cache. The default is None.
        racelist_path : str, optional
            path to racelist directory cache. The default is None.
        sectionals_path : str, optional
            path to sectionals directory cache. The default is None.
        gps_path : str, optional
            path to post race GPS data directory cache.
            The default is None.
        route_path : str, optional
            path to route directory cache.
            The default is None.
        sectionals_history_path : str, optional
            path to sectionals history directory cache.
            The default is None.
        sectionals_raw_path : str, optional
            path to sectionals raw directory cache. The default is None.
        jumps_path : str, optional
            path to jumps/obstacles directory cache. The default is None.
        performance_path : str, optional
            path to performance directory cache. The default is None.

        Raises
        ------
        Exception
            if no Gmax/TPD licence is set manually or via environment vars.
        """
        self.licence = licence
        self.set_fixtures_path(path = fixtures_path)
        self.set_racelist_path(path = racelist_path)
        self.set_gps_path(path = gps_path)
        self.set_route_path(path = route_path)
        self.set_sectionals_path(path = sectionals_path)
        self.set_sectionals_history_path(path = sectionals_history_path)
        self.set_sectionals_raw_path(path = sectionals_raw_path)
        self.set_jumps_path(path = jumps_path)
        self.set_tracker_performance_path(path = performance_path)
        if not self:
            raise Exception(
                ['No licence key set by GmaxFeed, pass licence = "my_licence" to '
                ' constructor, or set GMAXLICENCE = "my_licence" as environment '
                'variable'][0]
                )
    
    def __bool__(self) -> bool:
        return self.licence is not None
    
    def __repr__(self) -> str:
        return "< GmaxFeed >"
    
    @property
    def licence(self) -> str:
        return os.environ.get("GMAXLICENCE")
    
    @licence.setter
    def licence(self, licence: str = None) -> None:
        if licence is not None:
            os.environ["GMAXLICENCE"] = licence
    
    def _confirm_exists(self, path: str) -> bool:
        if not os.path.exists(path):
            os.mkdir(path)
    
    def set_fixtures_path(self, path: str = None) -> None:
        self._fixtures_path = path or os.environ.get('FIXTURES_PATH') or 'fixtures'
        self._confirm_exists(self._fixtures_path)
    
    def set_racelist_path(self, path: str = None) -> None:
        self._racelist_path = path or os.environ.get('RACELIST_PATH') or 'racelist'
        self._confirm_exists(self._racelist_path)
    
    def set_sectionals_path(self, path: str = None) -> None:
        self._sectionals_path = path or os.environ.get('SEC_PATH') or 'sectionals'
        self._confirm_exists(self._sectionals_path)
    
    def set_gps_path(self, path: str = None) -> None:
        self._gps_path = path or os.environ.get('GPS_PATH') or 'gpsData'
        self._confirm_exists(self._gps_path)
    
    def set_route_path(self, path: str = None) -> None:
        self._route_path = path or os.environ.get('ROUTE_PATH') or 'routes'
        self._confirm_exists(self._route_path)
    
    def set_sectionals_history_path(self, path: str = None) -> None:
        self._sectionals_history_path = path or os.environ.get('SEC_HIST_PATH') or 'sectionals-hist'
        self._confirm_exists(self._sectionals_history_path)
    
    def set_sectionals_raw_path(self, path: str = None) -> None:
        self._sectionals_raw_path = path or os.environ.get('SEC_RAW_PATH') or 'sectionals-raw'
        self._confirm_exists(self._sectionals_raw_path)
    
    def set_jumps_path(self, path: str = None) -> None:
        self._jumps_path = path or os.environ.get('JUMPS_PATH') or 'jumps'
        self._confirm_exists(self._jumps_path)
    
    def set_tracker_performance_path(self, path: str = None) -> None:
        self._errors_path = path or os.environ.get('PERFORMANCE_PATH') or 'tracker-errors'
        self._confirm_exists(self._errors_path)
    
    def get_fixtures(self, date: str = None, **kwargs) -> list or False:
        """
        fetch fixtures for the given date (or datetime.today() if date = None)
        from the gmax /fixtures feed, return next 7 days of fixtures from date given

        Parameters
        ----------
        self date : str
            date for which to fetch upcoming fixtures.
        
        **params
        new : bool, optional
            whether to ignore the cached file and fetch new. The default is False.
        offline : bool, optional
            whether to only use cached files. The default is False.
        no_return : bool, optional
            whether to return None to save memory updating files.
            The default is False.

        Returns
        -------
        list or False
            list of fixtures or False.
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        data = []
        if date is None:
            date = datetime.utcnow()
        else:
            date = to_datetime(date)
        date_str = date.strftime('%Y-%m-%d')
        path = os.path.join(self._fixtures_path, date_str)
        if os.path.exists(path) and not new:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            limit_date = date + timedelta(days = 6)
            if (not new and mtime > limit_date) or offline:
                if no_return:
                    data = check_file_exists(
                        direc = self._fixtures_path,
                        fname = date_str
                        )
                else:
                    data = load_file(
                        direc = self._fixtures_path,
                        fname = date_str
                        )
                if data is not None:
                    return data
        # if data is None file doesn't exist, try downloading a new file if offline is False
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/fixtures.ashx?DateLocal={0}&k={1}'.format(
                date_str, self.licence
                )
            data = process_url_response(
                url = url,
                direc = self._fixtures_path,
                fname = date_str,
                version = 1
                ) or False
        if no_return:
            data = None
        return data
    
    def get_race(self,
                 sharecode: str,
                 date: str or datetime = None,
                 **kwargs
                 ) -> dict or False:
        """
        fetch the TPD/Gmax RaceList data for just the given sharecode.
        If 'date' is given that's used as the date to fetch from cache in 
        offline == new == True mode, else parsed from within the sharecode.

        Parameters
        ----------
        sharecode : str
            The sharecode for which to get all racelist details.
        date : str, optional
            the date for the sharecode to aid cache search.
            The default is None.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
            The record for the sharecode, or False if not found.
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        data = {}
        if date is None:
            date = datetime.strptime(sharecode[2:10], '%Y%m%d')
        else:
            date = to_datetime(date)
        date_str = date.strftime('%Y-%m-%d')
        path = os.path.join(self._racelist_path, date_str)
        if os.path.exists(path) and not new:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            limit_date = date + timedelta(days = 6)
            if (not new and mtime > limit_date) or offline:
                data = load_file(
                    direc = self._racelist_path,
                    fname = date_str
                    )
                if data is not None:
                    return data.get(sharecode) or False
        # if data is None file doesn't exist, try downloading a new file if offline is False
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/racelist.ashx?Sharecode={0}&k={1}'.format(
                sharecode, self.licence
                )
            # returns a list of 1 dict or empty list - process manually here as don't want to cache just one race
            txt = read_url(url)
            if txt:
                data = {row['I']:row for row in json.loads(txt)}
        return data.get(sharecode) or False
    
    def get_racelist(self,
                     date: str or datetime = None,
                     sharecode: str = None,
                     **kwargs
                     ) -> dict:
        """
        fetch a racelist for some date.
        
        sometimes may want to query the metadata for a specific race, like when
        populating jumps data and checking the race is NH for this case can 
        leave date as None and pass a sharecode, the date is then inferred from 
        the sharecode assuming sc[2:10] = %Y%m%d, but preferred route for this
        is self.get_race(sharecode = sc, date = date, **kwargs).

        Parameters
        ----------
        date : str or datetime, optional
            date for which to fetch racelist.
            format is %Y-%m-%d. The default is None.
        sharecode : str, optional
            specific sharecode for which to fetch metadata.
            The default is None.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        data = {}
        if sharecode is not None:
            date = datetime.strptime(sharecode[2:10], '%Y%m%d')
        if date is None:
            date = datetime.today()
        if type(date) is datetime or type(date) is date_:
            date = date.strftime('%Y-%m-%d')
        path = os.path.join(self._racelist_path, date)
        if os.path.exists(path):
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            limit_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days = 6)
            if (not new and mtime > limit_date) or offline:
                data = load_file(direc = self._racelist_path, fname = date)
                if data is not None:
                    if sharecode is not None:
                        return data.get(sharecode) or False
                    else:
                        return data
        # if data is None file doesn't exist, try downloading a new file if offline is False
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/racelist.ashx?DateLocal={0}&k={1}'.format(
                date, self.licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._racelist_path,
                fname = date,
                version = 2
                )
        if sharecode is not None:
            return data.get(sharecode) or False
        else:
            return data
    
    def get_racelist_range(self,
                           start_date: datetime or str = None,
                           end_date: datetime or str = None,
                           **kwargs
                           ) -> dict:
        """
        get racelist feed for a range of dates.

        Parameters
        ----------
        start_date : datetime or str, optional
            lower date boundary, inclusive. The default is None.
        end_date : datetime or str, optional
            upper date boundary, inclusive. The default is None.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        if start_date is None:
            start_date = datetime(2016, 1, 1)
        else:
            start_date = to_datetime(start_date)
        if end_date is None:
            end_date = datetime.utcnow()
        else:
            end_date = to_datetime(end_date)
        if end_date < start_date:
            end_date = start_date
        end_date += timedelta(days = 1) # to include last date in range
        range_ = (end_date - start_date).days
        dates = [start_date + timedelta(days = dt) for dt in range(0, range_, 1)]
        result = apply_thread_pool(
            self.get_racelist,
            dates,
            new = new,
            offline = offline
            )
        data = {}
        for row in result:
            if row:
                data.update(row)
        return data
    
    def get_points(self, sharecode: str, **kwargs) -> dict:
        """
        get post race GPS points for an iterable of sharecodes.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        data = None
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._gps_path,
                    fname = sharecode
                    )
            else:
                data = load_file(direc = self._gps_path, fname = sharecode)
            if data is not None:
                return {'sc': sharecode, 'data': data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/points.ashx?Sharecode={0}&k={1}'.format(
                sharecode, self.licence
                )
            # returns rows of dicts delimited by newline characters, r"\r\n", readlines() issue blank final element of list
            data = process_url_response(
                url = url,
                direc = self._gps_path,
                fname = sharecode,
                version = 3
                )
        if no_return:
            data = None
        return {'sc': sharecode, 'data': data}
    
    def get_sectionals(self, sharecode: str, **kwargs) -> dict:
        """
        get post race sectional data for an iterable of sharecodes.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        data = None
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._sectionals_path,
                    fname = sharecode
                    )
            else:
                data = load_file(direc = self._sectionals_path, fname = sharecode)
            if data is not None:
                if not no_return and sharecode[:2] in METRIC_GATES:
                    data = alter_sectionals_gate_label(sectionals = data)
                return {'sc': sharecode, 'data': data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals.ashx?Sharecode={0}&k={1}'.format(
                sharecode, self.licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._sectionals_path,
                fname = sharecode,
                version = 1
                )
        if no_return:
            data = None
        if data and sharecode[:2] in METRIC_GATES:
            data = alter_sectionals_gate_label(sectionals = data)
        return {'sc': sharecode, 'data': data}
    
    def get_sectionals_history(self, sharecode: str, **kwargs) -> dict:
        """
        get sectionals history feed for an iterable of sharecodes.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        data = None
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._sectionals_history_path,
                    fname = sharecode
                    )
            else:
                data = load_file(
                    direc = self._sectionals_history_path,
                    fname = sharecode
                    )
            if data is not None:
                if not no_return and sharecode[:2] in METRIC_GATES:
                    data = alter_sectionals_gate_label(sectionals = data)
                return {'sc': sharecode, 'data': data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals-history.ashx?Sharecode={0}&k={1}'.format(
                sharecode, self.licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._sectionals_history_path,
                fname = sharecode,
                version = 1
                )
        if no_return:
            data = None
        if data and sharecode[:2] in METRIC_GATES:
            data = alter_sectionals_gate_label(sectionals = data)
        return {'sc': sharecode, 'data': data}
    
    def get_sectionals_raw(self, sharecode: str, **kwargs) -> dict:
        """
        get sectionals raw feed for an iterable of sharecodes.

        internal use function.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        # internal use only
        licence = os.environ.get('ALTLICENCE')
        data = None
        if licence is None:
            return {'sc': sharecode, 'data': None}
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._sectionals_raw_path,
                    fname = sharecode
                    )
            else:
                data = load_file(
                    direc = self._sectionals_raw_path,
                    fname = sharecode
                    )
            if data is not None:
                if not no_return and sharecode[:2] in METRIC_GATES:
                    data = alter_sectionals_gate_label(sectionals = data)
                return {'sc': sharecode, 'data': data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals-raw.ashx?Sharecode={0}&k={1}'.format(
                sharecode,
                licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._sectionals_raw_path,
                fname = sharecode,
                version = 1
                )
        if no_return:
            data = None
        if data and sharecode[:2] in METRIC_GATES:
            data = alter_sectionals_gate_label(sectionals = data)
        return {'sc': sharecode, 'data': data}
    
    def get_sectionals_modified(self,
                                dt: str or datetime,
                                **kwargs
                                ) -> dict:
        """
        get sectionals modified feed for a given datetime.
        
        spec : 
        https://www.gmaxequine.com/downloads/GX-UG-00059%202021-06-21%20Gmax%20Race%20Modified%20Data%20Feed%20Specification.pdf

        given a datetime in ISO format, YYYY-mm-ddTHH:MM:SSZ, return a list of
        gmax sharecodes for which the post race data has been modified any time
        between the given datetime and the following 7 days.
        
        This method does not cache the response.
        
        fields in each record are:
            "I": str, sharecode
            "Modified": datetime, UTC tzaware datetime of modification
            "Published": bool, publish status of race

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict - map of sharecode to it's modified timestamp and published status
        """
        data = {}
        if type(dt) is str:
            dt = dateutil.parser.parse(dt)
        datestring = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = 'https://www.gmaxequine.com/TPD/client/sectionals-modified.ashx?DateFrom={0}&k={1}'.format(
            datestring, self.licence
            )
        txt = read_url(url)
        if txt:
            data = json.loads(txt)
            for row in data:
                row["Modified"] = dateutil.parser.parse(row["Modified"])
            data = {row["I"] : row for row in data}
        return data

    def get_sectionals_modified_range(self,
                                      start_date: datetime or str = None,
                                      end_date: datetime or str = None,
                                      **kwargs
                                      ) -> dict:
        """
        get sectionals modified feed for a range of dates.

        Parameters
        ----------
        start_date : datetime or str, optional
            lower date boundary, inclusive. The default is None.
        end_date : datetime or str, optional
            upper date boundary, inclusive. The default is None.

        Returns
        -------
        dict - map of sharecode to it's modified timestamp and published status
        """
        if start_date is None:
            start_date = datetime(2016, 1, 1)
        else:
            start_date = to_datetime(start_date)
        if end_date is None:
            end_date = datetime.utcnow()
        else:
            end_date = to_datetime(end_date)
        if end_date < start_date:
            end_date = start_date
        range_ = (end_date - start_date).days
        dates = [
            start_date + timedelta(days = dt) for dt in range(0, range_, 6)
            ]
        dates.append(end_date)
        results = apply_thread_pool(
            self.get_sectionals_modified,
            dates
            )
        data = {}
        for result in results:
            if result:
                data.update(result)
        return data

    def get_tracker_performance(self, sharecode: str, **kwargs) -> dict:
        """
        get performance/accuracy feed for an iterable of sharecodes.

        internal use function for our own monitoring of tracker quality.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        # internal use only
        data = None
        if not new:
            data = load_file(direc = self._errors_path, fname = sharecode)
            if data is not None and any([row.get("RX") for row in data]):
                if no_return:
                    data = None
                return {'sc': sharecode, 'data': data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/performance.ashx?Sharecode={0}&k={1}'.format(
                sharecode, self.licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._errors_path,
                fname = sharecode,
                version = 1
                )
        if no_return:
            data = None
        return {'sc': sharecode, 'data': data}
    
    def get_obstacles(self, sharecode: str, **kwargs) -> dict:
        """
        get jumps feed for an iterable of sharecodes.

        NOTE that the obstacle locations are only available from Dec 2020,
        and in some cases may be missing after this date due to issues
        surveying the locations on the day.

        Parameters
        ----------
        sharecode : str
            Gmax/TPD sharecode/race_id.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.
        metadata : dict, optional
            dict of the race metadata for this race, as returned by
            self.get_race(sharecode)

        Returns
        -------
        dict
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        metadata = kwargs.get("metadata") or \
            self.get_race(sharecode = sharecode, offline = offline)
        if not metadata or \
            "RaceType" not in metadata or \
            not any([x in metadata["RaceType"].lower() for x in ["hurdle", "chase", "nh flat"]]):
            return {"sc": sharecode, "data": None}
        data = None
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._jumps_path,
                    fname = sharecode
                    )
            else:
                data = load_file(direc = self._jumps_path, fname = sharecode)
            if data is not None:
                return {"sc": sharecode, "data": data}
        if not offline:
            url = "https://www.gmaxequine.com/TPD/client/jumps.ashx?Sharecode={0}&k={1}".format(
                sharecode, self.licence
                )
            # returns a list of dicts
            data = process_url_response(
                url = url,
                direc = self._jumps_path,
                fname = sharecode,
                version = 1
                )
        if no_return:
            data = None
        return {"sc": sharecode, "data": data}
    
    def get_route(self, course_code: str or int, **kwargs) -> dict:
        """
        save a KML file in route directory.
        
        returns a parsed version using utils.route_xml_to_json function.

        Parameters
        ----------
        course_code : str or int,
            course code to fetch.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict : {"course_code": int, "data": str or False}
        """
        new = kwargs.get("new")
        offline = kwargs.get("offline")
        no_return = kwargs.get("no_return")
        output = {"course_code": course_code, "data": False}
        course_code = str(course_code).zfill(2)
        fname = 'Racecourse-{0}.kml'.format(course_code)
        if not new:
            if no_return:
                data = check_file_exists(
                    direc = self._route_path,
                    fname = fname
                    )
            else:
                data = load_file(
                    direc = self._route_path,
                    fname = fname,
                    is_json = False
                    )
            if data is not None:
                output["data"] = data
                return output
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/routes.ashx?Racecourse={0}&k={1}'.format(
                course_code, self.licence
                )
            # returns a kml encoded text file
            output["data"] = process_url_response(
                url = url,
                direc = self._route_path,
                fname = fname,
                version = 4
                )
        if no_return:
            data = None
        return output
    
    def get_routes(self,
                   course_codes: list = None,
                   processing_function = route_xml_to_json,
                   **kwargs
                   ) -> dict:
        """
        use threadpool to fetch route files for a list of courses.

        Parameters
        ----------
        course_codes : list
            list of course codes to fetch.
        processing_function : function
            the function to use to process the KML map file.
            Default is Utils.route_xml_to_json
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
            map of coursecode to list of track coordinates.
        """
        if not course_codes:
            sharecodes = self.get_racelist_range(
                start_date = datetime.today() - timedelta(days = 365), 
                end_date = datetime.today(),
                offline = True
                )
            course_codes = list(set([sc[:2] for sc in sharecodes]))
        res = apply_thread_pool(
            func = self.get_route,
            iterable = course_codes,
            **kwargs
            )
        return {
            row["course_code"]: processing_function(row["data"])
            for row in res if row["data"]
            }
    
    def get_data(self,
                 sharecodes: dict or list,
                 request: set = {
                     'sectionals',
                     'sectionals-raw',
                     'sectionals-history',
                     'points',
                     'obstacles'
                     },
                 filter: RaceMetadata = None,
                 **kwargs
                 ) -> dict or None:
        """
        pass dict of racelist data sc -> metadata. if list is passed instead 
        assumed to be raceids and won't be filtered. 
        
        multithreaded entry point for getting big selection of data, 
        downloading new if not present, else using cached version.
        
        note given sharecodes will only appear in the response if the fetched
        data evaluates to True, else the sharecode is dropped from the response.

        Parameters
        ----------
        sharecodes : dict or list
            iterable of sharecodes for which to fetch requested files.
        request : set, optional
            iterable of wanted files.
            The default is {
                'sectionals',
                'sectionals-raw',
                'sectionals-history',
                'points',
                'obstacles'
                }.
        filter : RaceMetadata, optional
            RaceMetadata instance filter. The default is None.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        output = {}
        if type(sharecodes) is dict:
            if filter is None:
                filter = RaceMetadata()
                if all([x not in request for x in [
                        'sectionals-raw', 'sectionals-history', 'points', 'obstacles']
                        ]):
                    filter.set_filter(published = True)
            filter.apply_filter(data = sharecodes)
            sharecodes = list(filter)
        elif not sharecodes:
            return output
        labels2func = {
            "sectionals": self.get_sectionals,
            "sectionals-raw": self.get_sectionals_raw,
            "sectionals-history": self.get_sectionals_history,
            "points": self.get_points,
            "obstacles": self.get_obstacles,
            "performance": self.get_tracker_performance
            }
        for label, func in labels2func.items():
            if label in request:
                result = apply_thread_pool(
                    func = func,
                    iterable = sharecodes,
                    **kwargs
                    )
                output[label] = {
                    row['sc']: row['data'] for row in result if row['data']
                    }
        return output
    
    def update(self,
               start_date: datetime or str = None,
               end_date: datetime or str = None,
               request: set = {'sectionals', 'points'},
               filter: RaceMetadata = None,
               update_before: datetime = None,
               **kwargs
               ) -> None:
        """
        update all the cached file in daterange given, only refresh if new passed. 
        racelists are always freshed if file mtime is less than a week after the
        date it refers if licence key is only activated for one of the above
        feeds then make sure to pass only the request set you want, else
        unauthorsied feed/s or will have folder full of empty text files
        
        Parameters
        ----------
        start_date : datetime or str, optional
            lower date boundary. The default is None.
        end_date : datetime or str, optional
            upper date boundary. The default is None.
        request : set, optional
            iterable of requested feeds.
            The default is {'sectionals', 'points'}.
        filter : RaceMetadata, optional
            filter instance to use. The default is None.
        update_before : datetime  # TODO
            update all records matching filter where getmtime is before the 
            given datetime.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is True, and will always be overridden to be True.
        """
        if start_date is None:
            start_date = datetime.now(tz = timezone.utc) - timedelta(days = 14)
        elif type(start_date) == str:
            start_date = dateutil.parser.parse(start_date)
        if start_date.tzinfo is None or start_date.tzinfo.utcoffset(start_date) is None:
            start_date = start_date.replace(tzinfo = timezone.utc)
        if end_date is None:
            end_date = datetime.now(tz = timezone.utc) - timedelta(days = 1)
        elif type(end_date) == str:
            end_date = dateutil.parser.parse(end_date)
        if end_date.tzinfo is None or end_date.tzinfo.utcoffset(end_date) is None:
            end_date = end_date.replace(tzinfo = timezone.utc)
        
        if filter is None:
            filter = RaceMetadata()
            filter.set_filter(published = True)
        
        sharecodes = self.get_racelist_range(
            start_date = start_date,
            end_date = end_date,
            **kwargs
            )
        kwargs["no_return"] = True
        _ = self.get_data(
            sharecodes = sharecodes,
            request = request,
            filter = filter,
            **kwargs
            )
    
    def load_all_sectionals(self,
                            start_date: datetime = None,
                            end_date: datetime = None,
                            filter: RaceMetadata = None,
                            to_csv: bool = True,
                            compression: str = None,
                            fname: str = None,
                            **kwargs
                            ) -> dict:
        """
        load all sectionals.

        Parameters
        ----------
        start_date : datetime, optional
            lower date boundary. The default is None.
        end_date : datetime, optional
            upper date boundary. The default is None.
        filter : RaceMetadata, optional
            filter instance to use. The default is None.
        to_csv : bool, optional
            DESCRIPTION. The default is True.
        compression : str, optional
            compression type to use. The default is None.
        fname : str, optional
            filename to use if to_csv is True.
            The default is None.
        
        **params
        new : bool, optional
            whether to force download a new file.
            The default is False.
        offline : bool, optional
            whether to treat request without internet connection.
            The default is False.
        no_return : bool, optional
            return None from target funcs, save memory when just updating files.
            The default is False.

        Returns
        -------
        dict
        """
        sharecodes = self.get_racelist_range(
            start_date = start_date,
            end_date = end_date,
            **kwargs
            )
        if filter is None:
            filter = RaceMetadata()
            filter.set_filter(published = True)
        filter.apply_filter(data = sharecodes) # apply filter in place
        # return keys from filter._list, post filtered
        sharecodes = {sc: filter.get(sc) for sc in filter}
        sects = self.get_data(
            sharecodes = sharecodes,
            request = {'sectionals'}
            ).get('sectionals')
        if to_csv:
            sectionals = []
            for s in sects.values():
                sectionals.extend(s)
            export_sectionals_to_csv(
                sectionals = sectionals,
                fname = fname,
                compression = compression
                )
        else:
            for sc in sects:
                sharecodes[sc]['sectionals'] = reformat_sectionals_list(sects[sc])
            data = export_sectionals_to_xls(sharecodes)
            return data


# TODO to be completed when the API is ready
class TPDFeed(GmaxFeed):
    """
    adds derivative feeds from tpd.zone, par lines, expected finish times etc
    """
    def __init__(self,
                 licence:str = None, 
                 racelist_path:str = None, 
                 sectionals_path:str = None, 
                 gps_path:str = None, 
                 route_path:str = None,
                 sectionals_history_path:str = None,
                 sectionals_raw_path:str = None,
                 jumps_path:str = None,
                 tpd_user:str = None,
                 tpd_passwd:str = None,
                 par_path:str = None,
                 expected_times_path:str = None) -> None:
        super().__init__(
            licence = licence,
            racelist_path = racelist_path,
            sectionals_path = sectionals_path,
            gps_path = gps_path,
            route_path = route_path,
            sectionals_history_path = sectionals_history_path,
            sectionals_raw_path = sectionals_raw_path,
            jumps_path = jumps_path
            )
        self.set_tpd_auth(tpd_user = tpd_user, tpd_passwd = tpd_passwd)
        self.set_sectionals_history_path(path = sectionals_history_path)
        self.set_sectionals_raw_path(path = sectionals_raw_path)
        self.set_jumps_path(path = jumps_path)
        if not self:
            logger.warning(
                'No licence key set by GmaxFeed - pass licence = "my_licence" to constructor, or set GMAXLICENCE="my_licence" as environment variable'
                )
        
    def __bool__(self) -> bool:
        return self.licence is not None and self.get_tpd_auth() is not None
    
    def __repr__(self) -> str:
        return "< TPDFeed, valid:{0} >".format(bool(self))
    
    def set_tpd_auth(self, tpd_user:str, tpd_passwd:str) -> None:
        if "TPD_USER" not in os.environ and tpd_user is not None:
            os.environ["TPD_USER"] = tpd_user
        if "TPD_PASSWD" not in os.environ and tpd_passwd is not None:
            os.environ["TPD_PASSWD"] = tpd_passwd
    
    def get_tpd_auth(self) -> tuple:
        # requests takes arg auth=(user, passwd)
        return (os.environ.get("TPD_USER"), os.environ.get("TPD_PASSWD"))
    
    def set_race_pars_path(self, path: str = None) -> None:
        """
        pars for each sharecode condiering ground, class, age. delivered as arrays of,
        p - Progress, distance from finish line
        xy - longitude and latitiude coords
        v - velocity
        sf, sl - stride frequency and length
        b - bearing, clockwise angle in radians from North
        
        all are based on the timeline, eg, P(t), V(t), SF(t)..
        easiest strategy to line up with race is by replacing t array with 
        timestamps based on offtime indicated in progress feed,
        offtime = progress['T'] - progress['R']. then par['t'] = [offtime + i for i in par['t']].
        
        this strategy is easy to implement but has issue around startline where
        a fraction of a second either way makes a large difference to the position
        on on the timeline. Alternative more robust method would be to identify
        the timestamp at which the leader is RaceLength-50m from the finish, and
        match that with the index in the pars at which the par lines are also
        RaceLength-50m from the finish and use that timestamp as the reference
        point.
        """
        self._race_pars = path or os.environ.get('RACE_PARS_PATH') or 'race_pars'
        self._confirm_exists(self._race_pars)
    
    def set_average_times_path(self, path: str = None) -> None:
        self._average_times = path or os.environ.get('AVE_TIMES_PATH') or 'average_times'
        self._confirm_exists(self._average_times)


def update(start_date: datetime or str = None,
           end_date: datetime or str = None,
           request: set = {'sectionals', 'points'},
           filter: RaceMetadata = None,
           **kwargs
           ) -> None:
    """
    update filesystem of gmax data from start_date to end_date.

    Parameters
    ----------
    start_date : datetime or str, optional
        lower date. The default is None.
    end_date : datetime or str, optional
        upper date. The default is None.
    request : set, optional
        iterable of feeds to update. The default is {'sectionals', 'points'}.
    filter : RaceMetadata, optional
        GmaxRacetype filter to use. The default is None.
    """
    gmax_feed = GmaxFeed()
    gmax_feed.update(
        start_date = start_date,
        end_date = end_date,
        request = request,
        filter = filter,
        **kwargs
        )
