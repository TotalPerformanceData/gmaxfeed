#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 11:48:33 2019

Aesthetic class for requesting data from the gmax API.
RaceMetadata can be used for filtering the sharecodes
Keep up to date using update function, either by running gmaxfeed/main_update.py or scheduling update() in other applications.
Don't run this as main as imports don't work then.

Gmax Licence can be hard coded in GmaxFeed in line 90, or more preferably set as an environment variable either by placing 
inserting lines of definitions after the imports as indicated on lines 34 to 43 (commented out) or more preferably still by 
setting persistent environment variables;

setting env variable:
OSX:
    in the terminal, use export GMAXLICENCE=my_licence
    permanently add to source by adding a line to bash_profile:
    nano ~/.bash_profile
    then scroll to bottom and add line,
    export GMAXLICENCE=my_licence
    type "source ~/.bash_profile" to enable. should be saved for future terminal sessions.
Linux:
    as above, but the "~/.bash_profile" is instead "/.bashrc"
Windows:
    the command is "set" instead of "export", and to set persistently for future cmd sessions I think you just need to enter,
    setx GMAXLICENCE my_licence

@author: george swindells
@email: george.swindells@totalperformancedata.com
"""

import os
import dateutil
import json

_dir = os.path.abspath(os.path.dirname(__file__))
_par_dir, _ = os.path.split(_dir)

from .Utils import (listdir2,
                    read_json,
                    reformat_sectionals_list,
                    export_sectionals_to_xls,
                    read_url,
                    load_file,
                    process_url_response,
                    apply_thread_pool)
from datetime import datetime, timedelta, timezone
from datetime import date as date_

"""
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

from loguru import logger
logger.add(os.path.join(_par_dir, 'logs', 'postrace_feeds.log'), level='INFO', format="{time} {level} {message}")


class RaceMetadata:
    """
    house metadata about the races, and filter for given countries, courses, published status etc if applicable
    can define without data, and call set_filter() to apply to something later, such as in gmax_feed.get_data()
    """
    def __init__(self, data:dict = None, direc:str = None):
        self.clear()
        self.import_data(data = data, direc = direc)
        self._filter = {}
    
    def __iter__(self) -> str:
        """
        iterate the values which passed the filter parameters, returning tuple of (key, value).
        Be careful not to make changes to the items in the dict as the keys in 
        self._list and self._data actually point to the same place 
        """
        yield from self._list.keys()
    
    def __len__(self) -> int:
        return len(self._list)
    
    def __repr__(self) -> str:
        return "< RaceMetadata - Races:{0} >".format(len(self._data))
    
    def set_filter(self,
                   countries:list or set = None,
                   courses:list or set = None,
                   course_codes:list or set = None,
                   published:bool = None,
                   start_date:datetime or str = None,
                   end_date:datetime or str = None,
                   race_types:list or set = None,
                   opts:dict = {}) -> None:
        """
        set the internal filter using the named values or a dict of named values

        Parameters
        ----------
        countries : list or set, optional
            country codes to be included, such as GB, US, CA, FR. The default is None.
        courses : list or set, optional
            course names to be included, such as 'Wolverhampton', 'Newcastle'. The default is None.
        course_codes : list or set, optional
            gmax courses codes to be included, such as '04', '14', '53'. The default is None.
        published : bool, optional
            whether to included only published, or only unpublished races. The default is None meaning no filter applied.
        start_date : datetime or str, optional
            lower date boundary. The default is None.
        end_date : datetime or str, optional
            upper date boundary. The default is None.
        race_types : list or set, optional
            Gmax RaceTypes to include in the filter. The default is None.
        opts : dict, optional
            DESCRIPTION. The default is {}.

        Raises
        ------
        Exception
            error if unexpected key given to options.
        """
        self._filter = {'countries':countries,
                        'courses':courses,
                        'course_codes':course_codes,
                        'published':published,
                        'start_date':start_date,
                        'end_date':end_date,
                        'race_types':race_types}
        for key in opts:
            if key in self._filter:
                if key != 'published' and type(opts[key]) not in [list, set, dict]:
                    opts[key] = [opts[key]]
                self._filter[key] = opts[key]
            else:
                raise Exception("key {0} given to RaceMetadata instance.set_filter(), not recognised as valid option".format(key))
    
    def get(self, sharecode:str) -> dict or None:
        return self._data.get(sharecode)
    
    def clear(self) -> None:
        self._data = {}
        self._list = self._data
    
    def import_data(self, data:list or dict = None, direc:str = None) -> None:
        """
        add the races in data to self._data, takes either list of dicts, or dict mapping each sharecode -> race_metadata
        if data is None and a directory is passed instead (path to racelist folder) this contents of the folder are iterated and imported
        can be called multiple times, for instance if you run it in the morning to gether all metadata in one place and then
        want to add metadata for new races that have appeared in the gmax racelist later that day.
        """
        if data is None:
            if direc is None:
                return
            files = listdir2(direc)
            for file in files:
                d = read_json(os.path.join(direc, file))
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
                countries:bool = True,
                courses:bool = True,
                course_codes:bool = True,
                race_types:bool = True) -> dict:
        """
        get a set of all possible values within self._data for the given fields
        and return as dictionary of sets. Useful for passing "everything except" 
        conditions to filter, eg, for all courses except Ascot Newcastle and Bath,
        obj.filter(courses = obj.get_set().get('courses') - {'Ascot', 'Newcastle', 'Bath'})
        """
        output = {}
        if countries:
            output['countries'] = set([sc.get('Country') for sc in self._data.values()])
        if courses:
            output['courses'] = set([sc.get('Racecourse') for sc in self._data.values()])
        if course_codes:
            output['course_codes'] = set([sc[:2] for sc in self._data]) # assumes first two chars are the course code, might change in later years
        if race_types:
            output['race_types'] = set([sc.get('RaceType') for sc in self._data.values()]) # assumes first two chars are the course code, might change in later years
        return output
    
    def apply_filter(self,
                     countries:list or set = None,
                     courses:list or set = None,
                     course_codes:list or set = None,
                     published:bool = None,
                     start_date:datetime or str = None,
                     end_date:datetime or str = None,
                     race_types:list or set = None) -> None:
        """
        filter the sharecodes within self._data by the given conditions
        passing sets will be much faster if giving long lists of options.
        courses = ['Ascot', 'Newcastle', 'Lingfield Park'] # if courses is not None, only include entry if the course of the record is in the given list or set
        course_codes = ['01', '35', '30']
        countries = ['US', 'GB'] 
        published = True # compare the race_data['Published'] field against the given published paramter and return if matches
        start_date = datetime(2020, 1, 1, tzinfo = timezone.utc) # must be timezone aware as compared to race_data['PostTime'] parsed field
        end_date = datetime(2020, 1, 1, tzinfo = timezone.utc)
        """
        countries = countries or self._filter.get('countries')
        courses = courses or self._filter.get('courses')
        course_codes = course_codes or self._filter.get('course_codes')
        race_types = race_types or self._filter.get('race_types')
        published = published or self._filter.get('published')
        start_date = start_date or self._filter.get('start_date')
        end_date = end_date or self._filter.get('end_date')
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

    
def _apply_filter(sharecodes:dict, filter:RaceMetadata) -> None:
    """
    when filtering data using the RaceMetadata class, set the filters using filter.set_filter(),
    and then pass the sharecodes and RaceMetadata object to this.
    sharecodes must be a dicts of all the metadata if passing to this func
    """
    filter.import_data(data = sharecodes)
    filter.apply_filter()


class GmaxFeed:
    """
    instantiate with licence key so don't have to keep placing into function calls.
    if licence key not passed on instantiation checks for a licence key in env called 'GMAXLICENCE'
    """
    def __init__(self, 
                 licence:str = None,
                 fixtures_path:str = None,
                 racelist_path:str = None, 
                 sectionals_path:str = None, 
                 gps_path:str = None, 
                 route_path:str = None,
                 sectionals_history_path:str = None,
                 sectionals_raw_path:str = None,
                 jumps_path:str = None,
                 performance_path:str = None) -> None:
        self.set_licence(licence = licence)
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
            logger.warning('No licence key set by GmaxFeed - pass licence = "my_licence" to constructor, or set GMAXLICENCE="my_licence" as environment variable')
        
    def __bool__(self) -> bool:
        return self.get_licence() is not None
    
    def __repr__(self) -> str:
        return "< GmaxFeed, valid:{0}>".format(bool(self))
    
    def _confirm_exists(self, path:str) -> bool:
        if not os.path.exists(path):
            os.mkdir(path)
    
    def set_fixtures_path(self, path:str = None) -> None:
        self._fixtures_path = path or os.environ.get('FIXTURES_PATH') or 'fixtures'
        self._confirm_exists(self._fixtures_path)
    
    def set_racelist_path(self, path:str=None) -> None:
        self._racelist_path = path or os.environ.get('RACELIST_PATH') or 'racelist'
        self._confirm_exists(self._racelist_path)
    
    def set_sectionals_path(self, path:str=None) -> None:
        self._sectionals_path = path or os.environ.get('SEC_PATH') or 'sectionals'
        self._confirm_exists(self._sectionals_path)
    
    def set_gps_path(self, path:str=None) -> None:
        self._gps_path = path or os.environ.get('GPS_PATH') or 'gpsData'
        self._confirm_exists(self._gps_path)
    
    def set_route_path(self, path:str=None) -> None:
        self._route_path = path or os.environ.get('ROUTE_PATH') or 'routes'
        self._confirm_exists(self._route_path)
    
    def set_sectionals_history_path(self, path:str=None) -> None:
        self._sectionals_history_path = path or os.environ.get('SEC_HIST_PATH') or 'sectionals-hist'
        self._confirm_exists(self._sectionals_history_path)
    
    def set_sectionals_raw_path(self, path:str=None) -> None:
        self._sectionals_raw_path = path or os.environ.get('SEC_RAW_PATH') or 'sectionals-raw'
        self._confirm_exists(self._sectionals_raw_path)
    
    def set_jumps_path(self, path:str=None) -> None:
        self._jumps_path = path or os.environ.get('JUMPS_PATH') or 'jumps'
        self._confirm_exists(self._jumps_path)
    
    def set_tracker_performance_path(self, path:str=None) -> None:
        self._errors_path = path or os.environ.get('PERFORMANCE_PATH') or 'tracker-errors'
        self._confirm_exists(self._errors_path)
    
    def set_licence(self, licence:str=None) -> None:
        if "GMAXLICENCE" not in os.environ and licence is not None:
            os.environ["GMAXLICENCE"] = licence
        
    def get_licence(self) -> str or None:
        return os.environ.get("GMAXLICENCE")
    
    def get_fixtures(self, date: str = None, new: bool = False, offline: bool = False) -> list or False:
        """
        fetch fixtures for the given date (or datetime.today() if date = None)
        from the gmax /fixtures feed, return next 7 days of fixtures from date given

        Parameters
        ----------
        self date : str
            date for which to fetch upcoming fixtures.
        new : bool, optional
            whether to ignore the cached file and fetch new. The default is False.
        offline : bool, optional
            whether to only use cached files. The default is False.

        Returns
        -------
        list or False
            list of fixtures or False.
        """
        data = []
        if date is None:
            date = datetime.utcnow()
        elif type(date) is str:
            date = dateutil.parser.parse(date)
        elif type(date) is date_:
            date = datetime.combine(date, datetime.min.time())
        date_str = date.strftime('%Y-%m-%d')
        path = os.path.join(self._fixtures_path, date_str)
        if os.path.exists(path) and not new:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            limit_date = date + timedelta(days = 6)
            if (not new and mtime > limit_date) or offline:
                data = load_file(direc = self._fixtures_path, fname = date_str)
                if data is not None:
                    return data
        # if data is None file doesn't exist, try downloading a new file if offline is False
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/fixtures.ashx?DateLocal={0}&k={1}'.format(date_str, self.get_licence())
            # returns a list of 1 dict or empty list - process manually here as don't want to cache just one race
            txt = read_url(url)
            if txt:
                data = json.loads(txt)
        return data
    
    def get_race(self, sharecode: str, date: str or datetime = None, new:bool = False, offline:bool = False) -> dict or False:
        """
        fetch the TPD/Gmax RaceList data for just the given sharecode.
        If 'date' is given that's used as the date to fetch from cache in 
        offline == new == True mode, else parsed from within the sharecode.

        Parameters
        ----------
        sharecode : str
            The sharecode for which to get all racelist details.
        date : str, optional
            the date for the sharecode to aid cache search. The default is None.
        new : bool, optional
            if a new version is to be fetched. The default is False.
        offline : bool, optional
            if in offline mode and only to use cache. The default is False.

        Returns
        -------
        dict
            The record for the sharecode, or False if not found.
        """
        data = {}
        if date is None:
            date = datetime.strptime(sharecode[2:10], '%Y%m%d')
        elif type(date) is str:
            date = dateutil.parser.parse(date)
        elif type(date) is date_:
            date = datetime.combine(date, datetime.min.time())
        date_str = date.strftime('%Y-%m-%d')
        path = os.path.join(self._racelist_path, date_str)
        if os.path.exists(path) and not new:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            limit_date = date + timedelta(days = 6)
            if (not new and mtime > limit_date) or offline:
                data = load_file(direc = self._racelist_path, fname = date_str)
                if data is not None:
                    return data.get(sharecode) or False
        # if data is None file doesn't exist, try downloading a new file if offline is False
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/racelist.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns a list of 1 dict or empty list - process manually here as don't want to cache just one race
            txt = read_url(url)
            if txt:
                data = {row['I']:row for row in json.loads(txt)}
        return data.get(sharecode) or False
    
    def get_racelist(self, date:str or datetime = None, new:bool = False, offline:bool = False, sharecode:str = None) -> dict:
        """
        datestr format is '%Y-%m-%d'
        sometimes may want to query the metadata for a specific race, like when populating jumps data and checking the race is NH
        for this case can leave date as None and pass a sharecode, the date is then inferred from the sharecode assuming sc[2:10] = %Y%m%d.
        """
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
            url = 'https://www.gmaxequine.com/TPD/client/racelist.ashx?DateLocal={0}&k={1}'.format(date, self.get_licence())
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._racelist_path, fname = date, version = 2)
        if sharecode is not None:
            return data.get(sharecode) or False
        else:
            return data
    
    def get_racelist_range(self, start_date:datetime or str = None, end_date:datetime or str = None, new:bool = False, offline:bool = False) -> dict:
        if start_date is None:
            start_date = datetime(2016,1,1)
        if type(start_date) is str:
            start_date = dateutil.parser.parse(start_date)
        if end_date is None:
            end_date = datetime.utcnow()
        if type(end_date) is str:
            end_date = dateutil.parser.parse(end_date)
        if end_date < start_date:
            end_date = start_date
        end_date += timedelta(days=1) # to include last date in range
        range_ = (end_date - start_date).days
        dates = [start_date + timedelta(days=dt) for dt in range(0, range_, 1)]
        result = apply_thread_pool(self.get_racelist, dates, new = new, offline = offline)
        data = {}
        for row in result:
            if row:
                data.update(row)
        return data
    
    def get_points(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        # return of {'sc':sharecode, 'data':data} for benefit of multithreading to easier group runners in same races
        data = None
        if not new:
            data = load_file(direc = self._gps_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/points.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns rows of dicts delimited by newline characters, r"\r\n", readlines() issue blank final element of list
            data = process_url_response(url = url, direc = self._gps_path, fname = sharecode, version = 3)
        return {'sc':sharecode, 'data':data}
    
    def get_sectionals(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        data = None
        if not new:
            data = load_file(direc = self._sectionals_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._sectionals_path, fname = sharecode, version = 1)
        return {'sc':sharecode, 'data':data}
    
    def get_sectionals_history(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        data = None
        if not new:
            data = load_file(direc = self._sectionals_history_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals-history.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._sectionals_history_path, fname = sharecode, version = 1)
        return {'sc':sharecode, 'data':data}
    
    def get_sectionals_raw(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        # internal use only
        licence = os.environ.get('ALTLICENCE')
        data = None
        if licence is None:
            return {'sc':sharecode, 'data':None}
        if not new:
            data = load_file(direc = self._sectionals_raw_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/sectionals-raw.ashx?Sharecode={0}&k={1}'.format(sharecode, licence)
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._sectionals_raw_path, fname = sharecode, version = 1)
        return {'sc':sharecode, 'data':data}
    
    def get_tracker_performance(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        # internal use only
        data = None
        if not new:
            data = load_file(direc = self._errors_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/performance.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._errors_path, fname = sharecode, version = 1)
        return {'sc':sharecode, 'data':data}
    
    def get_obstacles(self, sharecode:str, new:bool = False, offline:bool = False) -> dict:
        metadata = self.get_race(sharecode = sharecode, offline = offline)
        if not metadata or 'RaceType' not in metadata or not any([x in metadata.get('RaceType') for x in ['Hurdle', 'Chase', 'NH Flat']]):
            return {'sc':sharecode, 'data':None}
        data = None
        if not new:
            data = load_file(direc = self._jumps_path, fname = sharecode)
            if data is not None:
                return {'sc':sharecode, 'data':data}
        if not offline:
            url = 'https://www.gmaxequine.com/TPD/client/jumps.ashx?Sharecode={0}&k={1}'.format(sharecode, self.get_licence())
            # returns a list of dicts
            data = process_url_response(url = url, direc = self._jumps_path, fname = sharecode, version = 1)
        return {'sc':sharecode, 'data':data}
    
    def get_route(self, course_codes:str or int = None, new:bool = False, offline:bool = False) -> str:
        # return dict of cc->string in kml format. Can be parsed easily using a lib like beautiful soup 4. Course file not available returns pointless msg "please check later"
        output = {}
        if course_codes is None:
            course_codes = [1,3,4,6,11,12,14,17,19,23,24,30,35,37,40,43,46,47,53,57,58,59,61,64,71,72,73,74,75,76,77,78,79,80,81,82,83,84,90,91]
        elif type(course_codes) in [str, int]:
            course_codes = [course_codes]
        course_codes = [str(i).zfill(2) for i in course_codes]
        for cc in course_codes:
            fname = 'Racecourse-{0}.kml'.format(cc)
            if not new:
                data = load_file(direc = self._route_path, fname = fname)
                if data is not None:
                    output[cc] = data
                    continue
            if not offline:
                url = 'https://www.gmaxequine.com/TPD/client/routes.ashx?Racecourse={0}&k={1}'.format(cc, self.get_licence())
                # returns a kml format text file
                output[cc] = process_url_response(url = url, direc = self._route_path, fname = fname, version = 4)
        return output
    
    def get_data(self, sharecodes:dict or list, request:set = {'sectionals', 'sectionals-raw', 'sectionals-history', 'points', 'obstacles'}, new:bool = False, offline:bool = False, filter:RaceMetadata = None) -> dict:
        """
        pass dict of racelist data sc -> metadata. if list is passed instead assumed to be raceids and won't be filtered. 
        multithreaded entry point for getting big selection of data, downloading new if not present, else using cached version
        """
        if type(sharecodes) is dict:
            if filter is None:
                filter = RaceMetadata()
                filter.set_filter(published = True) # think i forced this for the purpose of not requesting sectional and points for unpublished races, but sec-raw, obstacles and perf would still be valid
            _apply_filter(sharecodes = sharecodes, filter = filter) # in place
            sharecodes = list(filter) # return keys from filter._list, post filtered
        elif not sharecodes:
            return {}
        output = {}
        if 'sectionals' in request:
            output['sectionals'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_sectionals, sharecodes, new = new, offline = offline) if row['data']}
        if 'sectionals-raw' in request:
            output['sectionals-raw'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_sectionals_raw, sharecodes, new = new, offline = offline) if row['data']}
        if 'sectionals-history' in request:
            output['sectionals-history'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_sectionals_history, sharecodes, new = new, offline = offline) if row['data']}
        if 'points' in request:
            output['points'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_points, sharecodes, new = new, offline = offline) if row['data']}
        if 'obstacles' in request:
            output['obstacles'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_obstacles, sharecodes, new = new, offline = offline) if row['data']}
        if 'performance' in request:
            output['performance'] = {row['sc']:row['data'] for row in apply_thread_pool(self.get_tracker_performance, sharecodes, new = new, offline = offline) if row['data']}
        return output
    
    def update(self, start_date:datetime or str = None, end_date:datetime or str = None, request:set = {'sectionals', 'points'}, new:bool = False, offline:bool = False, filter:RaceMetadata = None) -> None:
        """
        update all the cached file in daterange given, only refresh if new passed. racelists are always freshed if file mtime is less than a week after the date it refers
        if licence key is only activated for one of the above feeds then make sure to pass only the request set you want, else unauthorsied feed/s or will have folder full of empty text files
        """
        sharecodes = self.get_racelist_range(start_date = start_date, end_date = end_date, new = new, offline = offline)
        _ = self.get_data(sharecodes = sharecodes, request = request, new = new, offline = offline, filter = filter)
    
    def load_all_sectionals(self, start_date:datetime = None, end_date:datetime = None, offline:bool = False, filter:RaceMetadata = None) -> dict:
        sharecodes = self.get_racelist_range(start_date = start_date, end_date = end_date, offline = offline)
        if filter is None:
            filter = RaceMetadata()
            filter.set_filter(published = True)
        _apply_filter(sharecodes = sharecodes, filter = filter) # in place
        sharecodes = {sc:filter.get(sc) for sc in filter} # return keys from filter._list, post filtered
        sects = self.get_data(sharecodes = sharecodes, request = {'sectionals'}).get('sectionals')
        for sc in sects:
            sharecodes[sc]['sectionals'] = reformat_sectionals_list(sects[sc])
        data = export_sectionals_to_xls(sharecodes)
        return data

# TODO to be completed
class TPDFeed(GmaxFeed):
    """
    adds derivative feeds from https://www.tpd-viewer.com, par lines, expected finish times etc
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
        super().__init__(licence = licence, racelist_path = racelist_path, sectionals_path = sectionals_path, gps_path = gps_path, route_path = route_path, sectionals_history_path = sectionals_history_path, sectionals_raw_path = sectionals_raw_path, jumps_path = jumps_path)
        self.set_tpd_auth(tpd_user = tpd_user, tpd_passwd = tpd_passwd)
        self.set_sectionals_history_path(path = sectionals_history_path)
        self.set_sectionals_raw_path(path = sectionals_raw_path)
        self.set_jumps_path(path = jumps_path)
        if not self:
            logger.warning('No licence key set by GmaxFeed - pass licence = "my_licence" to constructor, or set GMAXLICENCE="my_licence" as environment variable')
        
    def __bool__(self) -> bool:
        return self.get_licence() is not None and self.get_tpd_auth() is not None
    
    def __repr__(self) -> str:
        return "< TPDFeed, valid:{0}>".format(bool(self))
    
    def set_tpd_auth(self, tpd_user:str, tpd_passwd:str) -> None:
        if "TPD_USER" not in os.environ and tpd_user is not None:
            os.environ["TPD_USER"] = tpd_user
        if "TPD_PASSWD" not in os.environ and tpd_passwd is not None:
            os.environ["TPD_PASSWD"] = tpd_passwd
    
    def get_tpd_auth(self) -> tuple:
        # requests takes arg auth=(user, passwd)
        return (os.environ.get("TPD_USER"), os.environ.get("TPD_PASSWD"))
    
    def set_race_pars_path(self, path:str=None) -> None:
        """
        pars for each sharecode condiering ground, class, age. delivered as arrays of,
        p - Progress, distance from finish line
        xy - longitude and latitiude coords
        v - velocity
        sf, sl - stride frequency and length
        b - bearing, clockwise angle in radians from North
        all are based on the timeline, eg, P(t), V(t), SF(t)..
        easiest strategy to line up with race is by replacing t array with timestamps based on offtime indicated in progress feed, offtime = progress['T'] - progress['R']. then par['t'] = [offtime + i for i in par['t']].
        this strategy is easy to implement but has issue around startline where a fraction of a second either way makes a large difference to the position on on the timeline. Alternative more robust method would be to identify the timestamp
        at which the leader is RaceLength-50m from the finish, and match that with the index in the pars at which the par lines are also RaceLength-50m from the finish and use that timestamp as the reference point.
        """
        self._race_pars = path or os.environ.get('RACE_PARS_PATH') or 'race_pars'
        self._confirm_exists(self._race_pars)
    
    def set_average_times_path(self, path:str=None) -> None:
        self._average_times = path or os.environ.get('AVE_TIMES_PATH') or 'average_times'
        self._confirm_exists(self._average_times)


def update(start_date:datetime or str = None, end_date:datetime or str = None, request:set = {'sectionals', 'points'}, filter:RaceMetadata = None) -> None:
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
    gmax_feed = GmaxFeed()
    gmax_feed.update(start_date = start_date, end_date = end_date, request = request, filter = filter)
    
    
