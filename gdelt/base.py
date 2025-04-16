#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author:
# Linwood Creekmore
# Email: valinvescap@gmail.com


##################################
# Standard library imports
##################################

import datetime
import json
import multiprocessing.pool
import os
import re
from functools import partial
from multiprocessing import Pool, cpu_count
import concurrent

##################################
# Third party imports
##################################

import numpy as np
import pandas as pd
import requests

##################################
# Local imports
##################################
from gdelt.dateFuncs import (_dateRanger, _gdeltRangeString)
from gdelt.getHeaders import _events1Heads, _events2Heads, _mentionsHeads, \
    _gkgHeads
from gdelt.helpers import _cameos, _tableinfo
from gdelt.inputChecks import (_date_input_check)
from gdelt.parallel import _mp_worker
from gdelt.vectorizingFuncs import _urlBuilder, _geofilter


##################################
# Third party imports
##################################


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    @property
    def _get_daemon(self):  # pragma: no cover
        return False

    def _set_daemon(self, value):  # pragma: no cover
        pass

    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class NoDaemonProcessPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


##############################################
#  Admin to load local files
##############################################


this_dir, this_filename = os.path.split(__file__)
BASE_DIR = os.path.dirname(this_dir)

UTIL_FILES_PATH = os.path.join(BASE_DIR, "gdeltPyR", "utils", "schema_csvs")

try:

    codes = pd.read_json(os.path.join(BASE_DIR, 'data', 'cameoCodes.json'),
                         dtype=dict(cameoCode='str', GoldsteinScale=np.float64))
    codes.set_index('cameoCode', drop=False, inplace=True)

except:  # pragma: no cover
    a = 'https://raw.githubusercontent.com/linwoodc3/gdeltPyR/master' \
        '/utils/' \
        'schema_csvs/cameoCodes.json'
    codes = json.loads((requests.get(a).content.decode('utf-8')))

##############################
# Core GDELT class
##############################


class gdelt(object):
    """GDELT Object
        Read more in the :ref:`User Guide <k_means>`.

        Attributes
        ----------
        gdelt2url : string,default:  https://api.gdeltproject.org/api/v2/
            Base url for GDELT 2.0 services.
        cores : int, optional, default: system-generated
            Count of total CPU cores available.
        pool: function
            Standard multiprocessing function to establish Pool workers
        proxies: dict
            Dictionary containing proxy information for the requests module.
            For details on how to set, see
            http://docs.python-requests.org/en/master/user/advanced/#proxies
            Example:
                >>>proxies = {'http': 'http://10.10.1.10:3128',\
                'https': 'http://10.10.1.10:1080'}
                >>> requests.get('http://example.org', proxies=proxies)
                Or with a password or specific schema
                >>>proxies = {'http': 'http://user:pass@10.10.1.10:3128/'}
                >>>proxies = {'http://10.20.1.128': 'http://10.10.1.10:5323'}


        # TODO add abiity to pick custom time windows

        Examples
        --------
        >>> from gdelt
        >>> gd = gdelt.gdelt()
        >>> results = gd.Search(['2016 10 19'],table='events',coverage=True)
        >>> print(len(results))
        244767
        >>> print(results.columns)
        Index(['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
       'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
       'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
       'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code',
       'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
       'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
       'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent',
       'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
       'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
       'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
       'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat',
       'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type',
       'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code',
       'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
       'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName',
       'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code',
       'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED',
       'SOURCEURL'],
       dtype='object')


        Notes
        ------
        gdeltPyR retrieves Global Database of Events, Language, and Tone
        (GDELT) data (version 1.0 or version 2.0) via parallel HTTP GET
        requests and is an alternative to accessing GDELT
        data via Google BigQuery .

        Performance will vary based on the number of available cores
        (i.e. CPUs), internet connection speed, and available RAM. For
        systems with limited RAM, Later iterations of gdeltPyR will include
        an option to store the output directly to disc.
        """

    def __init__(self,
                 gdelt2url=' https://api.gdeltproject.org/api/v2/',
                 cores=cpu_count(),
                 proxies=None

                 ):

        self.codes = codes
        self.translation = None
        self.cores = cores
        self.proxies = proxies
        self.baseUrl = gdelt2url
  
        self.proxies = proxies
        if proxies:
            if isinstance(proxies, dict):
                self.proxies = proxies
            else:
                raise TypeError("The proxies parameter must be a dictionary. "
                                "See http://docs.python-requests.org/en/master/"
                                "user/advanced/#proxies for more information.")


    ###############################
    # Searcher function for GDELT
    ###############################

    def Search(self,
               date,
               table='events',
               coverage=False,
               translation=False,
               output=None,
               queryTime=datetime.datetime.now().strftime('%m-%d-%Y %H:%M:%S'),
               normcols=False
               ):
        """Core searcher method to set parameters for GDELT data searches

        Keyword arguments
        ----------
        date : str, required
            The string representation of a datetime (single) or date
            range (list of strings) that is (are) the targeted timelines to
            pull GDELT data.

        table : string,{'events','gkg','mentions'}
           
        coverage : bool, default: False
            When set to 'True', gdeltPyR will pull back every 15 minute interval in the day (
            full results) or, if pulling for the current day, pull all 15
            minute intervals up to the most recent 15 minute interval of the
            current our.  For example, if the current date is 22 August,
            2016 and the current time is 0828 HRs Eastern, our pull would
            get pull every 15 minute interval in the day up to 0815HRs.
            When coverate is set to true and a date range is entered,
            we pull every 15 minute interval for historical days and up to
            the most recent 15 minute interval for the current day, if that
            day is included.
            
        translation : bool, default: False
            Whether or not to pull the translation database available from
            version 2 of GDELT. If translation is True, the translated set
            is downloaded, if set to False the english set is downloaded. 

        queryTime : datetime object, system generated
            This records the system time when gdeltPyR's query was executed,
            which can be used for logging purposes.

        output : string, {None,'df','gpd','shp','shapefile', 'json', 'geojson'
                'r','geodataframe'}
            Select the output format for the returned GDELT data

            Options
            -------

            json - Javascript Object Notation output; returns list of
            dictionaries in Python or a list of json objects

            csv- Outputs a CSV format; all dates and columns are joined
            
            shp- Writes an ESRI shapefile to current directory or path; output
            is filtered to exclude rows with no latitude or longitude
            
            geojson- 
            
            geodataframe- Returns a geodataframe; output is filtered to exclude
            rows with no latitude or longitude.  This output can be manipulated
            for geoprocessing/geospatial operations such as reprojecting the 
            coordinates, creating a thematic map (choropleth map), merging with
            other geospatial objects, etc.  See http://geopandas.org/ for info.

        normcols : bool
            Applies a generic lambda function to normalize GDELT columns 
            for compatibility with SQL or Shapefile outputs.  
        Examples
        --------

        >>> gd = gdelt.gdelt()
        >>> results = gd.Search(['2016 Oct 10'], table='gkg')
        >>> print(len(results))
        2398
        >>> print(results.V2Persons.iloc[2])
        Juanita Broaddrick,1202;Monica Lewinsky,1612;Donald Trump,12;Donald
        Trump,244;Wolf Blitzer,1728;Lucianne Goldberg,3712;Linda Tripp,3692;
        Bill Clinton,47;Bill Clinton,382;Bill Clinton,563;Bill Clinton,657;Bill
         Clinton,730;Bill Clinton,1280;Bill Clinton,2896;Bill Clinton,3259;Bill
          Clinton,4142;Bill Clinton,4176;Bill Clinton,4342;Ken Starr,2352;Ken
          Starr,2621;Howard Stern,626;Howard Stern,4286;Robin Quivers,4622;
          Paula Jones,3187;Paula Jones,3808;Gennifer Flowers,1594;Neil Cavuto,
          3362;Alicia Machado,1700;Hillary Clinton,294;Hillary Clinton,538;
          Hillary Clinton,808;Hillary Clinton,1802;Hillary Clinton,2303;Hillary
           Clinton,4226
        >>> results = gd.Search(['2016 Oct 10'], table='gkg',output='r')

        Notes
        ------
        Read more about GDELT data at http://gdeltproject.org/data.html

        gdeltPyR retrieves Global Database of Events, Language, and Tone
        (GDELT) data (version 1.0 or version 2.0) via parallel HTTP GET
        requests and is an alternative to accessing GDELT
        data via Google BigQuery.

        Performance will vary based on the number of available cores
        (i.e. CPUs), internet connection speed, and available RAM. For
        systems with limited RAM, Later iterations of gdeltPyR will include
        an option to store the output directly to disc.

        """

        # check for valid table names; fail early
        valid = ['events', 'gkg', 'vgkg', 'iatv', 'mentions']
        if table not in valid:
            raise ValueError('You entered "{}"; this is not a valid table name.'
                             ' Choose from "events", "mentions", or "gkg".'
                .format(table))

        _date_input_check(date)
        self.coverage = coverage
        self.date = date
        baseUrl = self.baseUrl
        self.queryTime = queryTime
        self.table = table
        self.translation = translation
        self.datesString = _gdeltRangeString(_dateRanger(self.date),
                                            coverage=self.coverage)


        #################################
        # R dataframe check; fail early
        #################################
        if output == 'r':  # pragma: no cover
            try:
                import feather

            except ImportError:
                raise ImportError(('You need to install `feather` in order '
                                   'to output data as an R dataframe. Keep '
                                   'in mind the function will return a '
                                   'pandas dataframe but write the R '
                                   'dataframe to your current working '
                                   'directory as a `.feather` file.  Install '
                                   'by running\npip install feather\nor if '
                                   'you have Anaconda (preferred)\nconda '
                                   'install feather-format -c conda-forge\nTo '
                                   'learn more about the library visit https:/'
                                   '/github.com/wesm/feather'))

        ##################################
        # Partial Functions
        #################################

        v2RangerCoverage = partial(_gdeltRangeString,
                                   coverage=True)

        v2RangerNoCoverage = partial(_gdeltRangeString, 
                                     coverage=False)
        urlsv2mentions = partial(_urlBuilder, table='mentions', translation=self.translation)
        urlsv2events = partial(_urlBuilder,table='events', translation=self.translation)
        urlsv2gkg = partial(_urlBuilder, table='gkg', translation=self.translation)

        eventWork = partial(_mp_worker, table='events', proxies=self.proxies)
        codeCams = partial(_cameos, codes=codes)



        if self.table =='events':
            try:
                self.events_columns = \
                pd.read_csv(os.path.join(BASE_DIR, "data", 'events2.csv'))[
                    'name'].values.tolist()

            except:  # pragma: no cover
                self.events_columns = _events2Heads()

        elif self.table == 'mentions':
            try:
                self.mentions_columns = \
                    pd.read_csv(
                        os.path.join(BASE_DIR, "data", 'mentions.csv'))[
                        'name'].values.tolist()

            except:  # pragma: no cover
                self.mentions_columns = _mentionsHeads()
        else:
            try:
                self.gkg_columns = \
                    pd.read_csv(
                        os.path.join(BASE_DIR, "data", 'gkg2.csv'))[
                        'name'].values.tolist()

            except:  # pragma: no cover
                self.gkg_columns = _gkgHeads()



        if self.table == 'events' or self.table == '':
            columns = self.events_columns
            if self.coverage is True:  # pragma: no cover

                self.download_list = (urlsv2events(v2RangerCoverage(
                    _dateRanger(self.date))))
            else:

                self.download_list = (urlsv2events(v2RangerNoCoverage(
                    _dateRanger(self.date))))

        if self.table == 'gkg':
            columns = self.gkg_columns
            if self.coverage is True:  # pragma: no cover

                self.download_list = (urlsv2gkg(v2RangerCoverage(
                    _dateRanger(self.date))))
            else:
                self.download_list = (urlsv2gkg(v2RangerNoCoverage(
                    _dateRanger(self.date))))
                # print ("2 gkg", urlsv2gkg(self.datesString))

        if self.table == 'mentions':
            columns = self.mentions_columns
            if self.coverage is True:  # pragma: no cover

                self.download_list = (urlsv2mentions(v2RangerCoverage(
                    _dateRanger(self.date))))

            else:

                self.download_list = (urlsv2mentions(v2RangerNoCoverage(
                    _dateRanger(self.date))))



        if isinstance(self.datesString, str):
            if self.table == 'events':

                results = eventWork(self.download_list)
            else:
                # if self.table =='gkg':
                #     results = eventWork(self.download_list)
                #
                # else:
                results = _mp_worker(self.download_list, proxies=self.proxies)

        else:

            if self.table == 'events':

                pool = Pool(processes=cpu_count())
                downloaded_dfs = list(pool.imap_unordered(eventWork,
                                                          self.download_list))
                pool.close()
                pool.terminate()
                pool.join()
            else:

           
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    # Submit tasks to the executor
                    downloaded_dfs = list(executor.map(_mp_worker,self.download_list))


            # print(downloaded_dfs)
            results = pd.concat(downloaded_dfs)
            del downloaded_dfs
            results.reset_index(drop=True, inplace=True)


        # check for empty dataframe
        if results is not None:
            if len(results.columns) == 57:  # pragma: no cover
                results.columns = columns[:-1]

            else:
                results.columns = columns

        # if dataframe is empty, raise error
        elif results is None or len(results) == 0:  # pragma: no cover
            raise ValueError("This GDELT query returned no data. Check "
                             "query parameters and "
                             "retry")

        # Add column of human readable codes; need updated CAMEO
        if self.table == 'events':
            cameoDescripts = results.EventCode.apply(codeCams)

            results.insert(27, 'CAMEOCodeDescription',
                           value=cameoDescripts.values)

        ###############################################
        # Setting the output options
        ###############################################

        # dataframe output
        if output == 'df':
            self.final = results

        # json output
        elif output == 'json':
            self.final = results.to_json(orient='records')

        # csv output
        elif output == 'csv':
            self.final = results.to_csv(encoding='utf-8')

        # geopandas dataframe output
        elif output == 'gpd' or output == 'geodataframe' or output == 'geoframe':
            self.final = _geofilter(results)
            self.final = self.final[self.final.geometry.notnull()]


        else:
            self.final = results

        #########################
        # Return the result
        #########################

        # normalized columns
        if normcols:
            self.final.columns = list(map(lambda x: (x.replace('_', "")).lower(), self.final.columns))

        return self.final


    def schema(self,tablename):
        """

        Parameters
        ----------
        :param tablename: str
            Name of table to retrieve desired schema

        Returns
        -------
        :return: dataframe
            pandas dataframe with schema
        """

        return _tableinfo(table=tablename)  # pragma: no cover
