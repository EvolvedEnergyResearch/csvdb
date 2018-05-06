#
# Created 19 Sep 2017 (energyPATHWAYS version)
# Updated 27 Mar 2018 (csvdb version)
# @author: Rich Plevin
#
# Database abstraction layer. Abstracted from energyPATHWAYS' database.py
#
# This module and table.py provide 3 classes for interacting with the CSV data: CsvDatabase,
# CsvTable, and CsvMetadata. CsvDatabase takes a pathname to a directory with a ".csvdb"
# extension, and loads all the .csv and .csv.gz files found there.
#
# CsvDatabase.__init__ optionally loads all CSV data files into CsvTable instances.
#
# The generated classes use the cached data stored in the Csv* classes to initialize the classes
# in schema.py, which is generated by csvdb/bin/genClasses.py. The classes in schema.py subclass
# directly from DataObject in data_object.py. This superclass contains revised versions of many functions
# from datamapfunctions.py, modified to operate on string keys rather than integer ids.
#
from __future__ import print_function
import os
import pandas as pd

from .error import CsvdbException
from .foreign_key import ForeignKey
from .table import CsvTable

pd.set_option('display.width', 200)


class CsvMetadata(object):
    __slots__ = ['table_name', 'key_col', 'attr_cols', 'df_cols', 'df_key_col', 'drop_cols']

    def __init__(self, table_name, key_col=None, attr_cols=None,
                 df_cols=None, df_key_col=None, drop_cols=None):
        """
        A simple struct to house table metadata. Attribute columns (`attr_cols`)
        become instance variables in generated classes. Dataframe columns (`df_cols`)
        for rows matching `key_cols` (which must be a subset of `attr_cols`) are
        stored in a dataframes linked from an instance of the generated class.
        Defaults: `key_col` = 'name'; `attr_cols` = all columns not specified in
        `df_cols` or `drop_cols`; df_cols = []; `drop_cols` = [].
        """
        self.table_name = table_name
        self.key_col    = key_col or 'name'
        self.df_key_col = df_key_col
        self.df_cols    = df_cols or []
        self.drop_cols  = drop_cols or []
        self.attr_cols  = attr_cols or []   # if None, all cols minus (df_cols + drop_cols) are assumed

class CsvDatabase(object):
    """
    A database class that caches table data and provides a few fetch methods.
    """
    file_map = {}   # maps table names => file names under the database root folder
    instances = {}  # maps normalized database pathname to CsvDatabase instances

    def __init__(self, pathname=None, load=True, metadata=None,
                 # Deprecated: given explicit metadata, can probably drop these arguments
                 tables_to_not_load=None, tables_without_classes=None, tables_to_ignore=None,
                 data_tables=None, tables_to_load_on_demand=None):
        """
        Initialize a CsvDatabase.

        :param pathname: (str) the location of the directory containing the CSV files to process
        :param load: (bool) if True, cache the CSV data on initialization
        :param metadata: (dict with keys = table name strings; values = CsvMetadata instances).
        :param tables_to_not_load: (list of str) names of tables to exclude from loading (only relevant if load is True)
        :param tables_without_classes: (list of str) names of table for which classes should not be instantiated.
           By default, all CSV files found in the database directory are assumed to require class instances
           for each row of data.
        :param tables_to_ignore: (list of str) basenames of CSV files that should simply be ignored.
        :param data_tables: (list of str) names of tables that should be treated as "data" tables.
        """
        self.pathname = pathname

        metadata = metadata or []
        self.metadata = {md.table_name : md for md in metadata}     # convert the list to a dict

        self.table_objs  = {}        # dict of table instances keyed by name
        self.table_names = {}        # all known table names
        self.text_maps   = {}        # dict by table name of dicts by id of text mapping tables

        self.tables_without_classes = (tables_without_classes or []) + ['foreign_keys']
        self.tables_to_ignore = tables_to_ignore or []

        self.data_tables = data_tables or []
        self.tables_to_load_on_demand = tables_to_load_on_demand or []

        tables_to_not_load = tables_to_not_load or []

        self.create_file_map()
        self._cache_foreign_keys()

        # cache data for all tables for which there are generated classes
        if load:
            table_names = self.tables_with_classes()
            for name in table_names:
                if name not in tables_to_not_load:
                    self.get_table(name)

    @classmethod
    def get_database(cls, pathname, **kwargs):
        """
        Return the singleton CsvDatabase instance for the given pathname.

        :param pathname: (str) the path to the CSV database directory.
        :param kwargs: On first call, accepts arguments to the CsvDatabase
           __init__ method. Subsequent calls return the cached instance,
           so kwargs are ignored.
        :return: (CsvDatabase instance)
        """
        instances = CsvDatabase.instances

        # Special case: if pathname is None and there is only one item
        # in the instances dict, return that item. Saves having to pass
        # the database path repeatedly if there's only one database in use.
        if pathname is None and len(instances) == 1:
            values = instances.values()
            return values[0]

        pathname = os.path.normpath(pathname)

        try:
            return instances[pathname]

        except KeyError:
            instances[pathname] = instance = cls(pathname, **kwargs)
            instance.table_names = {name: True for name in instance.get_table_names()}
            return instance

    def table_metadata(self, table_name):
        return self.metadata.get(table_name, CsvMetadata(table_name))

    def is_table(self, name):
        return self.table_names.get(name, False)

    def get_table(self, name):
        try:
            return self.table_objs[name]

        except KeyError:
            metadata = self.metadata.get(name, CsvMetadata(name))
            tbl = CsvTable(self, name, metadata)
            self.table_objs[name] = tbl
            return tbl

    # Deprecated?
    def tables_with_classes(self, include_on_demand=False):
        exclude = self.tables_without_classes

        # Don't create classes for excluded tables; these are rendered as DataFrames only
        tables = [name for name in self.get_table_names() if name not in exclude]
        ignore = self.tables_to_ignore + (self.tables_to_load_on_demand if not include_on_demand else [])
        result = sorted(list(set(tables) - set(ignore)))
        return result

    def get_row_from_table(self, name, key_col, key, scenario=None, raise_error=True):
        tbl = self.get_table(name)
        tup = tbl.get_row(key_col, key, scenario=scenario, raise_error=raise_error)
        return tup

    def get_rows_from_table(self, name, key_col, key, scenario=None, raise_error=True):
        tbl = self.get_table(name)
        tups = tbl.get_row(key_col, key, scenario=scenario, allow_multiple=True, raise_error=raise_error)
        return tups

    def get_table_names(self):
        return self.file_map.keys()

    def get_columns(self, table):
        tbl = self.get_table(table)
        if tbl:
            return tbl.get_columns()

        # Otherwise, return the column headers from the file
        pathname = self.file_for_table(table)
        with open(pathname, 'r') as f:
            headers = f.readline().strip()
        result = headers.split(',')
        return result

    @staticmethod
    def find_col(table_name, exceptions, candidates, cols):
        col = exceptions.get(table_name)
        if col:
            return col

        for col in candidates:
            if col in cols:
                return col
        return None

    def get_key_col(self, table_name):
        md = self.table_metadata(table_name)
        return md.key_col

    def _cache_foreign_keys(self):
        """
        The CSV database reads the foreign key data that was exported from postgres.
        """
        pathname = self.file_for_table('foreign_keys')
        df = pd.read_csv(pathname, index_col=None)
        for _, row in df.iterrows():
            tbl_name, col_name, for_tbl_name, for_col_name = tuple(row)
            ForeignKey(tbl_name, col_name, for_tbl_name, for_col_name)

    def create_file_map(self):
        pathname = self.pathname

        if not os.path.exists(pathname):
            raise CsvdbException('Database path "{}" does not exist'.format(pathname))

        if not os.path.isdir(pathname):
            raise CsvdbException('Database path "{}" is not a directory'.format(pathname))

        for dirpath, dirnames, filenames in os.walk(pathname, topdown=False):
            for filename in filenames:
                basename = os.path.basename(filename)
                if (basename.endswith('.csv') or basename.endswith('.gz')):
                    tbl_name = basename.split('.')[0]
                    self.file_map[tbl_name] = os.path.abspath(os.path.join(dirpath, filename))

        print("Found {} .CSV files for {}".format(len(self.file_map), pathname))


    def file_for_table(self, tbl_name):
        return self.file_map.get(tbl_name)
