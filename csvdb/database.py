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
from glob import glob
import gzip
import os
import pandas as pd
import re
import pdb
from .error import CsvdbException
from .table import CsvTable, REF_SCENARIO, SENSITIVITY_COL

pd.set_option('display.width', 200)

# case-insensitive match of *.csv and *.csv.gz files
CSV_PATTERN = re.compile('.*\.csv(\.gz)?$', re.IGNORECASE)

# case-insensitive match of *.gz files
ZIP_PATTERN = re.compile('.*\.gz$', re.IGNORECASE)

class CsvMetadata(object):
    __slots__ = ['table_name', 'data_table', 'key_col', 'attr_cols',
                 'df_cols', 'df_value_col', 'df_filters', 'drop_cols', 'lowcase_cols']

    def __init__(self, table_name, data_table=False, key_col=None, attr_cols=None,
                 df_cols=None, df_value_col=None, df_filters=None, drop_cols=None,
                 lowcase_cols=None):
        """
        A simple struct to house table metadata. Attribute columns (`attr_cols`)
        become instance variables in generated classes. Dataframe columns (`df_cols`)
        for rows matching `key_cols` (which must be a subset of `attr_cols`) are
        stored in a DataFrames linked from an instance of the generated class.
        Defaults: `key_col` = 'name'; `attr_cols` = all columns not specified in
        `df_cols` or `drop_cols`; df_cols = []; `drop_cols` = [].
        """
        self.table_name = table_name
        self.data_table = data_table

        if data_table:
            # ignore all other parameters, if any, to constructor
            self.key_col = None
            self.df_filters = self.df_cols = self.attr_cols = self.drop_cols = self.lowcase_cols = []
            self.df_value_col = []
        else:
            self.key_col      = key_col or 'name'
            self.df_filters   = df_filters or []
            self.df_cols      = df_cols or []
            self.df_value_col = df_value_col or ['value']
            self.drop_cols    = drop_cols or []
            self.attr_cols    = attr_cols or []   # if None, all cols minus (df_cols + drop_cols) are assumed

        key_col = self.key_col
        self.lowcase_cols = set((lowcase_cols or []) + ([key_col] if key_col else []))

SHAPE_DIR = 'ShapeData'

class ShapeDataMgr(object):
    """
    Handles the special case of the pre-sliced ShapesData
    """
    def __init__(self, db_path, compile_sensitivities):
        self.db_path = db_path
        self.tbl_name = SHAPE_DIR   # Deprecated?
        self.slices = {}            # maps shape name to DF containing that shape's data rows
        self.file_map = self.create_filemap(db_path)
        self.compile_sensitivities = compile_sensitivities

    def load_all(self):
        if self.slices:
            return self.slices

        for shape_name, filename in self.file_map.iteritems():
            openFunc = gzip.open if re.match(ZIP_PATTERN, filename) else open
            with openFunc(filename, 'rb') as f:
                print("Reading shape data for {}".format(shape_name))
                df = pd.read_csv(f, index_col=None)
                if SENSITIVITY_COL in df.columns:
                    df[SENSITIVITY_COL] = df[SENSITIVITY_COL].fillna(REF_SCENARIO)
                if self.compile_sensitivities:
                    if SENSITIVITY_COL in df.columns:
                        df = df[SENSITIVITY_COL].to_frame().drop_duplicates()
                        df['name'] = shape_name
                    else:
                        df = None
                self.slices[shape_name] = df

    @classmethod
    def create_filemap(cls, db_path):
        """
        ShapeData is stored in gzipped slices of original 3.5 GB table.
        The files are in a "{db_name}.db/ShapeData/{shape_name}.csv.gz"
        This is a classmethod so it can be called by clean.py to generate
        CsvMetadata instances before creating the CsvDatabase.
        """
        shape_dir = os.path.join(db_path, SHAPE_DIR)
        shape_files_zip = glob(os.path.join(shape_dir, '*.csv.gz'))
        shape_files_csv = glob(os.path.join(shape_dir, '*.csv'))

        file_map = {}
        for filename in shape_files_zip + shape_files_csv:
            basename = os.path.basename(filename)
            shape_name = basename.split('.')[0]
            file_map[shape_name] = filename

        return file_map

    def get_slice(self, name):
        if not self.slices:
            self.load_all()

        #name = name.replace(' ', '_')
        return self.slices[name]

class CsvDatabase(object):
    """
    A database class that caches table data and provides a few fetch methods.
    """
    instances = {}  # maps normalized database pathname to CsvDatabase instances

    def __init__(self, pathname=None, load=True, metadata=None, mapped_cols=None,
                 tables_to_not_load=None, tables_without_classes=None, tables_to_ignore=None, output_tables=False,
                 compile_sensitivities=False):
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
        self.output_tables = output_tables
        self.compile_sensitivities = compile_sensitivities
        self.mapped_cols = mapped_cols
        # maps table names => file names under the database root folder
        self.file_map = {}

        metadata = metadata or []
        self.metadata = {md.table_name : md for md in metadata}     # convert the list to a dict
        self.table_objs  = {}        # dict of table instances keyed by name
        self.table_names = {}        # all known table names
        self.text_maps   = {}        # dict by table name of dicts by id of text mapping tables

        self.tables_without_classes = tables_without_classes or []
        self.tables_to_ignore = tables_to_ignore or []

        tables_to_not_load = tables_to_not_load or []

        self.create_file_map()
        self.shapes = ShapeDataMgr(pathname, compile_sensitivities)

        # cache data for all tables for which there are generated classes
        if load:
            table_names = self.tables_with_classes()
            for name in table_names:
                if name not in tables_to_not_load:
                    self.get_table(name)

    @classmethod
    def clear_cached_database(cls):
        CsvDatabase.instances = {}

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
            tbl = CsvTable(self, name, metadata, self.output_tables, self.compile_sensitivities, mapped_cols=self.mapped_cols)
            self.table_objs[name] = tbl
            return tbl

    def tables_with_classes(self, include_on_demand=False):
        exclude = self.tables_without_classes

        # Don't create classes for excluded tables; these are rendered as DataFrames only
        tables = [name for name in self.get_table_names() if name not in exclude]
        data_tables = [name for name, md in self.metadata.items() if md.data_table]

        ignore = data_tables + self.tables_to_ignore
        result = sorted(list(set(tables) - set(ignore)))
        return result

    def get_row_from_table(self, name, key_col, key, scenario=None, raise_error=True, **filters):
        tbl = self.get_table(name)
        tup = tbl.get_row(key_col, key, scenario=scenario, raise_error=raise_error, **filters)
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

    def create_file_map(self):
        pathname = self.pathname

        if not os.path.exists(pathname):
            raise CsvdbException('Database path "{}" does not exist'.format(pathname))

        if not os.path.isdir(pathname):
            raise CsvdbException('Database path "{}" is not a directory'.format(pathname))

        for dirpath, dirnames, filenames in os.walk(pathname, topdown=False):
            if os.path.basename(dirpath) == SHAPE_DIR:
                continue

            for filename in filenames:
                basename = os.path.basename(filename)
                if re.match(CSV_PATTERN, basename):
                    tbl_name = basename.split('.')[0]
                    self.file_map[tbl_name] = os.path.abspath(os.path.join(dirpath, filename))

        print("Found {} .CSV files for {}".format(len(self.file_map), pathname))


    def file_for_table(self, tbl_name):
        return self.file_map.get(tbl_name) or self.shapes.file_map.get(tbl_name)
