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
from collections import OrderedDict
import csv
import gzip
import os
import pandas as pd
import re
from .error import CsvdbException, ValidationFormatError
from .table import CsvTable, REF_SCENARIO, SENSITIVITY_COL
import pdb

pd.set_option('display.width', 200)

# case-insensitive match of *.csv and *.csv.gz files
CSV_PATTERN = re.compile('.*\.csv(\.gz)?$', re.IGNORECASE)
CSV_DIR_PATTERN = '.csvd'

SPACES_PATTERN = re.compile('\s\s+')

# case-insensitive match of *.gz files
ZIP_PATTERN = re.compile('.*\.gz$', re.IGNORECASE)

def getResource(pkg_name, rel_path):
    """
    Extract a resource (e.g., file) from the given relative path in
    the named package.

    :param pkg_name: (str) the name of the package to read from
    :param rel_path: (str) a path relative to the package
    :return: the file contents
    """
    import pkgutil

    contents = pkgutil.get_data(pkg_name, rel_path)
    return contents.decode('utf-8')

def resourceStream(pkg_name, rel_path):
    """
    Return a stream on the resource found on the given path relative
    to package pkg_name.

    :param pkg_name: (str) the name of the package to read from
    :param rel_path: (str) a path relative to the given package
    :return: (file-like stream) a file-like buffer opened on the desired resource.
    """
    import io

    text = getResource(pkg_name, rel_path)
    return io.BytesIO(str(text))

class CsvMetadata(object):
    __slots__ = ['table_name', 'data_table', 'key_col', 'has_key_col', 'attr_cols',
                 'df_cols', 'df_value_col', 'df_filters', 'drop_cols', 'lowcase_cols']

    def __init__(self, table_name, data_table=False, key_col=None, has_key_col=True,
                 attr_cols=None, df_cols=None, df_value_col=None, df_filters=None,
                 drop_cols=None, lowcase_cols=None):
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
            self.has_key_col = False
            self.df_filters = self.df_cols = self.attr_cols = self.drop_cols = self.lowcase_cols = []
            self.df_value_col = []
        else:
            self.key_col      = key_col or ('name' if has_key_col else None)
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
        self.file_map = self.create_file_map(db_path)
        self.compile_sensitivities = compile_sensitivities

    def load_all(self, verbose=True):
        if self.slices:
            return self.slices

        for shape_name, filename in self.file_map.items():
            if type(filename) is not list:
                filename = [filename]
            dfs = []
            for fn in filename:
                openFunc = gzip.open if re.match(ZIP_PATTERN, fn) else open
                with openFunc(fn, 'rb') as f:
                    if verbose:
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
                    dfs.append(df)
            self.slices[shape_name] = None if all([df is None for df in dfs]) else pd.concat(dfs)

    @classmethod
    def create_file_map(cls, db_path):
        """
        ShapeData is stored in gzipped slices of original 3.5 GB table.
        The files are in a "{db_name}.self/ShapeData/{shape_name}.csv.gz"
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

        # csv directory files get appended together when they are read in
        shape_csv_dirs = glob(os.path.join(shape_dir, '*.csvd'))

        for shape_csv_dir in shape_csv_dirs:
            shape_files_zip = glob(os.path.join(shape_dir, shape_csv_dir, '*.csv.gz'))
            shape_files_csv = glob(os.path.join(shape_dir, shape_csv_dir, '*.csv'))
            basename = os.path.basename(shape_csv_dir)
            shape_name = basename.split('.')[0]
            if len(shape_files_zip + shape_files_csv):
                file_map[shape_name] = shape_files_zip + shape_files_csv

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
                 tables_to_not_load=None, tables_without_classes=None, tables_to_ignore=None,
                 output_tables=False, compile_sensitivities=False, filter_columns=None):
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
        :param output_tables: (bool)
        :param compile_sensitivities: (bool)
        :param filter_columns: (list of str)
        """
        self.pathname = pathname
        self.output_tables = output_tables
        self.compile_sensitivities = compile_sensitivities
        self.mapped_cols = mapped_cols
        # maps table names => file names under the database root folder
        self.file_map = {}
        self.filter_columns = filter_columns or []
        self.val_dict = None    # stored when first read

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
                    self.get_table(name,filter_columns=self.filter_columns)

    @classmethod
    def clear_cached_database(cls):
        CsvDatabase.instances = {}

    @classmethod
    def get_database(cls, pathname, **kwargs):
        """
        Return the singleton CsvDatabase instance for the given pathname.

        :rtype:
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

    def get_table(self, name, filter_columns=None):
        try:
            return self.table_objs[name]
        except KeyError:
            metadata = self.metadata.get(name, CsvMetadata(name))
            tbl = CsvTable(self, name, metadata, self.output_tables, self.compile_sensitivities, mapped_cols=self.mapped_cols, filter_columns=filter_columns)
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
            if SHAPE_DIR in dirpath:
                continue

            if os.path.basename(dirpath)[-5:] == CSV_DIR_PATTERN:
                # we have a directory that should be treated like one csv file
                tbl_name = os.path.basename(dirpath).split('.')[0]
                self.file_map[tbl_name] = [os.path.abspath(os.path.join(dirpath, filename)) for filename in filenames]
            else:
                for filename in filenames:
                    basename = os.path.basename(filename)
                    if re.match(CSV_PATTERN, basename):
                        tbl_name = basename.split('.')[0]
                        self.file_map[tbl_name] = os.path.abspath(os.path.join(dirpath, filename))


    def file_for_table(self, tbl_name):
        return self.file_map.get(tbl_name) or self.shapes.file_map.get(tbl_name)

    @staticmethod
    def check_value_list(series, values):
        import types

        bad = []

        if len(series) == 0:
            return None

        values = [val.lower() if val and isinstance(val, types.StringTypes) else val for val in values]

        for val in series.unique():
            test_value = val.lower() if val and isinstance(val, types.StringTypes) else val
            if test_value not in values:
                bad += [(i, val) for i in series[series == test_value].index]

        return bad

    def check_table(self, tbl_name, val_dict, data=None, check_unique=True):
        """
        Check whether the CsvDatabase tables (CSV files) are clean.

        :param tbl_name: (str) the name of the table to check
        :param val_dict: (OrderedDict of OrderedDicts) keyed by (table, column) tuple,
            holding dictionaries of validation info defined in validation.csv
        :param data: (pandas.DataFrame) the data to check (e.g., before saving a CSV file).
           If None, the data in the existing table is checked.
        :param check_unique: (bool) whether to check that all keys are unique per table
        :return: (list of str) return an empty list if the table is "clean" else a list of error messages
        """
        errors = []

        tbl = self.get_table(tbl_name)
        data = data or tbl.data

        if filter(lambda name: name.startswith('Unnamed: '), data.columns):
            errors.append("Table {} has a 'Unnamed' column".format(tbl_name))
            return errors

        if len(data) == 0:
            # print("Skipping empty table", tbl_name)
            return errors

        if check_unique:
            md = self.table_metadata(tbl_name)

            # use key column plus any df_cols to check for uniqueness
            key_cols = ([md.key_col] if md.has_key_col else []) + md.df_cols

            if key_cols:
                # extract key columns as tuples to check for uniqueness
                combo_keys = [tup for tup in data[key_cols].itertuples(name=None, index=False)]
                if len(combo_keys) != len(set(combo_keys)):
                    errors.append("Duplicate keys found in table {} for df_cols {}".format(tbl_name, key_cols))

        for colname in data.columns:

            # Prefer the more specific (table, column) over generic column spec
            val_info = val_dict.get((tbl_name, colname)) or val_dict.get(('', colname))

            if val_info:
                series = data[colname]
                values = val_info.values
                type_func = val_info.type_func
                bad = []

                # If there are explicit values, check against them
                if values:
                    bad = self.check_value_list(series, values)

                # Otherwise, if there's a type-checking function, call that
                elif type_func:
                    bad = type_func(series, not val_info.not_null)

                if bad:
                    if values and len(values) > 5:
                        values = values[:2] + ["..."] + values[-2:]

                    errors.append("Errors in {}.{}:".format(tbl_name, colname))
                    for i, value in bad:
                        if values:
                            errors.append("    Value '{}' at line {} not found in allowable list {}".format(
                                value, i + 2, values))  # +1 for header; +1 to translate 0 offset
                        elif type_func:
                            errors.append("    Value '{}' at line {} failed data type check with function {}".format(
                                value, i + 2, type_func.__name__))

        return errors

    def check_tables(self, val_dict, check_unique=True, print_msgs=True):
        """
        Check whether the CsvDatabase tables (CSV files) are clean.

        :param val_dict: (OrderedDict of OrderedDicts) keyed by (table, column) tuple,
            holding dictionaries of validation info defined in validation.csv
        :param check_unique: (bool) whether to check that all keys are unique per table
        :return: List of error messages, or empty list if the tables are "clean"
        """
        errmsgs = []

        # the shapes.file_map is an empty dict when --no-shapes specified
        tbl_names = self.file_map.keys() + self.shapes.file_map.keys()

        for tblname in tbl_names:

            if tblname == 'GEOGRAPHIES':  # TODO: generalize this
                continue

            errs = self.check_table(tblname, val_dict, check_unique=check_unique)
            if errs:
                errmsgs += errs

        # show all error messages
        if print_msgs:
            for e in errmsgs:
                print(e)

        return errmsgs

    def list_tables(self, skip_dirs):
        tables = []

        for dirpath, dirnames, filenames in os.walk(self.pathname, topdown=False):
            if skip_dirs and skip_dirs in dirpath:
                continue

            for filename in filenames:
                if re.match(CSV_PATTERN, filename):
                    basename = os.path.basename(filename)
                    tbl_name = basename.split('.')[0]
                    tables.append(tbl_name)

        return tables

    def clean_tables(self, update, skip_dirs=None, trim_blanks=True,
                     drop_empty_rows=True, drop_empty_cols=True):
        """
        Fix common errors in CSV files, according the the keyword args given.
        Options include trim_blanks => remove blanks surrounding column names
        and data values; drop_empty_cols => drop columns with empty names and
        values; drop_empty_rows => drop rows with all col values empty. By
        default, all options are True.

        :param self: (CsvDatabase) a CsvDatabase instance
        :param update: (bool) whether to write modifications back to the
            table (CSV) file
        :param skip_dirs: (list of str) directories to skip when cleaning
        :param trim_blanks: (bool) whether to trim blanks surrounding column
            names and data values.
        :param drop_empty_rows: (bool) whether to drop empty rows.
        :param drop_empty_cols: (bool) whether to drop empty columns.
        :return: (bool) whether any errors where found and fixed.
        """
        counts = {'empty_rows': 0, 'empty_cols': 0, 'trimmed_blanks': 0}

        any_modified = False

        for tblname in self.list_tables(skip_dirs):
            modified = False
            pathname = self.file_map[tblname]

            data = []
            openFunc = gzip.open if pathname.endswith('.gz') else open
            with openFunc(pathname, 'rb') as infile:
                csvreader = csv.reader(infile, delimiter=',')
                for row in csvreader:
                    data.append(row)

            rows, columns = len(data), len(data[0])
            # print("Table: {} ({:,} rows, {} cols)".format(tblname, rows, columns))

            if drop_empty_rows:
                # any rows that are all blanks get dropped
                data = [row for row in data if not all([val == '' for val in row])]
                if len(data) < rows:
                    counts['empty_rows'] += 1
                    modified = True

            if drop_empty_cols:
                # transpose list to make it easy to check the columns for blanks
                data = map(list, zip(*data))

                # any cols (transposed to rows) that are all blanks get dropped
                data = [row for row in data if not all([val == '' for val in row])]
                if len(data) < columns:
                    counts['empty_cols'] += 1
                    modified = True

                # transpose back to the original data shape
                data = map(list, zip(*data))

            if trim_blanks:
                # replace any strings that have extra spaces
                for row_num, row in enumerate(data):
                    for col_num, val in enumerate(row):
                        stripped = re.sub(SPACES_PATTERN, ' ', val.strip())
                        if val != stripped:
                            counts['trimmed_blanks'] += 1
                            data[row_num][col_num] = stripped
                            modified = True

            if modified:
                any_modified = True
                if update:
                    print("   Writing", self.file_map[tblname])
                    openFunc = gzip.open if pathname.endswith('.gz') else open
                    with openFunc(pathname, 'wb') as outfile:
                        csvwriter = csv.writer(outfile, delimiter=',')
                        for row in data:
                            csvwriter.writerow(row)
        if any_modified:
            def report(msg, key):
                value = counts[key]
                if value:
                    print('  ', msg.format(value))

            report("Removed empty rows from {} tables", 'empty_rows')
            report("Removed empty cols from {} tables", 'empty_cols')
            report("Trimmed blanks from {} values", 'trimmed_blanks')

            if update:
                print("Database errors found and fixed - files have been modified")
            else:
                print("Database errors found but update=False - no files were modified")
        else:
            print("Finished cleaning common self errors with no issues found")

    def validate(self, pkg_name, update, trim_blanks=True, check_unique=True,
                 drop_empty_rows=True, drop_empty_cols=True):
        dbdir = self.pathname

        # Add shape tables to metadata
        shapes_file_map = ShapeDataMgr.create_file_map(dbdir)
        metadata = self.metadata
        for tbl_name in shapes_file_map.keys():
            metadata[tbl_name] = CsvMetadata(tbl_name, data_table=True)

        print("Cleaning common errors in csvdb table files")
        self.clean_tables(update, skip_dirs='ShapeData', trim_blanks=trim_blanks,
                          drop_empty_rows=drop_empty_rows, drop_empty_cols=drop_empty_cols)

        print("\nChecking self integrity")
        # Assume all shape tables are "data tables", i.e., no "name" column is expected
        self.shapes.load_all(verbose=False)

        val_dict = self.read_validation_csv(pkg_name)

        msgs = self.check_tables(val_dict, check_unique=check_unique)
        if not msgs:
            print("Database is clean")

        CsvDatabase.clear_cached_database()

    def read_validation_csv(self, pkg_name, use_cache=True):
        """
        Read and parse {package_dir}/etc/validation.csv.
        If use_cache is True and the validation dict has been cached,
        return it. Otherwise, read it from the designated package, and
        if use_cache is True, store it in the CsvDatabase instance.
        """
        from .check import ValidationInfo

        if use_cache and self.val_dict:
            return self.val_dict

        extra_inputs = 'additional_valid_inputs'
        col_names = ['table_name', 'column_name', 'not_null', 'linked_column', 'dtype', 'folder',
                     'referenced_table', 'referenced_field', 'cascade_delete', extra_inputs]
        col_set = set(col_names)

        f = resourceStream(pkg_name, 'etc/validation.csv')
        rows = [row for row in csv.reader(f)]

        # column names
        names = [name for name in rows[0] if (name and not name.startswith('_c_'))]  # drop empty/generic col names
        count = len(names)

        name_set = set(names)
        if name_set != col_set:
            if name_set - col_set:
                raise ValidationFormatError('Unknown validation columns: {}'.format(name_set - col_set))

            if col_set - name_set:
                raise ValidationFormatError('Missing validation columns: {}'.format(col_set - name_set))

        # Store data keyed by tuple of (table, column), where table may be '' in some cases
        val_dict = OrderedDict()

        for row in rows[1:]:    # skip column names
            (table_name, column_name, not_null, linked_column, dtype, folder,
             referenced_table, referenced_field, cascade_delete, extra) = row[:count]

            # convert "additional inputs" col into a list of strings of all non-empty values
            # from trailing, unnamed or generic (_c_NN) columns. If initial value is '', convert
            # to an empty list so value is always a list.
            extra_values = ([extra] + [value for value in row[count:] if value != '']) if extra else []

            obj = ValidationInfo(self, table_name, column_name, not_null, linked_column, dtype, folder,
                                 referenced_table, referenced_field, cascade_delete, extra_values)

            key = (table_name, column_name)
            val_dict[key] = obj

        if use_cache:
            self.val_dict = val_dict

        return val_dict

    # This function is part of the API used by Excel GUI
    def save_table(self, pkg_name, tbl_name, df, subdir=None, validate=True, use_cache=True):
        """
        Save a dataframe as a database table.

        :param pkg_name: (str) the name of the package to read from
        :param tbl_name: (str) the name of the table to write the CSV file
        :param df: (pandas.DataFrame) the data to write
        :param subdir: (str or None) path relative to dbdir in which to write the CSV file
        :param validate: (bool) whether to validate the data before writing
        :param use_cache: (bool) whether to reuse previously read validation data, if available.
        :return: a list of error strings, or an empty list if no errors.
        """
        val_dict = self.read_validation_csv(pkg_name, use_cache=use_cache)
        errors = self.check_table(tbl_name, val_dict, data=df, check_unique=True) if validate else None

        if not errors:
            pathname = os.path.join(self.pathname, subdir or '', tbl_name + '.csv')
            df.to_csv(pathname, index=None)

        return errors
