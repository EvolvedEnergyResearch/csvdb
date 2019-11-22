#!/usr/bin/env python
from __future__ import print_function
from glob import glob
import gzip
import os
import types
import re
import csv
from .database import CsvDatabase, CsvMetadata, ShapeDataMgr
from .error import CsvdbException, ValidationDataError, ValidationFormatError
import pdb

VALIDATION_FILE = 'VALIDATION.txt'

FILE_PATTERN   = re.compile('.*\.(csv|gz)$')
SPACES_PATTERN = re.compile('\s\s+')

_True  = ['t', 'y', 'true',  'yes', 'on']
_False = ['f', 'n', 'false', 'no',  'off']
_Bool  = _True + _False

def check_bool(series):
    bad = []

    for i, value in series.items():
        if isinstance(value, types.StringTypes) and value.lower() not in _Bool:
            bad.append((i, value))

    return bad

def check_bool_or_empty(series):
    bad = []

    for i, value in series.items():
        if isinstance(value, types.StringTypes) and value.lower() not in _Bool and value is not None:
            bad.append((i, value))

    return bad

def check_type(series, aType, allow_null=False):
    bad = []

    for i, value in series.items():
        if value is None and allow_null:
            continue
        try:
            aType(value)
        except:
            bad.append((i, value))

    return bad

def check_float(series):
    return check_type(series, float)

def check_int(series):
    return check_type(series, int)

def check_float_or_empty(series):
    return check_type(series, float, allow_null=True)

def check_int_or_empty(series):
    return check_type(series, int, allow_null=True)

_check_fns = {
    'int'   : check_int,
    'int_or_empty': check_int_or_empty,
    'float' : check_float,
    'float_or_empty': check_float_or_empty,
    'bool'  : check_bool,
    'bool_or_empty'  : check_bool_or_empty
}

def check_value_list(series, values):
    bad = []

    if len(series) == 0:
        return None

    values = [val.lower() if val and isinstance(val, types.StringTypes) else val for val in values]

    for val in series.unique():
        test_value = val.lower() if val and isinstance(val, types.StringTypes) else val
        if test_value not in values:
            bad += [(i, val) for i in series[series == test_value].index]

    return bad

def is_float(s):
    """
    Return whether the string value represents a float

    :param s: (str) a string
    :return: (bool) whether it looks like a float
    """
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_validation_file_path(validationdir):
    for dirpath, dirnames, filenames in os.walk(validationdir, topdown=False):
        if VALIDATION_FILE in filenames:
            return os.path.join(dirpath, VALIDATION_FILE)
    raise ValueError("Unable to find validation file {} within package {}".format(VALIDATION_FILE, validationdir))

class ValidationInfo(object):
    def __init__(self, db, row):
        self.col_type = row['type'].lower()
        self.type_func = _check_fns[self.col_type] if self.col_type else None
        self.target_name = row['table_name']
        self.target_col = row['table_column']
        self.folder = row['folder']
        self.ref_tbl = ref_tbl = row['referenced_table']
        self.ref_col = ref_col = row['referenced_field']
        extra_values = row['additional_valid_inputs']

        filename = ''  # place holder for location of validation.csv

        self.values = []

        if self.folder:
            pattern = os.path.join(db.pathname, self.folder, '*')
            paths = glob(pattern)
            self.values = map(extract_name, paths)

        elif ref_tbl and ref_col:
            try:
                tbl = db.get_table(ref_tbl)
            except CsvdbException:
                raise ValidationFormatError(filename, "unknown table '{}'".format(ref_tbl))

            if ref_col not in tbl.data.columns:
                raise ValidationFormatError(filename, "unknown column '{}' in table '{}'".format(ref_col, ref_tbl))

            self.values = list(tbl.data[ref_col].unique())

            # TODO: handle this in metadata?
            if ref_col == 'shape':
                self.values.append(None)

        if extra_values:
            # parse / validate numbers
            if all(map(str.isdigit, extra_values)):
                extra_values = map(int, extra_values)
            elif all(map(is_float, extra_values)):
                extra_values = map(float, extra_values)

            self.values += extra_values


def validation_dict(db, val_list):
    value_dict = {}     # maps (tbl, col) tuples to the ValidationInfo obj, checking for duplicates

    for row_dict in val_list:
        obj = ValidationInfo(db, row_dict)

        filename = '' # placeholder
        key = (obj.ref_table, obj.ref_col)

        # Check for duplicates
        if key in value_dict:
            key_str = '{}.{}'.format(key[0], key[1]) if key[0] else key[1]
            raise ValidationFormatError(filename, "duplicate entry for column '{}'".format(key_str))

        value_dict[key] = obj

    return value_dict

#
# TODO: Separate out the parsing of validation.txt from the processing of the data,
# TODO: since we're now reading this (and more) info from validation.csv, which is
# TODO: parsed and passed as a list of row dicts to validate_db().
#
# Deprecated
#
def read_metadata(db, validationdir):
    """
    Reads the CsvDatabase validation metadata from the file 'validation.txt' found
    at the top level of the CsvDatabase directory.

    :param db: (CsvDatabase) a CsvDatabase instance
    :return: (Dict) a dict keyed by column names or table.column names, with
        values being either a list of strings or a function to call on the column
        to validate the values therein.
    """
    filename = get_validation_file_path(validationdir)

    with open(filename, 'r') as f:
        lines = [(num, line.strip()) for num, line in enumerate(f) if not line.startswith('#')]

    known_types = _check_fns.keys()

    value_dict = {}     # maps col names to either a list of strings or a function to call on the column

    # helper function to check for duplicate entries
    def save_values(num, colname, values):
        if colname in value_dict:
            raise ValidationDataError(filename, num, "duplicate entry for column '{}'".format(colname))

        value_dict[colname] = values

    for num, line in lines:
        if line:        # ignore blank lines
            items = filter(lambda item: item != "", line.split(','))
            if len(items) < 2:
                raise ValidationDataError(filename, num, "expected at least 2 items, got '{}'".format(line))

            target_col = items.pop(0)   # may be just a column name or table.column

            if len(items) == 1:     # must be a type, a 'table.column' reference, or a 'directory/'
                value = items[0]
                aType = value.lower()

                # type check
                if aType in known_types:
                    save_values(num, target_col, _check_fns[aType]) # saves a function ref, not a list of values

                # compare with values in given table.column
                elif '.' in value:
                    parts = value.split('.')
                    if len(parts) != 2:
                        raise ValidationDataError(filename, num, "expected TABLE.COLUMN, got '{}'".format(value))

                    tblname, colname = parts

                    try:
                        tbl = db.get_table(tblname)
                    except CsvdbException:
                        raise ValidationDataError(filename, num, "unknown table '{}'".format(tblname))

                    if colname not in tbl.data.columns:
                        raise ValidationDataError(filename, num, "unknown column '{}' in table '{}'".format(colname, tblname))

                    values = list(tbl.data[colname].unique())

                    # TODO: special cases; handle in metadata rather than hard-coding?
                    if target_col == 'geography':
                        values.append('global')
                    elif target_col == 'gau':
                        values.append('all')
                    elif target_col == 'shape':
                        values.append(None)

                    save_values(num, target_col, values)

                # Final '/' => compare column values to files in this directory
                elif value.endswith('/'):
                    pattern = os.path.join(db.pathname, value, '*')
                    paths = glob(pattern)
                    values = map(extract_name, paths)

                    save_values(num, target_col, values)
            else:
                # treat numbers correctly
                if all(map(str.isdigit, items)):
                    items = map(int, items)
                elif all(map(is_float, items)):
                    items = map(float, items)

                save_values(num, target_col, items)

    return value_dict

def extract_name(path):
    """
    From, say, '/database/ShapeData/pv_utility_2-axis.csv.gz', return the
    basename of the file up to the first '.', i.e., 'pv_utility_2-axis'

    :param path: (str) a pathname
    :return: (str) return the basename of the path up to the first '.', if any.
    """
    basename = os.path.basename(path)
    parts = basename.split('.')
    return parts[0]

def check_tables(db, col_md):
    """
    Check whether the CsvDatabase tables (CSV files) are clean.

    :param db: (CsvDatabase) a CsvDatabase instance
    :param col_md: (Dict) values dictionary returned by read_metadata()
    :return: True if the tables are "clean" (i.e., no errors), False otherwise
    """
    isClean = True

    # the shapes.file_map is an empty dict when --no-shapes specified
    tbl_names = db.file_map.keys() + db.shapes.file_map.keys()

    for tblname in tbl_names:

        if tblname == 'GEOGRAPHIES':        # TODO: generalize this
            continue

        tbl = db.get_table(tblname)
        data = tbl.data

        if filter(lambda name: name.startswith('Unnamed: '), data.columns):
            print("Table {} has a 'Unnamed' column".format(tblname))
            isClean = False

        if len(data) == 0:
            # print("Skipping empty table", tblname)
            continue

        for colname in data.columns:

            # If TABLE.COLUMN is found, prefer that over generic column spec
            fullname = tblname + '.' + colname
            validation = col_md.get(fullname) or col_md.get(colname)

            if validation:
                series = data[colname]

                if isinstance(validation, list):
                    errors = check_value_list(series, validation)
                else: # it's a validation function
                    errors = validation(series)

                if errors:
                    isClean = False
                    print("Errors in {}.{}:".format(tblname, colname))
                    for i, value in errors:
                        if isinstance(validation, list):
                            if len(validation)>5:
                                validation = validation[:2] + ["..."] + validation[-2:]
                            print("    Value '{}' at line {} not found in allowable list {}".format(value, i+2, validation))   # +1 for header; +1 to translate 0 offset
                        else:
                            print("    Value '{}' at line {} failed data type check with function {}".format(value, i+2, validation))

    return isClean

def list_tables(db_pathname, skip_dirs=None):
    tables = []

    for dirpath, dirnames, filenames in os.walk(db_pathname, topdown=False):
        if skip_dirs and skip_dirs in dirpath:
            continue

        for filename in filenames:
            if re.match(FILE_PATTERN, filename):
                basename = os.path.basename(filename)
                tbl_name = basename.split('.')[0]
                tables.append(tbl_name)

    return tables

def clean_tables(db, update, skip_dirs=None, trim_blanks=True,
                 drop_empty_rows=True, drop_empty_cols=True):
    """
    Fix common errors in CSV files, according the the keyword args given.
    Options include trim_blanks => remove blanks surrounding column names
    and data values; drop_empty_cols => drop columns with empty names and
    values; drop_empty_rows => drop rows with all col values empty. By
    default, all options are True.

    :param db: (CsvDatabase) a CsvDatabase instance
    :param update: (bool) whether to write modifications back to the
        table (CSV) file
    :param skip_dirs: (list of str) directories to skip when cleaning
    :param trim_blanks: (bool) whether to trim blanks surrounding column
        names and data values.
    :param drop_empty_rows: (bool) whether to drop empty rows.
    :param drop_empty_cols: (bool) whether to drop empty columns.
    :return: (bool) whether any errors where found and fixed.
    """
    counts = {'empty_rows' : 0, 'empty_cols' : 0, 'trimmed_blanks' : 0}

    any_modified = False

    for tblname in list_tables(db.pathname, skip_dirs=skip_dirs):
        modified = False
        pathname = db.file_map[tblname]

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
            data = [row for row in data if not all([val=='' for val in row])]
            if len(data) < rows:
                counts['empty_rows'] += 1
                modified = True

        if drop_empty_cols:
            # transpose list to make it easy to check the columns for blanks
            data = map(list, zip(*data))

            # any cols (transposed to rows) that are all blanks get dropped
            data = [row for row in data if not all([val=='' for val in row])]
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
                print("   Writing", db.file_map[tblname])
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
        print("Finished cleaning common db errors with no issues found")


def validate_db(dbdir, validationdir, val_list, update, metadata,
                trim_blanks=True, drop_empty_rows=True, drop_empty_cols=True):
    shapes_file_map = ShapeDataMgr.create_file_map(dbdir)
    shape_tables = shapes_file_map.keys()

    metadata += [CsvMetadata(tbl_name, data_table=True) for tbl_name in shape_tables]
    db = CsvDatabase.get_database(dbdir, load=False, metadata=metadata)

    print("Cleaning common errors in db files")
    clean_tables(db, update, skip_dirs='ShapeData', trim_blanks=trim_blanks,
                 drop_empty_rows=drop_empty_rows, drop_empty_cols=drop_empty_cols)

    col_md = read_metadata(db, validationdir)

    print("\nChecking db integrity")
    # Assume all shape tables are "data tables", i.e., no "name" column is expected
    db.shapes.load_all(verbose=False)

    good = check_tables(db, col_md)
    message = "Database is clean\n" if good else "Database contains data errors\n"
    print(message)
    CsvDatabase.clear_cached_database()
