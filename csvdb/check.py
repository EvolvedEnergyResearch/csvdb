#!/usr/bin/env python
from __future__ import print_function
from glob import glob
import gzip
import pandas as pd
import os
import types
import re
from .database import CsvDatabase, CsvMetadata, ShapeDataMgr
from .error import CsvdbException, ValidationDataError

VALIDATION_FILE = 'validation.txt'
TIMESTAMP_FILE  = 'last_clean'

FILE_PATTERN   = re.compile('.*\.(csv|gz)$')
SPACES_PATTERN = re.compile('\s\s+')

_True  = ['t', 'y', 'true',  'yes', 'on']
_False = ['f', 'n', 'false', 'no',  'off']
_Bool  = _True + _False

def check_bool(series):
    bad = []

    for i, value in series.iteritems():
        if isinstance(value, types.StringTypes) and value.lower() not in _Bool:
            bad.append((i, value))

    return bad

def check_type(series, aType):
    bad = []

    for i, value in series.iteritems():
        try:
            aType(value)
        except:
            bad.append((i, value))

    return bad

def check_float(series):
    return check_type(series, float)

def check_int(series):
    return check_type(series, int)

_check_fns = {
    'int'   : check_int,
    'float' : check_float,
    'bool'  : check_bool
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

def read_metadata(db):
    """
    Reads the CsvDatabase validation metadata from the file 'validation.txt' found
    at the top level of the CsvDatabase directory.

    :param db: (CsvDatabase) a CsvDatabase instance
    :return: (Dict) a dict keyed by column names or table.column names, with
        values being either a list of strings or a function to call on the column
        to validate the values therein.
    """
    filename = os.path.join(db.pathname, VALIDATION_FILE)

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

def check_tables(db, col_md, shapes):
    """
    Check whether the CsvDatabase tables (CSV files) are clean.

    :param db: (CsvDatabase) a CsvDatabase instance
    :param col_md: (Dict) values dictionary returned by read_metadata()
    :param shapes: (bool) whether to check shape files
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
            print("Skipping empty table", tblname)
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
                        print("    unknown value '{}' at line {}".format(value, i+2))   # +1 for header; +1 to translate 0 offset

    return isClean

def update_timestamp(db, remove=False):
    '''
    If remove is False, update the timestamp file's modification to
    the current time. If remove is True, just remove the file.

    :param db: (CsvDatabase) a CsvDatabase instance
    :param remove: (bool) whether to remove the timestamp file
    :return: nothing
    '''
    timestamp_path = os.path.join(db.pathname, TIMESTAMP_FILE)
    if remove:
        os.remove(timestamp_path)
    else:
        open(timestamp_path, 'w').close()

def changed_tables(db_pathname, skip_dirs=None):
    timestamp_path = os.path.join(db_pathname, TIMESTAMP_FILE)

    # If the timestamp file is not found, all files are checked
    timestamp = os.path.getmtime(timestamp_path) if os.path.exists(timestamp_path) else 0

    changed = []

    for dirpath, dirnames, filenames in os.walk(db_pathname, topdown=False):
        if skip_dirs and os.path.basename(dirpath) in skip_dirs:
            continue

        for filename in filenames:
            path = os.path.abspath(os.path.join(dirpath, filename))
            if re.match(FILE_PATTERN, filename) and os.path.getmtime(path) > timestamp:
                basename = os.path.basename(filename)
                tbl_name = basename.split('.')[0]
                changed.append(tbl_name)

    return changed

def clean_tables(db, update, skip_dirs=None):
    """
    Fix common errors in CSV files, including: removing surrounding blanks
    from column names and data values, dropping columns with empty names and
    values, AND??

    :param db: (CsvDatabase) a CsvDatabase instance
    :param skip_dirs: (list of str) directories to skip when cleaning
    :return: (bool) whether any errors where found and fixed.
    """
    for tblname in changed_tables(db.pathname, skip_dirs=skip_dirs):

        modified = False
        tbl = db.get_table(tblname)
        data = tbl.data

        rows, cols = data.shape
        print("Table: {} ({:,} rows, {} cols)".format(tblname, rows, cols))

        # pandas converts blank col names to 'Unnamed: n' where n is the col's index
        unnamed = filter(lambda name: name.startswith('Unnamed: '), data.columns)

        for colname in unnamed:
            # We convert blank values to None when reading CSV files.
            # Drop any 'unnamed' col whose values are all None.
            if all(map(lambda value: value is None, data[colname])):
                print("Removing empty columns from table {}".format(tblname))
                tbl.data.drop(colname, axis=1, inplace=True)
                modified = True

        # Remove surrounding whitespace in column names and replace multiple
        # whitespace chars with a single space.
        colnames = data.columns.str.strip()
        # N.B. the Series.str.replace() method failed with what looks like a bug, thus this approach
        colnames = pd.Index(map(lambda s: re.sub(SPACES_PATTERN, ' ', s), colnames))

        if not colnames.equals(data.columns):
            print("Removing extra blanks from column names in table {}".format(tblname))
            data.columns = colnames
            modified = True

        if len(data) == 0:
            print("Skipping empty table", tblname)
            continue

        for c, colname in enumerate(data.columns):
            print("   col {:2d}: {}".format(c, colname))

            series = data[colname]
            original = list(series.unique())
            cleansed = [re.sub(SPACES_PATTERN, ' ', value.strip().lower()) for value in original if isinstance(value, basestring)]

            count = 0
            for old, new in zip(original, cleansed):
                if isinstance(old, basestring) and old != new:
                    series.replace(old, new, inplace=True)
                    count += 1

            if count:
                print("Modified {} unique values in {}.{}".format(count, tblname, colname))

        if update and modified:
            pathname = db.file_for_table(tblname)
            print("Writing", pathname)
            openFunc = gzip.open if pathname.endswith('.gz') else open
            with openFunc(pathname) as f:
                data.to_csv(f, index=False)

    if update:
        update_timestamp(db)


def validate_db(dbdir, update, shapes, force, metadata=None):
    shapes_file_map = ShapeDataMgr.create_filemap(dbdir)
    shape_tables = shapes_file_map.keys()

    metadata += [CsvMetadata(tbl_name, data_table=True) for tbl_name in shape_tables]
    db = CsvDatabase.get_database(dbdir, load=False, metadata=metadata)

    col_md = read_metadata(db)

    # Assume all shape tables are "data tables", i.e., no "name" column is expected

    if force:
        update_timestamp(db, remove=True)

    if shapes:
        db.shapes.load_all()

    clean_tables(db, update)

    good = check_tables(db, col_md, shapes)
    message = "Database is clean" if good else "Database contains data errors"
    print(message)
