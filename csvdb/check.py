#!/usr/bin/env python
from __future__ import print_function
from glob import glob
import os
import types
import re
from .database import CsvDatabase
from .error import CsvdbException, ValidationDataError

VALIDATION_FILE = 'validation.txt'
TIMESTAMP_FILE  = 'last_check'

FILE_PATTERN = re.compile('.*\.(csv|gz|txt)$')

def update_timestamp(db_pathname):
    timestamp_path = os.path.join(db_pathname, TIMESTAMP_FILE)
    open(timestamp_path, 'w').close()

def changed_tables(db_pathname, skip_dirs=None):
    timestamp_path = os.path.join(db_pathname, TIMESTAMP_FILE)
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

    for i, val in series.iteritems():
        test_value = val.lower() if val and isinstance(val, types.StringTypes) else val
        if test_value not in values:
            bad.append((i, val))

    return bad


def read_metadata(db):
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

                    # special cases
                    if target_col == 'geography':
                        values.append('global')
                    elif target_col == 'gau':
                        values.append('all')
                    elif target_col == 'shape':
                        values.append(None)

                    save_values(num, target_col, values)

                # Compare column values to files in this directory
                elif value.endswith('/'):
                    pattern = os.path.join(db.pathname, value, '*')
                    paths = glob(pattern)
                    values = map(extract_name, paths)

                    save_values(num, target_col, values)
            else:
                save_values(num, target_col, items)

    return value_dict

def extract_name(path):
    '''
    From, say, '/database/ShapeData/pv_utility_2-axis.csv.gz', return the
    basename of the file up to the first '.', i.e., 'pv_utility_2-axis'

    :param path: (str) a pathname
    :return: (str) return the basename of the path up to the first '.', if any.
    '''
    basename = os.path.basename(path)
    parts = basename.split('.')
    return parts[0]

def check_tables(db, col_md):
    for tblname in db.file_map.keys():

        if tblname == 'GEOGRAPHIES':        # TODO: same for all identified data tables?
            continue

        tbl = db.get_table(tblname)
        data = tbl.data

        colnames = map(str.strip, data.columns)
        if colnames != list(data.columns):
            print("WARNING: Table {} has columns with surrounding blanks".format(tblname))

        if '' in colnames:
            print("WARNING: Table {} has a blank column name".format(tblname))

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
                    print("Errors in {}.{}:".format(tblname, colname))
                    for i, value in errors:
                        print("    unknown value '{}' at line {}".format(value, i+2))   # +1 for header; +1 to translate 0 offset

def validate_db(dbdir, shapes, metadata=None):
    # TODO: handle shapes option

    db = CsvDatabase.get_database(dbdir, load=False, metadata=metadata)
    col_md = read_metadata(db)
    check_tables(db, col_md)
