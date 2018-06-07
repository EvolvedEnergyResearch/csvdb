#!/usr/bin/env python
from __future__ import print_function
import click
import os
import types

from csvdb import CsvDatabase, CsvdbException, CsvMetadata

class MetaDataError(Exception):
    def __init__(self, path, linenum, msg):
        msg = "Syntax error in {} at line {}: {}".format(path, linenum+1, msg)
        super(MetaDataError, self).__init__(msg)

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
    filename = os.path.join(db.pathname, 'metadata.txt')
    with open(filename, 'r') as f:
        lines = [(num, line.strip()) for num, line in enumerate(f) if not line.startswith('#')]

    known_types = _check_fns.keys()

    value_dict = {}     # maps col names to either a list of strings or a function to call on the column

    # helper function to check for duplicate entries
    def save_values(num, colname, values):
        if colname in value_dict:
            raise MetaDataError(filename, num, "duplicate entry for column '{}'".format(colname))

        value_dict[colname] = values


    for num, line in lines:
        if line:        # ignore blank lines
            items = filter(lambda item: item != "", line.split(','))
            if len(items) < 2:
                raise MetaDataError(filename, num, "expected at least 2 items, got '{}'".format(line))

            target_col = items.pop(0)

            if len(items) == 1:     # either a type or a table.column reference
                value = items[0]
                aType = value.lower()

                # type check
                if aType in known_types:
                    save_values(num, target_col, _check_fns[aType]) # saves a function ref, not a list of values

                # compare with values in given table.column
                elif '.' in value:
                    parts = value.split('.')
                    if len(parts) != 2:
                        raise MetaDataError(filename, num, "expected TABLE.COLUMN, got '{}'".format(value))

                    tblname, colname = parts

                    try:
                        tbl = db.get_table(tblname)
                    except CsvdbException:
                        raise MetaDataError(filename, num, "unknown table '{}'".format(tblname))

                    if colname not in tbl.data.columns:
                        raise MetaDataError(filename, num, "unknown column '{}' in table '{}'".format(colname, tblname))

                    values = list(tbl.data[colname].unique())

                    # special cases
                    if target_col == 'geography':
                        values.append('global')
                    elif target_col == 'gau':
                        values.append('all')
                    elif target_col == 'shape':
                        values.append(None)

                    save_values(num, target_col, values)
            else:
                save_values(num, target_col, items)

    return value_dict


_DbMetadata = [
    CsvMetadata('GEOGRAPHIES', data_table=True),
    CsvMetadata('GEOGRAPHIES_SPATIAL_JOIN', data_table=True),
    CsvMetadata('CURRENCY_CONVERSION', data_table=True),
    CsvMetadata('INFLATION_CONVERSION', data_table=True),
]


@click.command()

# Positional argument
@click.argument('dbdir', type=click.Path(exists=True))

@click.option('--shapes/--no-shapes', default=False,
              help='Check the Shapes data. Default is to skip this check.')

def main(dbdir, shapes):
    print("DB: {}, shapes: {}".format(dbdir, shapes))

    db = CsvDatabase.get_database(dbdir, load=False, metadata=_DbMetadata)

    try:
        col_md = read_metadata(db)
    except MetaDataError as e:
        print(e)
        return

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
            validation = col_md.get(colname)
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


if __name__ == '__main__':
    main()
