
from glob import glob
import os
import types
from .error import CsvdbException, ValidationFormatError
import pdb
import numpy as np

_True  = ['t', 'y', 'true',  'yes', 'on']
_False = ['f', 'n', 'false', 'no',  'off']
_Bool  = _True + _False

def str_to_bool(value):
    return isinstance(value, types.StringTypes) and value.lower() in _True

def _check_bool(series, nullable):
    bad = []

    for i, value in series.items():
        if value is None and nullable:
            continue

        if not isinstance(value, (bool, np.bool_)) and (not isinstance(value, types.StringTypes) or value.lower() not in _Bool):
            bad.append((i, value))

    return bad

def _check_type(series, aType, nullable):
    bad = []

    for i, value in series.items():
        if value is None and nullable:
            continue
        try:
            aType(value)
        except:
            bad.append((i, value))

    return bad

def _check_float(series, nullable):
    return _check_type(series, float, nullable)

def _check_int(series, nullable):
    return _check_type(series, int, nullable)

_check_fns = {
    'int'            : _check_int,
    'float'          : _check_float,
    'bool'           : _check_bool,
}

def _is_float(s):
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

def _extract_name(path):
    """
    From, say, '/database/ShapeData/pv_utility_2-axis.csv.gz', return the
    basename of the file up to the first '.', i.e., 'pv_utility_2-axis'

    :param path: (str) a pathname
    :return: (str) return the basename of the path up to the first '.', if any.
    """
    basename = os.path.basename(path)
    parts = basename.split('.')
    return parts[0]

# we have a set of valid inputs, but also want to allow null
# we have a referenced table and referenced field, but also want to add additional valid options to it (less critical)
# we have a foreign key constraint within the same table
# we have a column that is not null if and when another column is not null (these are indicated by 'linked_column' == True)

# Stores a row of data from validation.csv in an object, after performing some type
# checking and conversion. Also collects values from a referenced folder or table.col.
class ValidationInfo(object):
    def __init__(self, db, table_name, column_name, not_null, linked_column,
                 dtype, folder, ref_tbl, ref_col, cascade_delete, extra_values):
        self.table_name = table_name
        self.column_name = column_name
        self.not_null = str_to_bool(not_null)
        self.linked_column = linked_column
        self.dtype = dtype.lower()
        self.type_func = _check_fns[self.dtype] if dtype else None
        self.folder = folder
        self.ref_tbl = ref_tbl
        self.ref_col = ref_col
        self.cascade_delete = str_to_bool(cascade_delete)

        self.values = []    # all legal values given

        if self.folder:
            pattern = os.path.join(db.pathname, self.folder, '*')
            paths = glob(pattern)
            self.values = map(_extract_name, paths)

        elif ref_tbl and ref_col:
            try:
                tbl = db.get_table(ref_tbl)
            except CsvdbException:
                raise ValidationFormatError("unknown table '{}'".format(ref_tbl))

            if ref_col not in tbl.data.columns:
                raise ValidationFormatError("unknown column '{}' in table '{}'".format(ref_col, ref_tbl))

            self.values = list(tbl.data[ref_col].unique())

            # TODO: handle this in metadata?
            if ref_col == 'shape':
                self.values.append(None)

        if extra_values:
            # parse / validate numbers
            if all(map(str.isdigit, extra_values)):
                extra_values = map(int, extra_values)
            elif all(map(_is_float, extra_values)):
                extra_values = map(float, extra_values)

            self.values += extra_values

    def __str__(self):
        return "<ValidationInfo {}.{}>".format(self.table_name, self.column_name)
