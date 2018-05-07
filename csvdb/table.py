from __future__ import print_function
import gzip
import pandas as pd
import pdb
import re

from .error import *
from .utils import col_match

# This string is inserted into sensitivity columns when value == None,
# to allow sensitivity to be used in dataframe indices.
REF_SCENARIO = '_reference_'

SENSITIVITY_COL = 'sensitivity'


_bad_chars = re.compile('[\.:\ ]+')

def clean_col_name(name):
    'Replace problematic chars in column names with underscores'
    return re.sub(_bad_chars, '_', name)


class CsvTable(object):
    def __init__(self, db, tbl_name, metadata):
        self.db = db
        self.name = tbl_name
        self.metadata = metadata
        self.data = None

        self.data_class = None
        self.load_all()

    def _compute_metadata(self):
        md = self.metadata
        if md.data_table:
            return

        tbl_name = self.name
        all_cols = self.get_columns()

        key_col    = md.key_col
        df_key_col = md.df_key_col
        df_cols    = md.df_cols
        drop_cols  = md.drop_cols

        df_key_cols = [df_key_col] if df_key_col else []
        if not md.attr_cols:
            non_attr_cols = df_cols + df_key_cols + drop_cols
            md.attr_cols = attr_cols = [col for col in all_cols if col not in non_attr_cols]

        # verify that key col is included in the the attr cols
        if key_col not in attr_cols:
            raise Exception("Table {}: key_col '{}' is not present in attr_cols {}".format(
                tbl_name, key_col, sorted(attr_cols)))

        # verify that all specified cols are present
        specified_cols = set(attr_cols + non_attr_cols)
        missing = specified_cols - set(all_cols)
        if missing:
            raise Exception("Table {}: cols {} are not present in table".format(tbl_name, sorted(missing)))


    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self.name)

    def load_all(self):
        if self.data is not None:
            return self.data

        db = self.db
        tbl_name = self.name
        filename = db.file_for_table(tbl_name)

        if not filename:
            raise CsvdbException('Missing filename for table "{}"'.format(tbl_name))

        openFunc = gzip.open if filename.endswith('.gz') else open
        with openFunc(filename, 'rb') as f:
            self.data = df = pd.read_csv(f, index_col=None)

        # drop leading or trailing blanks from column names
        df.columns = map(str.strip, df.columns)

        self._compute_metadata()

        col = self.metadata.key_col

        # Raise error if the key column is missing any values

        if col and len(df):
            if (col not in df.columns):
                raise MissingKeyColumn(tbl_name, col)

            if df[col].hasnans:
                raise MissingKeyValue(tbl_name, col)

            # ensure that keys are read as strings
            df[col] = df[col].astype(str)

        # TODO: Document this
        # Convert empty (NaN) sensitivities to value of REF_SCENARIO
        if self.has_sensitivity_col(df):
            s = df[SENSITIVITY_COL]
            s.where(pd.notnull(s), other=REF_SCENARIO, inplace=True)

        # Convert all remaining NaN values to None (N.B. can't do inplace with non nan value)
        self.data = df = df.where(pd.notnull(df), other=None)

        rows, cols = df.shape
        print("Cached {} rows, {} cols for table '{}' from {}".format(rows, cols, tbl_name, filename))


    def has_sensitivity_col(self, df=None):
        df = self.data if df is None else df
        return SENSITIVITY_COL in df.columns

    def get_row(self, key_col, key, scenario=None, allow_multiple=False, raise_error=True):
        """
        Get a tuple for the row with the given id in the table associated with this class.
        Expects to find exactly one row with the given id. The `key` and `scenario` together
        must forms a unique key. User must instantiate the database before calling this method.

        :param key_col: (str) the name of the column holding the key value
        :param key: (str) the unique id of a row in `table`
        :param scenario: (str) the scenario to load, or None, in which case `scenario` is ignored.
        :param allow_multiple: (bool) whether to allow multiple rows to be returned (else it's an error.)
        :param raise_error: (bool) whether to raise an error or return None if the
           {`key`, `scenario`} combination is not found in `table`.
        :return: (tuple) of values in the order the columns are defined in the table
        :raises RowNotFound: if raise_error is True and the {`key`, `scenario`} combination
            is not present in `table`.
        """
        name = self.name
        query = col_match(key_col, key)

        if scenario and self.has_sensitivity_col():
            query += ' and ' + col_match(SENSITIVITY_COL, scenario)

        if self.data is None:
            raise CsvdbException('No data has been loaded for table {}'.format(name))

        rows = self.data.query(query)
        tups = [tuple(row) for idx, row in rows.iterrows()]

        count = len(tups)
        if count == 0:
            if raise_error:
                raise RowNotFound(name, key)
            else:
                return None

        if count > 1:
            if allow_multiple:
                return tups
            else:
                raise DuplicateRowsFound(name, key)

        return tups[0]

    def get_columns(self):
        return list(self.data.columns)

    def get_dataframe(self, key_value, copy=True):
        """
        Return a DataFrame holding all rows from the underlying DataFrame
        for which the key column equals the given key_value. By default, a
        copy of the data is returned. If copy == False, a view slice of the
        underlying DataFrame is returned.

        :param key_value: (str) key value to use to identify rows to return
        :param copy: (bool) whether to return a copy or a slice of the underlying data
        :return: (DataFrame) either a copy of the rows found, or a slice of the table's
           underlying DataFrame
        """
        df = self.data
        key_col = self.db.get_key_col(self.name)
        result = df.query("%s == %r" % (key_col, key_value))
        return result.copy(deep=True) if copy else result

