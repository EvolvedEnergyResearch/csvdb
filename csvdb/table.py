
import pandas as pd
import numpy as np
import pdb
import polars as pl
import re
import time

from .csv_reader import read_csv_frame
from .error import *
from .utils import filter_query

# This string is inserted into sensitivity columns when value == None,
# to allow sensitivity to be used in dataframe indices.
REF_SENSITIVITY = '_reference_'

SENSITIVITY_COL = 'sensitivity'

Verbose = False

_bad_chars = re.compile('[.: ]+')

def clean_col_name(name):
    'Replace problematic chars in column names with underscores'
    return re.sub(_bad_chars, '_', name)

class CsvTable(object):
    def __init__(self, db, tbl_name, metadata, output_table, compile_sensitivities, mapped_cols=None, filter_columns=None):
        self.db = db
        self.name = tbl_name
        self.metadata = metadata
        self.output_table = output_table
        self.compile_sensitivities = compile_sensitivities
        self.data = None
        self.str_cols = mapped_cols.get(tbl_name, None) if mapped_cols else None
        self.filter_columns = filter_columns or []
        self.data_class = None
        self.read_diagnostics = {}
        self.load_all()

    def _compute_metadata(self):
        md = self.metadata
        if md.data_table:
            return

        tbl_name = self.name
        all_cols = self.get_columns()

        key_col   = md.key_col
        df_cols   = md.df_cols
        drop_cols = md.drop_cols + self.filter_columns
        non_attr_cols = []

        if not md.attr_cols:
            non_attr_cols = df_cols + drop_cols
            md.attr_cols = [col for col in all_cols if col not in non_attr_cols]

        attr_cols = md.attr_cols

        # verify that key col is included in the the attr cols
        if md.has_key_col and key_col not in attr_cols:
            raise CsvdbException("Table {}: key_col '{}' is not present in attr_cols {}".format(tbl_name, key_col, sorted(attr_cols)))

        # verify that all specified cols are present
        specified_cols = set(attr_cols + non_attr_cols)
        missing = specified_cols - set(all_cols)
        if missing:
            raise CsvdbException("Table {}: cols {} are not present in table".format(tbl_name, sorted(missing)))

    def _compute_output_metadata(self):
        md = self.metadata
        if md.data_table:
            return

        # tbl_name = self.name
        all_cols =  [x for x in self.get_columns() if x not in self.filter_columns]
        md.key_col = None
        md.df_value_col = ['value']
        md.df_cols = all_cols
        md.drop_cols = self.filter_columns
        md.attr_cols = [col for col in all_cols if col not in md.df_value_col]

    def _compute_sensitivity_metadata(self):
        md = self.metadata
        if md.data_table:
            return
        # tbl_name = self.name
        all_cols = self.get_columns()
        md.key_col = md.key_col
        md.df_value_col = ['sensitivity']
        md.df_cols = ([md.key_col] if md.key_col else []) + md.df_filters + md.df_value_col
        md.attr_cols = None
        md.drop_cols = [col for col in all_cols if col not in md.df_cols]


    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self.name)

    def _reset_read_diagnostics(self):
        self.read_diagnostics = {
            'table': self.name,
            'requested_engine': self.db.csv_engine,
            'selected_engine': None,
            'fallback_count': 0,
            'fallback_reasons': [],
            'files': [],
        }

    def _record_read_result(self, filepath, read_result):
        reason = read_result.fallback_reason
        self.read_diagnostics['files'].append({
            'file': filepath,
            'engine_used': read_result.engine_used,
            'fallback_reason': reason,
        })

        if reason:
            self.read_diagnostics['fallback_count'] += 1
            self.read_diagnostics['fallback_reasons'].append({
                'file': filepath,
                'reason': reason,
            })
            print("WARNING: Table {} fell back to pandas while reading '{}': {}".format(self.name, filepath, reason))

    @staticmethod
    def _concat_pandas_frames(dfs):
        if not dfs:
            return pd.DataFrame()

        unique_columns_tups = set([tuple(df.columns) for df in dfs])
        if len(unique_columns_tups) > 1:
            unique_columns_sets = set([frozenset(df.columns) for df in dfs])
            if len(unique_columns_sets) > 1:
                raise CsvdbException('Columns found to differ between csv directory files. Columns include: {}'.format(unique_columns_sets))
            return pd.concat(dfs, sort=True).reset_index(drop=True)

        return pd.concat(dfs).reset_index(drop=True)

    @staticmethod
    def _concat_polars_frames(dfs):
        if not dfs:
            return pl.DataFrame()

        unique_columns_tups = set([tuple(df.columns) for df in dfs])
        if len(unique_columns_tups) > 1:
            unique_columns_sets = set([frozenset(df.columns) for df in dfs])
            if len(unique_columns_sets) > 1:
                raise CsvdbException('Columns found to differ between csv directory files. Columns include: {}'.format(unique_columns_sets))

            sorted_cols = sorted(list(unique_columns_sets.pop()))
            dfs = [df.select(sorted_cols) for df in dfs]

        return pl.concat(dfs, how='vertical_relaxed')

    def _concat_frames(self, dfs):
        if not dfs:
            return pd.DataFrame(), 'pandas'

        if all(isinstance(df, pl.DataFrame) for df in dfs):
            return self._concat_polars_frames(dfs), 'polars'

        pandas_frames = [df.to_pandas() if isinstance(df, pl.DataFrame) else df for df in dfs]
        return self._concat_pandas_frames(pandas_frames), 'pandas'

    def load_all(self):
        if self.data is not None:
            return self.data

        self._reset_read_diagnostics()

        db = self.db
        tbl_name = self.name
        filename = db.file_for_table(tbl_name)

        if not filename:
            raise CsvdbException('Missing filename for table "{}"'.format(tbl_name))

        if type(filename) is not list:
            filename = [filename]

        dfs = []
        for fn in filename:
            if not (fn.endswith('.gz') or fn.endswith('.csv')):
                continue

            wait = 1
            while True:
                try:
                    read_result = read_csv_frame(
                        fn,
                        csv_engine=db.csv_engine,
                        str_cols=self.str_cols,
                        is_gzip=fn.endswith('.gz'),
                    )
                    dfs.append(read_result.frame)
                    self._record_read_result(fn, read_result)
                    break

                except (OSError, pd.errors.EmptyDataError) as e:
                    if wait<=7200:
                        print('Pausing {} seconds. Error: "{}" when reading path: {}'.format(wait, e, fn))
                        time.sleep(wait)
                        wait *= 2
                    else:
                        raise

        if not dfs:
            raise CsvdbException('No readable CSV files found for table {}: {}'.format(tbl_name, filename))

        df, selected_engine = self._concat_frames(dfs)
        self.read_diagnostics['selected_engine'] = selected_engine

        if isinstance(df, pl.DataFrame):
            # Keep pandas as the public dataframe type.
            df = df.to_pandas()
            # Pandas normalizes multiline CSV text to LF; match that behavior.
            for text_col in ('notes', 'source'):
                if text_col in df.columns:
                    ser = df[text_col]
                    if ser.map(lambda v: isinstance(v, str) and '\r\n' in v).any():
                        df.loc[:, text_col] = ser.map(lambda v: v.replace('\r\n', '\n') if isinstance(v, str) else v)

        self.data = df

        # TODO: skip this given data cleaning methods?
        # drop leading or trailing blanks from column names
        df.columns = [col.strip() for col in df.columns]
        if self.output_table:
            self._compute_output_metadata()
        elif self.compile_sensitivities:
            self._compute_sensitivity_metadata()
        else:
            self._compute_metadata()

        md = self.metadata
        col = md.key_col

        # Raise error if the key column is missing any values
        if col and len(df):
            if (col not in df.columns):
                raise MissingKeyColumn(tbl_name, col)

            if df[col].hasnans:
                df = df[~df[col].isnull()].copy()
                # raise MissingKeyValue(tbl_name, col)

            # ensure that keys are read as strings
            df.loc[:, col] = df[col].astype(str)

        # TODO: Document this
        # Convert empty (NaN) sensitivities to value of REF_SENSITIVITY
        if self.has_sensitivity_col(df):
            df[SENSITIVITY_COL] = df[SENSITIVITY_COL].astype(object)
            df.loc[pd.isnull(df[SENSITIVITY_COL]), SENSITIVITY_COL] = REF_SENSITIVITY

        elif self.compile_sensitivities:
            # if the data doesn't have a sensitivity column and we are compiling sensitivities, data is just None
            self.data = None
            return

        # Convert all remaining NaN values to None (N.B. can't do inplace with non nan value)
        if self.output_table:
            df = df.set_index([c for c in md.df_cols if c not in md.df_value_col]).sort_index()

        elif self.compile_sensitivities:
            df = df[md.df_cols]
            df = df.rename(columns={md.key_col:'name'})

            for filter_num, filter in enumerate(md.df_filters):
                df[filter] = filter + ':' + df[filter]
                df = df.rename(columns={filter: 'filter{}'.format(filter_num+1)})

            df = df.drop_duplicates()

        # self.data = df = df.where(~pd.isnull(df), other=None) # this no longer works: https://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb
        self.data = df = df.replace({np.nan: None})

        drop_cols = [x for x in self.filter_columns if x in self.data.columns]
        if drop_cols:
            self.data.drop(drop_cols, axis='columns', inplace=True)

        rows, cols = df.shape
        if Verbose:
            print("Cached {} rows, {} cols for table '{}' from {}".format(rows, cols, tbl_name, filename))

    def has_sensitivity_col(self, df=None):
        df = self.data if df is None else df
        return SENSITIVITY_COL in df.columns

    def get_row(self, key_col, key, scenario=None, allow_multiple=False, raise_error=False, **filters):
        """
        Get a tuple for the row with the given id in the table associated with this class.
        Expects to find exactly one row with the given id. The `key` and `scenario` together
        with any filters must form a unique key. User must instantiate the database before
        calling this method.

        :param key_col: (str) the name of the column holding the key value
        :param key: (str) the unique id of a row in `table`
        :param scenario: (instance of subclass of csvdb.AbstractScenario), or None
        :param allow_multiple: (bool) whether to allow multiple rows to be returned (else it's an error.)
        :param raise_error: (bool) whether to raise an error or return None if the
           {`key`, `scenario`} combination is not found in `table`.
        :param filters: (dict) any addition colname/value pairs to use to isolate the row of interest
        :return: (tuple) of values in the order the columns are defined in the table
        :raises RowNotFound: if raise_error is True and the {`key`, `scenario`} combination
            is not present in `table`.
        """
        name = self.name
        if self.data is None:
            raise CsvdbException('No data has been loaded for table {}'.format(name))

        filters[key_col] = key

        if scenario and self.has_sensitivity_col():
            sens = scenario.get_sensitivity(name, key, **filters)
            if sens:
                filters[SENSITIVITY_COL] = sens

        rows = filter_query(self.data, filters)
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
