from __future__ import print_function
from collections import defaultdict
import pdb
import numpy as np

from .database import CsvDatabase
from .error import SubclassProtocolError, CsvdbException
from .utils import col_match

def get_database():
    return CsvDatabase.get_database(None)

def _isListOfNoneOrNan(obj):
    if len(obj) != 1:
        return False

    item = obj[0]
    return item is None or (isinstance(item, float) and np.isnan(item))


class DataObject(object):
    # dict keyed by class object; value is list of instances of the class
    _instancesByClass = defaultdict(list)

    # These are here for completeness; they're shadowed in generated subclasses.
    _instances_by_key = {}
    _key_col = None
    _cols = None
    _table_name = None
    _index_level_tups = None

    def __init__(self, key, scenario):
        """
        Append self to a list for our subclass
        """
        cls = self.__class__
        self._instancesByClass[cls].append(self)
        self._scenario = scenario
        self._key = key
        self._timeseries = None          # dataframe or None

    def __str__(self):
        return "<{} {}='{}'>".format(self.__class__.__name__, self._key_col, self._key)

    @classmethod
    def load_from_db(cls, key, scenario, **filters):
        self = cls(key, scenario)
        self.init_from_db(key, scenario, **filters)
        return self

    @classmethod
    def instances(cls):
        """
        Return instances for any subclass of DataObject.
        """
        return DataObject._instancesByClass[cls]

    @classmethod
    def get_instance(cls, key):
        cls._instances_by_key.get(key, None)  # uses each class' internal dict

    def init_from_tuple(self, tup, scenario):
        """
        This method is created in the generated subclasses (e.g., in schema.py)
        """
        raise SubclassProtocolError(self.__class__, 'init_from_tuple')

    def init_from_series(self, series, scenario):
        self.init_from_tuple(tuple(series), scenario)

    def timeseries(self):
        try:
            return self._timeseries
        except KeyError:
            return None

    def load_timeseries(self, key, **filters):
        db = get_database()
        tbl_name = self._table_name
        tbl = db.get_table(tbl_name)
        md = tbl.metadata

        df = tbl.data
        df_key_col = md.df_key_col

        key_cond = col_match(md.key_col, key)
        filter_conds = [col_match(attr, value) for attr, value in filters.items()]
        conds = [key_cond] + filter_conds
        query = ' and '.join(conds)
        matches = df.query(query)

        # TODO: should this be an error?
        if len(matches) == 0:
            print("""Warning: table '{}': no rows with '{} == "{}"'""".format(tbl_name, md.key_col, key))
            return None

        # Find the unique sets of attributes for which to create a DF
        cols = md.attr_cols + [df_key_col]
        attrs = matches[cols].drop_duplicates()

        if len(attrs) > 1:
            raise CsvdbException("DataObject: table '{}': there are {} rows of data but no df_key_col defined".format(
                tbl_name, len(attrs)))

        df_key = attrs[df_key_col].iloc[0]
        slice = matches.query(col_match(df_key_col, df_key))
        timeseries = slice[md.df_cols]
        timeseries = timeseries.set_index([c for c in md.df_cols if c!=md.df_value_col]).sort_index()
        timeseries = timeseries.astype(float)
        if 'gau' in timeseries.index.names:
            timeseries.index = timeseries.index.rename(attrs['geography'].values[0], level='gau')
        self._timeseries = timeseries.copy(deep=True)

        row = attrs.drop(df_key_col, axis=1)
        tup = tuple(row.values[0])
        return tup

    def init_from_db(self, key, scenario, **filters):
        db = get_database()
        tbl_name = self._table_name
        tbl = db.get_table(tbl_name)
        md = tbl.metadata
        key = key.lower()

        if md.df_cols:
            tup = self.load_timeseries(key, **filters)
        else:
            tup = self.__class__.get_row(key, scenario=scenario, **filters)
            cols = tbl.get_columns()

            if tup is None:
                tup = [None] * len(cols)

            # filter out the non-attribute columns
            tup = [t for t, c in zip(tup, cols) if c in md.attr_cols]

        if tup:
            self.init_from_tuple(tup, scenario)

    @classmethod
    def get_row(cls, key, scenario=None, raise_error=False):
        """
        Get a tuple for the row with the given id in the table associated with this class.
        Expects to find exactly one row with the given id. User must instantiate the database
        prior before calling this method.

        :param key: (str) the unique id of a row in `table` (for a given scenario)
        :param scenario: (str) the name of the scenario to load (together with `key` forms unique key.)
        :param raise_error: (bool) whether to raise an error or return None if the id
           is not found.
        :return: (tuple) of values in the order the columns are defined in the table
        :raises RowNotFound: if `id` is not present in `table`.
        """
        db = get_database()
        tup = db.get_row_from_table(cls._table_name, cls._key_col, key, scenario=scenario, raise_error=raise_error)
        return tup

    def check_scenario(self, scenario):
        if scenario != self._scenario:
            raise CsvdbException("DataObject: mismatch between caller's scenario ({}) and self._scenario ({})".format(scenario, self._scenario))

