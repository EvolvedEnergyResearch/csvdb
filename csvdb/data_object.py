from __future__ import print_function
from collections import defaultdict
import pdb
import logging

from .database import CsvDatabase
from .error import SubclassProtocolError, CsvdbException
from .utils import filter_query

class StringMap(object):
    """
    A simple class to map strings to integer IDs and back again.
    """
    instance = None

    @classmethod
    def getInstance(cls):
        if not cls.instance:
            cls.instance = cls()

        return cls.instance

    def __init__(self):
        self.text_to_id = {}     # maps text to integer id
        self.id_to_text = {}     # maps id back to text
        self.next_id = 1        # the next id to assign

    def store(self, text):
        # If already known, return it
        id = self.get_id(text, raise_error=False)
        if id is not None:
            return id

        id = self.next_id
        self.next_id += 1

        self.text_to_id[text] = id
        self.id_to_text[id] = text
        return id

    def get_id(self, text, raise_error=True):
        return self.text_to_id[text] if raise_error else (None if id is None else self.text_to_id.get(text, None))

    def get_text(self, id, raise_error=True):
        return self.id_to_text[id] if raise_error else (None if id is None else self.id_to_text.get(id, None))

def str_to_id(text):
    obj = StringMap.getInstance()
    id = obj.store(text)
    return id

def get_database(pathname=None):
    return CsvDatabase.get_database(pathname)


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
        self.raw_values = None

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
        except KeyError:                # TODO: looks like this predates setting _timeseries = None in __init__
            return None

    def add_sensitivity_filter(self, key, filters): # This function is overwritten in EP with a version that does something
        return filters

    def timeseries_cleanup(self, timeseries): # This function is overwritten in EP with a slightly different version
        db = get_database()
        tbl_name = self._table_name
        tbl = db.get_table(tbl_name)
        md = tbl.metadata
        index_cols = [c for c in md.df_cols if c not in md.df_value_col]
        # replace NaNs in the index with 'None', which pandas treats better. The issue is we cannot have an index with all NaNs
        timeseries = timeseries.copy()  # avoid warning about setting a slice
        timeseries[index_cols] = timeseries[index_cols].fillna('_empty_')
        timeseries = timeseries.set_index(index_cols).sort_index()
        return timeseries

    def load_timeseries(self, key, **filters):
        db = get_database()
        tbl_name = self._table_name
        tbl = db.get_table(tbl_name)
        md = tbl.metadata

        df = tbl.data
        # Process key match as another filter
        filters[md.key_col] = key
        self.add_sensitivity_filter(key, filters)
        matches = filter_query(df, filters)

        if len(matches) == 0:
            logging.debug("""Warning: table '{}': no rows found with the following pattern: '{}'""".format(tbl_name, filters))
            self._has_data = False
            return None
        else:
            self._has_data = True

        # Find the unique sets of attributes for which to create a DF
        attrs = matches[md.attr_cols]

        if len(attrs) > 1:
            attrs = attrs.drop_duplicates()

        if len(attrs) > 1:
            columns_with_non_unique_values = [col for col in attrs.columns if len(attrs[col].unique())!=1]
            raise CsvdbException("DataObject: table '{}': there is unique data by row when it should be constant \n {}".format(tbl_name, attrs[[md.key_col]+columns_with_non_unique_values]))

        timeseries = matches[md.df_cols]
        if not timeseries[md.df_value_col].isnull().all().all(): # sometimes in EP the data is empty
            timeseries = self.timeseries_cleanup(timeseries)
            #todo improve this try/except
            try:
                timeseries = timeseries.astype(float)
            except:
                pass
            if 'gau' in timeseries.index.names:
                assert attrs['geography'].values[0] is not None, "table {}, key {}, geography can't be None".format(tbl_name, key)
                timeseries.index = timeseries.index.rename(attrs['geography'].values[0], level='gau')
            if 'oth_1' in timeseries.index.names:
                assert attrs['other_index_1'].values[0] is not None, "table {}, key {}, other_index_1 can't be None when oth_1 index exists".format(tbl_name, key)
                timeseries.index = timeseries.index.rename(attrs['other_index_1'].values[0], level='oth_1')
            if 'oth_2' in timeseries.index.names:
                assert attrs['other_index_2'].values[0] is not None, "table {}, key {}, other_index_2 can't be None when oth_2 index exists".format(tbl_name, key)
                timeseries.index = timeseries.index.rename(attrs['other_index_2'].values[0], level='oth_2')

            duplicate_index = timeseries.index.duplicated(keep=False)  # keep = False keeps all of the duplicate indices
            if any(duplicate_index):
                print("'{}' in table '{}': duplicate indices found (keeping first): \n {}".format(key, tbl_name, timeseries[duplicate_index]))
                timeseries = timeseries.groupby(level=timeseries.index.names).first()

            # we save the same data to two variables for ease of code interchangeability
            self._timeseries = timeseries.copy(deep=True) # RIO uses _timeseries
            self.raw_values = self._timeseries # EP uses raw_values
        else:
            self._timeseries = self.raw_values = None

        row = attrs

        tup = tuple(row.values[0])
        return tup

    def init_from_db(self, key, scenario, **filters):
        if key is None:
            return
        db = get_database()
        tbl_name = self._table_name
        tbl = db.get_table(tbl_name)
        md = tbl.metadata

        if md.df_cols:
            tup = self.load_timeseries(key, **filters)
        else:
            tup = self.__class__.get_row(key, scenario=scenario, **filters)
            cols = tbl.get_columns()
            if 'reference_name' in cols:
                locate = cols.index('reference_name')
                reference_name = tup[locate]
                if reference_name is not None:
                    key = reference_name
                    tup = self.__class__.get_row(key, scenario=scenario, **filters)

            if tup is None:
                self._has_data = False
                tup = [None] * len(cols)
            else:
                self._has_data = True

            # filter out the non-attribute columns
            tup = [t for t, c in zip(tup, cols) if c in md.attr_cols]

        if tup:
            self.init_from_tuple(tup, scenario)

    @classmethod
    def get_row(cls, key, scenario=None, raise_error=False, **filters):
        """
        Get a tuple for the row with the given id in the table associated with this class.
        Expects to find exactly one row with the given id. User must instantiate the database
        prior before calling this method.

        :param key: (str) the unique id of a row in `table` (for a given scenario)
        :param scenario: (str) the name of the scenario to load (together with `key` forms unique key.)
        :param raise_error: (bool) whether to raise an error or return None if the id
           is not found.
        :param filters (dict) additional col/value filtering to perform to isolate row of interest
        :return: (tuple) of values in the order the columns are defined in the table
        :raises RowNotFound: if `id` is not present in `table`.
        """
        db = get_database()
        tup = db.get_row_from_table(cls._table_name, cls._key_col, key,
                                    scenario=scenario, raise_error=raise_error, **filters)
        return tup

    def check_scenario(self, scenario):
        if scenario != self._scenario:
            raise CsvdbException("DataObject: mismatch between caller's scenario ({}) and self._scenario ({})".format(scenario, self._scenario))

    # TODO: Imported from EP. Not sure if it will remain.
    # Caller gets mapped cols via "from .text_mappings import MappedCols"
    def map_strings(self, df, mapped_cols, drop_str_cols=True):
        tbl_name = self._data_table_name

        strmap = StringMap.getInstance()
        str_cols = mapped_cols.get(tbl_name, [])

        for col in str_cols:
            # Ensure that all values are in the StringMap
            values = df[col].unique()
            for value in values:
                strmap.store(value)

            # mapped column "foo" becomes "foo_id"
            id_col = col + '_id'

            # Force string cols to str and replace 'nan' with None
            df[col] = df[col].astype(str)
            df.loc[df[col] == 'nan', col] = None

            # create a column with integer ids
            df[id_col] = df[col].map(lambda txt: strmap.get_id(txt, raise_error=False))

        if drop_str_cols:
            df.drop(str_cols, axis=1, inplace=True)

        return df
