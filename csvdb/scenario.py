from __future__ import print_function
from collections import defaultdict
import os
from .error import ScenarioFileError, CsvdbException
from .data_object import get_database

class CsvdbFilter(object):
    def __init__(self, table_name, key_value, sens_value, constraints=None):
        """
        Construct a CsvdbFilter object

        :param table_name: (str) the name of the table to apply the filters to.
        :param key_col: (str) the name of the key column
        :param key_value: (str) the value to match in the key column, or None
        :param sens_value: (str) the value to match in the sensitivity column, or None
        :param constraints: (list) tuples of the form (col_name, col_value) to filter on.
        """
        self.table_name  = table_name
        self.key_value   = key_value
        self.sens_value  = sens_value
        self.constraints = constraints or []

        db = get_database()
        md = db.table_metadata(table_name)
        self.key_col = md.key_col


class AbstractScenario(object):
    """
    A Scenario instance holds a set of sensitivity information used to filter individual
    tables. These filters are applied in csvdb's init_from_db() method. This class is
    abstract in that the subclass must implement the load() method based on the file
    format.
    """
    def __init__(self, name, dirpath, ext, filename=None):
        """
        Load the scenario for scenario_id and and return Scenario instance.

        :param name: (str) the name of the scenario
        :param dirpath: (str) the directory in which to look for the file.
            If None, use cfg.workingdir.
        :param ext: (str) a file type (with or without the leading ".") If the
            extension is missing from the scenario_id, it is added. This is also
            used to identify the file reader to use to load the scenario(s).
        :param filename: (str) the basename of the file to load, or None to use name.
        :raises ScenarioFileError: if the file isn't found or isn't formatted properly.
        """
        if ext[0] != '.':
            ext = '.' + ext

        self.pathname = pathname = self.filepath(filename, dirpath, ext)

        if not os.path.lexists(pathname):
            raise ScenarioFileError(pathname, "Scenario file does not exist.")

        self.name = name

        # Sensitivities are stored in a dict keyed by table name, with values being a
        # dict keyed by tuples of (key_value, filters...) where filters is a tuple of
        # (col_name, col_value) pairs. The value of the inner dict is the sensitivity name.
        self.filter_dict = defaultdict(dict)

        self.load()        # N.B. subclass responsibility to provide this

    @classmethod
    def filepath(cls, filename, dirpath, ext):
        filename += ('' if filename.endswith(ext) else ext)
        pathname = os.path.join(dirpath, filename)
        return pathname

    def load(self):
        raise CsvdbException("Called AbstractScenario's 'load' method; must be defined in a subclass.")

    def add_filter(self, filter):
        """
        Add a CsvdbFilter instance to the filter_dict's list for the referenced table.
        """
        constraints = tuple(filter.constraints) or None

        key = (filter.key_value, constraints)
        self.filter_dict[filter.table_name][key] = filter.sens_value

    def get_sensitivity(self, table_name, key_value, **filters):
        """
        Return the name of the sensitivity associated with the table, key, and optional filters, or None
        if no such sensitivity exists.
        """
        constraints = tuple((key, value) for key, value in filters.items()) or None
        lookup_key = (key_value, constraints)
        filter = self.filter_dict[table_name]

        sens_value = filter.get(lookup_key)
        return sens_value
