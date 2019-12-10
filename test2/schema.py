#
# This is a generated file. Manual edits may be lost!
#
import sys
from csvdb.data_object import DataObject # superclass of generated classes

_Module = sys.modules[__name__]  # get ref to our own module object

class HasKey(DataObject):
    _instances_by_key = {}
    _table_name = "HAS_KEY"
    _key_col = 'name'
    _cols = ["name", "value"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        HasKey._instances_by_key[self._key] = self

        self.name = name
        self.value = None

    def set_args(self, scenario, name=None, value=None):
        self.check_scenario(scenario)

        self.name = name
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):
        (name, value,) = tup

        self.set_args(scenario, name=name, value=value)

class INPUT(DataObject):
    _instances_by_key = {}
    _table_name = "INPUT"
    _key_col = 'name'
    _cols = ["cost", "name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        INPUT._instances_by_key[self._key] = self

        self.cost = None
        self.name = name

    def set_args(self, scenario, cost=None, name=None):
        self.check_scenario(scenario)

        self.cost = cost
        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):
        (name, cost,) = tup

        self.set_args(scenario, cost=cost, name=name)

class NoKey(DataObject):
    _instances_by_key = {}
    _table_name = "NO_KEY"
    _key_col = None
    _cols = ["sensitivity", "value"]
    _df_cols = ["type", "year"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, scenario):
        DataObject.__init__(self, None, scenario)

        NoKey._instances_by_key[self._key] = self

        self.sensitivity = None
        self.value = None

    def set_args(self,  scenario, sensitivity=None, value=None):
        self.check_scenario(scenario)

        self.sensitivity = sensitivity
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):
        (value, sensitivity,) = tup

        self.set_args(scenario, sensitivity=sensitivity, value=value)

class SUBTYPE(DataObject):
    _instances_by_key = {}
    _table_name = "SUBTYPE"
    _key_col = 'name'
    _cols = ["name", "value"]
    _df_cols = ["input"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        SUBTYPE._instances_by_key[self._key] = self

        self.name = name
        self.value = None

    def set_args(self, scenario, name=None, value=None):
        self.check_scenario(scenario)

        self.name = name
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):
        (name, value,) = tup

        self.set_args(scenario, name=name, value=value)

class TYPE(DataObject):
    _instances_by_key = {}
    _table_name = "TYPE"
    _key_col = 'name'
    _cols = ["name", "subtype"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TYPE._instances_by_key[self._key] = self

        self.name = name
        self.subtype = None

    def set_args(self, scenario, name=None, subtype=None):
        self.check_scenario(scenario)

        self.name = name
        self.subtype = subtype

    def init_from_tuple(self, tup, scenario, **kwargs):
        (name, subtype,) = tup

        self.set_args(scenario, name=name, subtype=subtype)

