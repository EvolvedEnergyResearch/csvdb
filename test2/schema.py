#
# This is a generated file. Manual edits may be lost!
#
import sys
from csvdb.data_object import DataObject # superclass of generated classes

_Module = sys.modules[__name__]  # get ref to our own module object

class EmissionsConstraint(DataObject):
    _instances_by_key = {}
    _table_name = "EMISSIONS_CONSTRAINT"
    _key_col = None
    _cols = ["extrapolation_method", "geography", "geography_map_key", "interpolation_method", "notes",
             "source", "unit"]
    _df_cols = ["sensitivity", "year", "gau", "value"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, scenario):
        DataObject.__init__(self, None, scenario)

        EmissionsConstraint._instances_by_key[self._key] = self

        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.notes = None
        self.source = None
        self.unit = None

    def set_args(self, scenario, extrapolation_method=None, geography=None, geography_map_key=None,
                 interpolation_method=None, notes=None, source=None, unit=None):
        self.check_scenario(scenario)

        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.notes = notes
        self.source = source
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (source, notes, geography, unit, interpolation_method, extrapolation_method,
         geography_map_key,) = tup

        self.set_args(scenario, extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  notes=notes, source=source, unit=unit)

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

