#
# This is a generated file. Manual edits may be lost!
#
import sys
from csvdb.data_object import DataObject # superclass of generated classes

_Module = sys.modules[__name__]  # get ref to our own module object

class BlendCapitalCost(DataObject):
    _instances_by_key = {}
    _table_name = "BLEND_CAPITAL_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_growth_rate", "extrapolation_method",
             "geography", "geography_map_key", "interpolation_method", "levelized", "name",
             "recovery_factor", "unit"]
    _df_cols = ["sensitivity", "gau", "vintage", "value"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        BlendCapitalCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.name = name
        self.recovery_factor = None
        self.unit = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_growth_rate=None,
                 extrapolation_method=None, geography=None, geography_map_key=None,
                 interpolation_method=None, levelized=None, name=None, recovery_factor=None, unit=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.levelized = levelized
        self.name = name
        self.recovery_factor = recovery_factor
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, geography, geography_map_key, interpolation_method,
         extrapolation_method, extrapolation_growth_rate, recovery_factor, levelized,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year,
                  extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  levelized=levelized, name=name, recovery_factor=recovery_factor, unit=unit)

class BlendExistingStorage(DataObject):
    _instances_by_key = {}
    _table_name = "BLEND_EXISTING_STORAGE"
    _key_col = 'name'
    _cols = ["extrapolation_growth_rate", "extrapolation_method", "geography", "geography_map_key",
             "interpolation_method", "name", "unit"]
    _df_cols = ["sensitivity", "gau", "year", "value"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        BlendExistingStorage._instances_by_key[self._key] = self

        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.unit = None

    def set_args(self, scenario, extrapolation_growth_rate=None, extrapolation_method=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, unit=None):
        self.check_scenario(scenario)

        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, unit, geography, geography_map_key, interpolation_method, extrapolation_method,
         extrapolation_growth_rate,) = tup

        self.set_args(scenario, extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  name=name, unit=unit)

class BlendExoDemand(DataObject):
    _instances_by_key = {}
    _table_name = "BLEND_EXO_DEMAND"
    _key_col = 'name'
    _cols = ["name", "unit", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        BlendExoDemand._instances_by_key[self._key] = self

        self.name = name
        self.unit = None
        self.value = None
        self.year = None

    def set_args(self, scenario, name=None, unit=None, value=None, year=None):
        self.check_scenario(scenario)

        self.name = name
        self.unit = unit
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, unit, year, value,) = tup

        self.set_args(scenario, name=name, unit=unit, value=value, year=year)

class BlendFuelInputs(DataObject):
    _instances_by_key = {}
    _table_name = "BLEND_CONNECTIONS"
    _key_col = 'name'
    _cols = ["fuel", "limit", "name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        BlendFuelInputs._instances_by_key[self._key] = self

        self.fuel = None
        self.limit = None
        self.name = name

    def set_args(self, scenario, fuel=None, limit=None, name=None):
        self.check_scenario(scenario)

        self.fuel = fuel
        self.limit = limit
        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, fuel, limit,) = tup

        self.set_args(scenario, fuel=fuel, limit=limit, name=name)

class BlendMain(DataObject):
    _instances_by_key = {}
    _table_name = "BLEND_MAIN"
    _key_col = 'name'
    _cols = ["book_life", "enforce_storage_constraints", "lifetime", "name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        BlendMain._instances_by_key[self._key] = self

        self.book_life = None
        self.enforce_storage_constraints = None
        self.lifetime = None
        self.name = name

    def set_args(self, scenario, book_life=None, enforce_storage_constraints=None, lifetime=None, name=None):
        self.check_scenario(scenario)

        self.book_life = book_life
        self.enforce_storage_constraints = enforce_storage_constraints
        self.lifetime = lifetime
        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, enforce_storage_constraints, book_life, lifetime,) = tup

        self.set_args(scenario, book_life=book_life, enforce_storage_constraints=enforce_storage_constraints,
                  lifetime=lifetime, name=name)

class ConversionCapitalCost(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_CAPITAL_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_growth_rate", "extrapolation_method",
             "geography", "geography_map_key", "interpolation_method", "levelized", "name",
             "recovery_factor", "time_unit", "unit"]
    _df_cols = ["sensitivity", "gau", "vintage", "value"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionCapitalCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.name = name
        self.recovery_factor = None
        self.time_unit = None
        self.unit = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_growth_rate=None,
                 extrapolation_method=None, geography=None, geography_map_key=None,
                 interpolation_method=None, levelized=None, name=None, recovery_factor=None,
                 time_unit=None, unit=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.levelized = levelized
        self.name = name
        self.recovery_factor = recovery_factor
        self.time_unit = time_unit
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, time_unit, geography, geography_map_key,
         interpolation_method, extrapolation_method, extrapolation_growth_rate, recovery_factor,
         levelized,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year,
                  extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  levelized=levelized, name=name, recovery_factor=recovery_factor, time_unit=time_unit,
                  unit=unit)

class ConversionEfficiency(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_EFFICIENCY"
    _key_col = 'name'
    _cols = ["extrapolation_growth_rate", "extrapolation_method", "geography", "geography_map_key",
             "input", "input_unit", "interpolation_method", "name", "output_unit"]
    _df_cols = ["sensitivity", "gau", "vintage", "value"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionEfficiency._instances_by_key[self._key] = self

        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.input = None
        self.input_unit = None
        self.interpolation_method = None
        self.name = name
        self.output_unit = None

    def set_args(self, scenario, extrapolation_growth_rate=None, extrapolation_method=None, geography=None,
                 geography_map_key=None, input=None, input_unit=None, interpolation_method=None,
                 name=None, output_unit=None):
        self.check_scenario(scenario)

        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.input = input
        self.input_unit = input_unit
        self.interpolation_method = interpolation_method
        self.name = name
        self.output_unit = output_unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, input, input_unit, output_unit, geography, geography_map_key, interpolation_method,
         extrapolation_method, extrapolation_growth_rate,) = tup

        self.set_args(scenario, extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, input=input, input_unit=input_unit,
                  interpolation_method=interpolation_method, name=name, output_unit=output_unit)

class ConversionFixedOmAnn(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_FIXED_OM_ANN"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "name", "notes", "source", "time_unit",
             "unit", "value", "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionFixedOmAnn._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.source = None
        self.time_unit = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, notes=None, source=None,
                 time_unit=None, unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.source = source
        self.time_unit = time_unit
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, unit, time_unit, geography, gau,
         geography_map_key, interpolation_method, extrapolation_method, vintage, value,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, extrapolation_method=extrapolation_method,
                  gau=gau, geography=geography, geography_map_key=geography_map_key,
                  interpolation_method=interpolation_method, name=name, notes=notes, source=source,
                  time_unit=time_unit, unit=unit, value=value, vintage=vintage)

class ConversionMain(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_MAIN"
    _key_col = 'name'
    _cols = ["name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionMain._instances_by_key[self._key] = self

        self.name = name

    def set_args(self, scenario, name=None):
        self.check_scenario(scenario)

        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name,) = tup

        self.set_args(scenario, name=name)

class ConversionRps(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_RPS"
    _key_col = 'name'
    _cols = ["gau", "geography", "geography_map_key", "name", "type", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionRps._instances_by_key[self._key] = self

        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.name = name
        self.type = None
        self.value = None
        self.year = None

    def set_args(self, scenario, gau=None, geography=None, geography_map_key=None, name=None, type=None, value=None,
                 year=None):
        self.check_scenario(scenario)

        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.name = name
        self.type = type
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, type, geography, gau, geography_map_key, year, value,) = tup

        self.set_args(scenario, gau=gau, geography=geography, geography_map_key=geography_map_key, name=name, type=type,
                  value=value, year=year)

class ConversionVariableOm(DataObject):
    _instances_by_key = {}
    _table_name = "CONVERSION_VARIABLE_OM"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "name", "notes", "source", "unit", "value",
             "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ConversionVariableOm._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.source = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, notes=None, source=None,
                 unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.source = source
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, unit, geography, gau, geography_map_key,
         interpolation_method, extrapolation_method, vintage, value,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, extrapolation_method=extrapolation_method,
                  gau=gau, geography=geography, geography_map_key=geography_map_key,
                  interpolation_method=interpolation_method, name=name, notes=notes, source=source,
                  unit=unit, value=value, vintage=vintage)

class ExistingMain(DataObject):
    _instances_by_key = {}
    _table_name = "EXISTING_MAIN"
    _key_col = 'name'
    _cols = ["capacity", "gau", "generation_p_max", "generation_p_min", "generator", "geography",
             "load_p_max", "load_p_min", "name", "operating_year", "ramp_rate", "ramp_rate_time_unit",
             "retirement_year", "shape", "technology_type", "unit"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ExistingMain._instances_by_key[self._key] = self

        self.capacity = None
        self.gau = None
        self.generation_p_max = None
        self.generation_p_min = None
        self.generator = None
        self.geography = None
        self.load_p_max = None
        self.load_p_min = None
        self.name = name
        self.operating_year = None
        self.ramp_rate = None
        self.ramp_rate_time_unit = None
        self.retirement_year = None
        self.shape = None
        self.technology_type = None
        self.unit = None

    def set_args(self, scenario, capacity=None, gau=None, generation_p_max=None, generation_p_min=None, generator=None,
                 geography=None, load_p_max=None, load_p_min=None, name=None, operating_year=None,
                 ramp_rate=None, ramp_rate_time_unit=None, retirement_year=None, shape=None,
                 technology_type=None, unit=None):
        self.check_scenario(scenario)

        self.capacity = capacity
        self.gau = gau
        self.generation_p_max = generation_p_max
        self.generation_p_min = generation_p_min
        self.generator = generator
        self.geography = geography
        self.load_p_max = load_p_max
        self.load_p_min = load_p_min
        self.name = name
        self.operating_year = operating_year
        self.ramp_rate = ramp_rate
        self.ramp_rate_time_unit = ramp_rate_time_unit
        self.retirement_year = retirement_year
        self.shape = shape
        self.technology_type = technology_type
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, technology_type, generator, shape, geography, gau, unit, ramp_rate,
         ramp_rate_time_unit, generation_p_min, generation_p_max, load_p_min, load_p_max,
         capacity, operating_year, retirement_year,) = tup

        self.set_args(scenario, capacity=capacity, gau=gau, generation_p_max=generation_p_max,
                  generation_p_min=generation_p_min, generator=generator, geography=geography,
                  load_p_max=load_p_max, load_p_min=load_p_min, name=name, operating_year=operating_year,
                  ramp_rate=ramp_rate, ramp_rate_time_unit=ramp_rate_time_unit,
                  retirement_year=retirement_year, shape=shape, technology_type=technology_type, unit=unit)

class NewTechCapitalCost(DataObject):
    _instances_by_key = {}
    _table_name = "NEW_TECH_CAPITAL_COST"
    _key_col = 'name'
    _cols = ["cost_type", "currency", "currency_year", "extrapolation_method", "geography",
             "geography_map_key", "interpolation_method", "levelized", "lifecycle", "name",
             "recovery_factor", "time_unit", "unit"]
    _df_cols = ["sensitivity", "gau", "vintage", "value"]
    _df_filters = ["cost_type", "lifecycle"]
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        NewTechCapitalCost._instances_by_key[self._key] = self

        self.cost_type = None
        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.lifecycle = None
        self.name = name
        self.recovery_factor = None
        self.time_unit = None
        self.unit = None

    def set_args(self, scenario, cost_type=None, currency=None, currency_year=None, extrapolation_method=None,
                 geography=None, geography_map_key=None, interpolation_method=None, levelized=None,
                 lifecycle=None, name=None, recovery_factor=None, time_unit=None, unit=None):
        self.check_scenario(scenario)

        self.cost_type = cost_type
        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.levelized = levelized
        self.lifecycle = lifecycle
        self.name = name
        self.recovery_factor = recovery_factor
        self.time_unit = time_unit
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, cost_type, lifecycle, unit, time_unit, geography,
         geography_map_key, interpolation_method, extrapolation_method, recovery_factor, levelized,) = tup

        self.set_args(scenario, cost_type=cost_type, currency=currency, currency_year=currency_year,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  levelized=levelized, lifecycle=lifecycle, name=name, recovery_factor=recovery_factor,
                  time_unit=time_unit, unit=unit)

class OneCol(DataObject):
    _instances_by_key = {}
    _table_name = "ONE_COL"
    _key_col = 'name'
    _cols = ["name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        OneCol._instances_by_key[self._key] = self

        self.name = name

    def set_args(self, scenario, name=None):
        self.check_scenario(scenario)

        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name,) = tup

        self.set_args(scenario, name=name)

class ProductCost(DataObject):
    _instances_by_key = {}
    _table_name = "PRODUCT_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "exponential_growth_rate", "extrapolation_method", "gau",
             "geography", "geography_map_key", "interpolation_method", "name", "unit", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ProductCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.exponential_growth_rate = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.unit = None
        self.value = None
        self.year = None

    def set_args(self, scenario, currency=None, currency_year=None, exponential_growth_rate=None,
                 extrapolation_method=None, gau=None, geography=None, geography_map_key=None,
                 interpolation_method=None, name=None, unit=None, value=None, year=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.exponential_growth_rate = exponential_growth_rate
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.unit = unit
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, geography, gau, geography_map_key, interpolation_method, extrapolation_method,
         exponential_growth_rate, currency, currency_year, unit, year, value,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year,
                  exponential_growth_rate=exponential_growth_rate,
                  extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  name=name, unit=unit, value=value, year=year)

class ProductEmissions(DataObject):
    _instances_by_key = {}
    _table_name = "PRODUCT_EMISSIONS"
    _key_col = 'name'
    _cols = ["energy_unit", "extrapolation_growth_rate", "extrapolation_method", "gau", "geography",
             "interpolation_method", "mass_unit", "name", "notes", "source", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ProductEmissions._instances_by_key[self._key] = self

        self.energy_unit = None
        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.interpolation_method = None
        self.mass_unit = None
        self.name = name
        self.notes = None
        self.source = None
        self.value = None
        self.year = None

    def set_args(self, scenario, energy_unit=None, extrapolation_growth_rate=None, extrapolation_method=None, gau=None,
                 geography=None, interpolation_method=None, mass_unit=None, name=None, notes=None,
                 source=None, value=None, year=None):
        self.check_scenario(scenario)

        self.energy_unit = energy_unit
        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.interpolation_method = interpolation_method
        self.mass_unit = mass_unit
        self.name = name
        self.notes = notes
        self.source = source
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, geography, gau, interpolation_method, extrapolation_method,
         extrapolation_growth_rate, mass_unit, energy_unit, year, value,) = tup

        self.set_args(scenario, energy_unit=energy_unit, extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  interpolation_method=interpolation_method, mass_unit=mass_unit, name=name, notes=notes,
                  source=source, value=value, year=year)

class ProductMain(DataObject):
    _instances_by_key = {}
    _table_name = "PRODUCT_MAIN"
    _key_col = 'name'
    _cols = ["name"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ProductMain._instances_by_key[self._key] = self

        self.name = name

    def set_args(self, scenario, name=None):
        self.check_scenario(scenario)

        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name,) = tup

        self.set_args(scenario, name=name)

class ProductPotential(DataObject):
    _instances_by_key = {}
    _table_name = "PRODUCT_POTENTIAL"
    _key_col = 'name'
    _cols = ["extrapolation_growth_rate", "extrapolation_method", "interpolation_method", "name",
             "notes", "source", "unit", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ProductPotential._instances_by_key[self._key] = self

        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.source = None
        self.unit = None
        self.value = None
        self.year = None

    def set_args(self, scenario, extrapolation_growth_rate=None, extrapolation_method=None, interpolation_method=None,
                 name=None, notes=None, source=None, unit=None, value=None, year=None):
        self.check_scenario(scenario)

        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.source = source
        self.unit = unit
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, unit, interpolation_method, extrapolation_method,
         extrapolation_growth_rate, year, value,) = tup

        self.set_args(scenario, extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, interpolation_method=interpolation_method,
                  name=name, notes=notes, source=source, unit=unit, value=value, year=year)

class ProductRps(DataObject):
    _instances_by_key = {}
    _table_name = "PRODUCT_RPS"
    _key_col = 'name'
    _cols = ["exponential_growth_rate", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "name", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        ProductRps._instances_by_key[self._key] = self

        self.exponential_growth_rate = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.value = None
        self.year = None

    def set_args(self, scenario, exponential_growth_rate=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, value=None, year=None):
        self.check_scenario(scenario)

        self.exponential_growth_rate = exponential_growth_rate
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, geography, gau, geography_map_key, interpolation_method, extrapolation_method,
         exponential_growth_rate, year, value,) = tup

        self.set_args(scenario, exponential_growth_rate=exponential_growth_rate,
                  extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  name=name, value=value, year=year)

class TechCapacityFactor(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_CAPACITY_FACTOR"
    _key_col = 'name'
    _cols = ["extrapolation_method", "gau", "geography", "geography_map_key", "interpolation_method",
             "name"]
    _df_cols = ["vintage", "value", "sensitivity"]
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechCapacityFactor._instances_by_key[self._key] = self

        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name

    def set_args(self, scenario, extrapolation_method=None, gau=None, geography=None, geography_map_key=None,
                 interpolation_method=None, name=None):
        self.check_scenario(scenario)

        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, geography, gau, geography_map_key, interpolation_method, extrapolation_method,) = tup

        self.set_args(scenario, extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method, name=name)

class TechCapitalCost(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_CAPITAL_COST"
    _key_col = 'name'
    _cols = ["cost_type", "currency", "currency_year", "extrapolation_method", "geography",
             "geography_map_key", "interpolation_method", "levelized", "name", "recovery_factor",
             "time_unit", "unit"]
    _df_cols = ["sensitivity", "gau", "vintage", "value"]
    _df_filters = ["cost_type"]
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechCapitalCost._instances_by_key[self._key] = self

        self.cost_type = None
        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.name = name
        self.recovery_factor = None
        self.time_unit = None
        self.unit = None

    def set_args(self, scenario, cost_type=None, currency=None, currency_year=None, extrapolation_method=None,
                 geography=None, geography_map_key=None, interpolation_method=None, levelized=None,
                 name=None, recovery_factor=None, time_unit=None, unit=None):
        self.check_scenario(scenario)

        self.cost_type = cost_type
        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.levelized = levelized
        self.name = name
        self.recovery_factor = recovery_factor
        self.time_unit = time_unit
        self.unit = unit

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, cost_type, unit, time_unit, geography, geography_map_key,
         interpolation_method, extrapolation_method, recovery_factor, levelized,) = tup

        self.set_args(scenario, cost_type=cost_type, currency=currency, currency_year=currency_year,
                  extrapolation_method=extrapolation_method, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  levelized=levelized, name=name, recovery_factor=recovery_factor, time_unit=time_unit,
                  unit=unit)

class TechCurtailmentCost(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_CURTAILMENT_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "name", "sensitivity", "unit", "value", "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechCurtailmentCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.name = name
        self.sensitivity = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, name=None, sensitivity=None, unit=None, value=None,
                 vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.name = name
        self.sensitivity = sensitivity
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, vintage, value, sensitivity,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, name=name, sensitivity=sensitivity,
                  unit=unit, value=value, vintage=vintage)

class TechEfficiency(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_EFFICIENCY"
    _key_col = 'name'
    _cols = ["blend_in", "blend_out", "max_load_value", "min_load_value", "name", "unit_in",
             "unit_out", "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechEfficiency._instances_by_key[self._key] = self

        self.blend_in = None
        self.blend_out = None
        self.max_load_value = None
        self.min_load_value = None
        self.name = name
        self.unit_in = None
        self.unit_out = None
        self.vintage = None

    def set_args(self, scenario, blend_in=None, blend_out=None, max_load_value=None, min_load_value=None, name=None,
                 unit_in=None, unit_out=None, vintage=None):
        self.check_scenario(scenario)

        self.blend_in = blend_in
        self.blend_out = blend_out
        self.max_load_value = max_load_value
        self.min_load_value = min_load_value
        self.name = name
        self.unit_in = unit_in
        self.unit_out = unit_out
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, unit_in, unit_out, blend_in, blend_out, vintage, max_load_value, min_load_value,) = tup

        self.set_args(scenario, blend_in=blend_in, blend_out=blend_out, max_load_value=max_load_value,
                  min_load_value=min_load_value, name=name, unit_in=unit_in, unit_out=unit_out,
                  vintage=vintage)

class TechFixedOm(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_FIXED_OM"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "name", "notes", "source", "unit", "value",
             "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechFixedOm._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.source = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, notes=None, source=None,
                 unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.source = source
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, unit, geography, gau, geography_map_key,
         interpolation_method, extrapolation_method, vintage, value,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, extrapolation_method=extrapolation_method,
                  gau=gau, geography=geography, geography_map_key=geography_map_key,
                  interpolation_method=interpolation_method, name=name, notes=notes, source=source,
                  unit=unit, value=value, vintage=vintage)

class TechItc(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_ITC"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "input_type", "name", "sensitivity", "unit", "value",
             "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechItc._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.input_type = None
        self.name = name
        self.sensitivity = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, input_type=None, name=None, sensitivity=None,
                 unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.input_type = input_type
        self.name = name
        self.sensitivity = sensitivity
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, input_type, vintage, value, sensitivity,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, input_type=input_type, name=name,
                  sensitivity=sensitivity, unit=unit, value=value, vintage=vintage)

class TechMain(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_MAIN"
    _key_col = None
    _cols = ["DSCR", "MACRS_term", "ancillary_service_eligible", "bleed_rate", "bleed_rate_time_unit",
             "book_life", "construction_time", "curtailable", "generation_p_max", "generation_p_min",
             "lifetime", "load_p_max", "load_p_min", "max_duration", "name", "ownership_model",
             "potential_group", "ramp_rate", "ramp_rate_time_unit", "shape", "type",
             "typical_unit_size", "zone"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, scenario):
        DataObject.__init__(self, None, scenario)

        TechMain._instances_by_key[self._key] = self

        self.DSCR = None
        self.MACRS_term = None
        self.ancillary_service_eligible = None
        self.bleed_rate = None
        self.bleed_rate_time_unit = None
        self.book_life = None
        self.construction_time = None
        self.curtailable = None
        self.generation_p_max = None
        self.generation_p_min = None
        self.lifetime = None
        self.load_p_max = None
        self.load_p_min = None
        self.max_duration = None
        self.name = None
        self.ownership_model = None
        self.potential_group = None
        self.ramp_rate = None
        self.ramp_rate_time_unit = None
        self.shape = None
        self.type = None
        self.typical_unit_size = None
        self.zone = None

    def set_args(self, scenario, DSCR=None, MACRS_term=None, ancillary_service_eligible=None, bleed_rate=None,
                 bleed_rate_time_unit=None, book_life=None, construction_time=None, curtailable=None,
                 generation_p_max=None, generation_p_min=None, lifetime=None, load_p_max=None,
                 load_p_min=None, max_duration=None, name=None, ownership_model=None,
                 potential_group=None, ramp_rate=None, ramp_rate_time_unit=None, shape=None, type=None,
                 typical_unit_size=None, zone=None):
        self.check_scenario(scenario)

        self.DSCR = DSCR
        self.MACRS_term = MACRS_term
        self.ancillary_service_eligible = ancillary_service_eligible
        self.bleed_rate = bleed_rate
        self.bleed_rate_time_unit = bleed_rate_time_unit
        self.book_life = book_life
        self.construction_time = construction_time
        self.curtailable = curtailable
        self.generation_p_max = generation_p_max
        self.generation_p_min = generation_p_min
        self.lifetime = lifetime
        self.load_p_max = load_p_max
        self.load_p_min = load_p_min
        self.max_duration = max_duration
        self.name = name
        self.ownership_model = ownership_model
        self.potential_group = potential_group
        self.ramp_rate = ramp_rate
        self.ramp_rate_time_unit = ramp_rate_time_unit
        self.shape = shape
        self.type = type
        self.typical_unit_size = typical_unit_size
        self.zone = zone

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, potential_group, type, zone, construction_time, lifetime, book_life, DSCR,
         MACRS_term, ownership_model, ramp_rate, ramp_rate_time_unit, typical_unit_size,
         generation_p_min, generation_p_max, load_p_min, load_p_max, bleed_rate, max_duration,
         bleed_rate_time_unit, ancillary_service_eligible, curtailable, shape,) = tup

        self.set_args(scenario, DSCR=DSCR, MACRS_term=MACRS_term, ancillary_service_eligible=ancillary_service_eligible,
                  bleed_rate=bleed_rate, bleed_rate_time_unit=bleed_rate_time_unit, book_life=book_life,
                  construction_time=construction_time, curtailable=curtailable,
                  generation_p_max=generation_p_max, generation_p_min=generation_p_min, lifetime=lifetime,
                  load_p_max=load_p_max, load_p_min=load_p_min, max_duration=max_duration, name=name,
                  ownership_model=ownership_model, potential_group=potential_group, ramp_rate=ramp_rate,
                  ramp_rate_time_unit=ramp_rate_time_unit, shape=shape, type=type,
                  typical_unit_size=typical_unit_size, zone=zone)

class TechPotential(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_POTENTIAL"
    _key_col = 'name'
    _cols = ["extrapolation_growth_rate", "extrapolation_method", "gau", "geography",
             "geography_map_key", "incremental_to_existing", "interpolation_method", "name", "notes",
             "sensitivity", "source", "time_unit", "type", "unit", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechPotential._instances_by_key[self._key] = self

        self.extrapolation_growth_rate = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.incremental_to_existing = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.sensitivity = None
        self.source = None
        self.time_unit = None
        self.type = None
        self.unit = None
        self.value = None
        self.year = None

    def set_args(self, scenario, extrapolation_growth_rate=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, incremental_to_existing=None, interpolation_method=None,
                 name=None, notes=None, sensitivity=None, source=None, time_unit=None, type=None,
                 unit=None, value=None, year=None):
        self.check_scenario(scenario)

        self.extrapolation_growth_rate = extrapolation_growth_rate
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.incremental_to_existing = incremental_to_existing
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.sensitivity = sensitivity
        self.source = source
        self.time_unit = time_unit
        self.type = type
        self.unit = unit
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, type, incremental_to_existing, source, notes, unit, time_unit, geography, gau,
         geography_map_key, interpolation_method, extrapolation_method, extrapolation_growth_rate,
         year, value, sensitivity,) = tup

        self.set_args(scenario, extrapolation_growth_rate=extrapolation_growth_rate,
                  extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, incremental_to_existing=incremental_to_existing,
                  interpolation_method=interpolation_method, name=name, notes=notes,
                  sensitivity=sensitivity, source=source, time_unit=time_unit, type=type, unit=unit,
                  value=value, year=year)

class TechPtc(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_PTC"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "name", "sensitivity", "unit", "value", "vintage", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechPtc._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.name = name
        self.sensitivity = None
        self.unit = None
        self.value = None
        self.vintage = None
        self.year = None

    def set_args(self, scenario, currency=None, currency_year=None, name=None, sensitivity=None, unit=None, value=None,
                 vintage=None, year=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.name = name
        self.sensitivity = sensitivity
        self.unit = unit
        self.value = value
        self.vintage = vintage
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, vintage, year, value, sensitivity,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, name=name, sensitivity=sensitivity,
                  unit=unit, value=value, vintage=vintage, year=year)

class TechRetirementCost(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_RETIREMENT_COST"
    _key_col = 'name'
    _cols = ["cost_type", "currency", "currency_year", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "levelized", "name", "notes",
             "recovery_factor", "sensitivity", "source", "time_unit", "unit", "value", "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechRetirementCost._instances_by_key[self._key] = self

        self.cost_type = None
        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.name = name
        self.notes = None
        self.recovery_factor = None
        self.sensitivity = None
        self.source = None
        self.time_unit = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, cost_type=None, currency=None, currency_year=None, extrapolation_method=None, gau=None,
                 geography=None, geography_map_key=None, interpolation_method=None, levelized=None,
                 name=None, notes=None, recovery_factor=None, sensitivity=None, source=None,
                 time_unit=None, unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.cost_type = cost_type
        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.levelized = levelized
        self.name = name
        self.notes = notes
        self.recovery_factor = recovery_factor
        self.sensitivity = sensitivity
        self.source = source
        self.time_unit = time_unit
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, cost_type, unit, time_unit, geography, gau,
         geography_map_key, interpolation_method, extrapolation_method, recovery_factor,
         levelized, vintage, value, sensitivity,) = tup

        self.set_args(scenario, cost_type=cost_type, currency=currency, currency_year=currency_year,
                  extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  levelized=levelized, name=name, notes=notes, recovery_factor=recovery_factor,
                  sensitivity=sensitivity, source=source, time_unit=time_unit, unit=unit, value=value,
                  vintage=vintage)

class TechRps(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_RPS"
    _key_col = 'name'
    _cols = ["RPS", "load_modifier", "name", "sensitivity", "vintage", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechRps._instances_by_key[self._key] = self

        self.RPS = None
        self.load_modifier = None
        self.name = name
        self.sensitivity = None
        self.vintage = None
        self.year = None

    def set_args(self, scenario, RPS=None, load_modifier=None, name=None, sensitivity=None, vintage=None, year=None):
        self.check_scenario(scenario)

        self.RPS = RPS
        self.load_modifier = load_modifier
        self.name = name
        self.sensitivity = sensitivity
        self.vintage = vintage
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, load_modifier, vintage, year, RPS, sensitivity,) = tup

        self.set_args(scenario, RPS=RPS, load_modifier=load_modifier, name=name, sensitivity=sensitivity, vintage=vintage,
                  year=year)

class TechSchedule(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_SCHEDULE"
    _key_col = 'name'
    _cols = ["extrapolation_method", "gau", "geography", "geography_map_key", "interpolation_method",
             "name", "sensitivity", "time_unit", "type", "unit", "value", "year"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechSchedule._instances_by_key[self._key] = self

        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.sensitivity = None
        self.time_unit = None
        self.type = None
        self.unit = None
        self.value = None
        self.year = None

    def set_args(self, scenario, extrapolation_method=None, gau=None, geography=None, geography_map_key=None,
                 interpolation_method=None, name=None, sensitivity=None, time_unit=None, type=None,
                 unit=None, value=None, year=None):
        self.check_scenario(scenario)

        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.sensitivity = sensitivity
        self.time_unit = time_unit
        self.type = type
        self.unit = unit
        self.value = value
        self.year = year

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, type, unit, time_unit, geography, gau, geography_map_key, interpolation_method,
         extrapolation_method, year, value, sensitivity,) = tup

        self.set_args(scenario, extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method,
                  name=name, sensitivity=sensitivity, time_unit=time_unit, type=type, unit=unit,
                  value=value, year=year)

class TechShutdownCost(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_SHUTDOWN_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "name", "notes", "source", "unit", "value"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechShutdownCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.name = name
        self.notes = None
        self.source = None
        self.unit = None
        self.value = None

    def set_args(self, scenario, currency=None, currency_year=None, name=None, notes=None, source=None, unit=None,
                 value=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.name = name
        self.notes = notes
        self.source = source
        self.unit = unit
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, unit, value,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, name=name, notes=notes, source=source,
                  unit=unit, value=value)

class TechStartupCost(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_STARTUP_COST"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "name", "sensitivity", "unit", "value"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechStartupCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.name = name
        self.sensitivity = None
        self.unit = None
        self.value = None

    def set_args(self, scenario, currency=None, currency_year=None, name=None, sensitivity=None, unit=None, value=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.name = name
        self.sensitivity = sensitivity
        self.unit = unit
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, currency, currency_year, unit, value, sensitivity,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, name=name, sensitivity=sensitivity,
                  unit=unit, value=value)

class TechVariableOm(DataObject):
    _instances_by_key = {}
    _table_name = "TECH_VARIABLE_OM"
    _key_col = 'name'
    _cols = ["currency", "currency_year", "extrapolation_method", "gau", "geography",
             "geography_map_key", "interpolation_method", "name", "notes", "sensitivity", "source",
             "unit", "value", "vintage"]
    _df_cols = []
    _df_filters = []
    _data_table_name = None

    def __init__(self, name, scenario):
        DataObject.__init__(self, name, scenario)

        TechVariableOm._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = name
        self.notes = None
        self.sensitivity = None
        self.source = None
        self.unit = None
        self.value = None
        self.vintage = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_method=None, gau=None, geography=None,
                 geography_map_key=None, interpolation_method=None, name=None, notes=None,
                 sensitivity=None, source=None, unit=None, value=None, vintage=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.extrapolation_method = extrapolation_method
        self.gau = gau
        self.geography = geography
        self.geography_map_key = geography_map_key
        self.interpolation_method = interpolation_method
        self.name = name
        self.notes = notes
        self.sensitivity = sensitivity
        self.source = source
        self.unit = unit
        self.value = value
        self.vintage = vintage

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, source, notes, currency, currency_year, unit, geography, gau, geography_map_key,
         interpolation_method, extrapolation_method, vintage, value, sensitivity,) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, extrapolation_method=extrapolation_method,
                  gau=gau, geography=geography, geography_map_key=geography_map_key,
                  interpolation_method=interpolation_method, name=name, notes=notes,
                  sensitivity=sensitivity, source=source, unit=unit, value=value, vintage=vintage)

