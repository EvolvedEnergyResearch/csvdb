#
# This is a generated file. Manual edits may be lost!
#
import sys
from rio_database import RioData # superclass of generated classes

_Module = sys.modules[__name__]  # get ref to our own module object

class TechCapacityFactor(RioData):
    _instances_by_key = {}
    _table_name = "TECH_CAPACITY_FACTOR"
    _key_col = "name"
    _cols = ["extrapolation_method", "gau", "geography", "geography_map_key", "interpolation_method",
             "name"]
    _df_cols = ["vintage", "value", "sensitivity"]
    _df_key_col = "None"
    _data_table_name = None

    def __init__(self, name, scenario):
        RioData.__init__(self, name, scenario)

        TechCapacityFactor._instances_by_key[self._key] = self

        self.extrapolation_method = None
        self.gau = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.name = None

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
        (name, geography, gau, geography_map_key, interpolation_method, extrapolation_method) = tup

        self.set_args(scenario, extrapolation_method=extrapolation_method, gau=gau, geography=geography,
                  geography_map_key=geography_map_key, interpolation_method=interpolation_method, name=name)

class TechCapitalCost(RioData):
    _instances_by_key = {}
    _table_name = "TECH_CAPITAL_COST"
    _key_col = "name"
    _cols = ["currency", "currency_year", "extrapolation_method", "geography", "geography_map_key",
             "interpolation_method", "levelized", "name", "recovery_factor", "time_unit", "unit"]
    _df_cols = ["vintage", "gau", "sensitivity", "value"]
    _df_key_col = "cost_type"
    _data_table_name = None

    def __init__(self, name, scenario):
        RioData.__init__(self, name, scenario)

        TechCapitalCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.extrapolation_method = None
        self.geography = None
        self.geography_map_key = None
        self.interpolation_method = None
        self.levelized = None
        self.name = None
        self.recovery_factor = None
        self.time_unit = None
        self.unit = None

    def set_args(self, scenario, currency=None, currency_year=None, extrapolation_method=None, geography=None,
                 geography_map_key=None, interpolation_method=None, levelized=None, name=None,
                 recovery_factor=None, time_unit=None, unit=None):
        self.check_scenario(scenario)

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
        (name, currency, currency_year, unit, time_unit, geography, geography_map_key,
         interpolation_method, extrapolation_method, recovery_factor, levelized) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, extrapolation_method=extrapolation_method,
                  geography=geography, geography_map_key=geography_map_key,
                  interpolation_method=interpolation_method, levelized=levelized, name=name,
                  recovery_factor=recovery_factor, time_unit=time_unit, unit=unit)

class TechMain(RioData):
    _instances_by_key = {}
    _table_name = "TECH_MAIN"
    _key_col = "name"
    _cols = ["DSCR", "MACRS_term", "ancillary_service_eligible", "bleed_rate", "bleed_rate_time_unit",
             "book_life", "construction_time", "curtailable", "generation_p_max", "generation_p_min",
             "interconnection", "lifetime", "load_p_max", "load_p_min", "max_duration", "name",
             "ownership_model", "potential_group", "ramp_rate", "ramp_rate_time_unit", "shape",
             "type", "typical_unit_size"]
    _df_cols = []
    _df_key_col = "None"
    _data_table_name = None

    def __init__(self, name, scenario):
        RioData.__init__(self, name, scenario)

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
        self.interconnection = None
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

    def set_args(self, scenario, DSCR=None, MACRS_term=None, ancillary_service_eligible=None, bleed_rate=None,
                 bleed_rate_time_unit=None, book_life=None, construction_time=None, curtailable=None,
                 generation_p_max=None, generation_p_min=None, interconnection=None, lifetime=None,
                 load_p_max=None, load_p_min=None, max_duration=None, name=None, ownership_model=None,
                 potential_group=None, ramp_rate=None, ramp_rate_time_unit=None, shape=None, type=None,
                 typical_unit_size=None):
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
        self.interconnection = interconnection
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

    def init_from_tuple(self, tup, scenario, **kwargs):    
        (name, potential_group, type, interconnection, construction_time, lifetime, book_life,
         DSCR, MACRS_term, ownership_model, ramp_rate, ramp_rate_time_unit, typical_unit_size,
         generation_p_min, generation_p_max, load_p_min, load_p_max, bleed_rate, max_duration,
         bleed_rate_time_unit, ancillary_service_eligible, curtailable, shape) = tup

        self.set_args(scenario, DSCR=DSCR, MACRS_term=MACRS_term, ancillary_service_eligible=ancillary_service_eligible,
                  bleed_rate=bleed_rate, bleed_rate_time_unit=bleed_rate_time_unit, book_life=book_life,
                  construction_time=construction_time, curtailable=curtailable,
                  generation_p_max=generation_p_max, generation_p_min=generation_p_min,
                  interconnection=interconnection, lifetime=lifetime, load_p_max=load_p_max,
                  load_p_min=load_p_min, max_duration=max_duration, name=name,
                  ownership_model=ownership_model, potential_group=potential_group, ramp_rate=ramp_rate,
                  ramp_rate_time_unit=ramp_rate_time_unit, shape=shape, type=type,
                  typical_unit_size=typical_unit_size)

class TechStartupCost(RioData):
    _instances_by_key = {}
    _table_name = "TECH_STARTUP_COST"
    _key_col = "name"
    _cols = ["currency", "currency_year", "name", "unit", "value"]
    _df_cols = []
    _df_key_col = "None"
    _data_table_name = None

    def __init__(self, name, scenario):
        RioData.__init__(self, name, scenario)

        TechStartupCost._instances_by_key[self._key] = self

        self.currency = None
        self.currency_year = None
        self.name = None
        self.unit = None
        self.value = None

    def set_args(self, scenario, currency=None, currency_year=None, name=None, unit=None, value=None):
        self.check_scenario(scenario)

        self.currency = currency
        self.currency_year = currency_year
        self.name = name
        self.unit = unit
        self.value = value

    def init_from_tuple(self, tup, scenario, **kwargs):
        (name, currency, currency_year, unit, value) = tup

        self.set_args(scenario, currency=currency, currency_year=currency_year, name=name, unit=unit, value=value)

