#
# Example of a subclass of CsvDatabase that adds metadata.
#

import pdb
import logging
import numpy as np
import pandas as pd

from RIO.csvdb.csvdb import CsvDatabase, CsvMetadata, DataObject
import RIO.config as cfg
from RIO.time_series import TimeSeries
import RIO.util as util

_RioMetadata = [
    CsvMetadata('GeographiesSpatialJoin'),

    CsvMetadata('TECH_MAIN'),  # all defaults apply

    CsvMetadata('TECH_CAPITAL_COST',
                key_col='name',
                df_key_col='cost_type',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('TECH_CAPACITY_FACTOR',
                df_cols=['vintage', 'gau', 'value', 'sensitivity'],
                drop_cols=['source', 'notes']),

    CsvMetadata('TECH_CAPACITY_FACTOR',
                df_cols=['vintage', 'gau', 'value', 'sensitivity']),

    CsvMetadata('TECH_STARTUP_COST',
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_CAPITAL_COST',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_EXISTING_STORAGE',
                df_cols=['sensitivity', 'gau', 'year', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('BLEND_FUEL_INPUTS'),

    CsvMetadata('CONVERSION_CAPITAL_COST',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes']),

    CsvMetadata('CONVERSION_EFFICIENCY',
                df_cols=['sensitivity', 'gau', 'vintage', 'value'],
                drop_cols=['source', 'notes'])
]

class RioDatabase(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(RioDatabase, self).__init__(pathname=pathname, load=load, metadata=_RioMetadata)

# Superclass to generated classes in schema.py. Just a placeholder for now,
# but could house datamapfunctions-type of functionality here.
class DataMapper(DataObject):
    def __init__(self, key, scenario):
        super(DataMapper, self).__init__(key, scenario)

        # TBD: may not be needed here
        # ivars set in create_index_levels
        # self.column_names = None
        # self.index_levels = None
        # self.df_index_names = None

    def clean_timeseries(self, attr='values', inplace=True, time_index_name='year',
                         time_index=None, lower=0, upper=None, interpolation_method='missing',
                         extrapolation_method='missing'):
        if time_index is None:
            time_index = cfg.cfgfile.get('case', 'years')
            # time_index = self._get_active_time_index(time_index, time_index_name)
        interpolation_method = self.interpolation_method if interpolation_method is 'missing' else interpolation_method
        extrapolation_method = self.extrapolation_method if extrapolation_method is 'missing' else extrapolation_method
        exp_growth_rate = self.extrapolation_growth if hasattr(self, 'extrapolation_growth') else None

        data = getattr(self, attr)
        clean_data = TimeSeries.clean(data=data, newindex=time_index, time_index_name=time_index_name,
                                      interpolation_method=interpolation_method,
                                      extrapolation_method=extrapolation_method,
                                      exp_growth_rate=exp_growth_rate).clip(lower=lower, upper=upper)

        if inplace:
            setattr(self, attr, clean_data)
        else:
            return clean_data

    def geo_map(self, converted_geography, attr='values', inplace=True, current_geography=None, current_data_type=None,
                fill_value=0., filter_geo=True):
        """ maps a dataframe to another geography using relational GeographyMapdatabase table
        if input type is a total, then the subsection is the geography
        to convert to and the supersection is the initial geography.
        Example:
          input_type = 'total'
          state --> census division.
          How much of state maine is in census division new england?
          new england = subsection
          maine = supersection
        Otherwise the subsection and supersection values are reversed.
        Example:
           input_type = 'intensity'
           state --> census division.
           How much of census division new england does the state of maine represent?
           maine = subsection
           new england = supersection
        """
        # Unless specified, input_type used is attribute of the object
        current_data_type = self.input_type if current_data_type is None else current_data_type
        current_geography = self.geography if current_geography is None else current_geography
        geography_map_key = cfg.cfgfile.get('case', 'default_geography_map_key') if not hasattr(self, 'geography_map_key') else self.geography_map_key

        if current_geography not in getattr(self, attr).index.names:
            logging.error("Dataframe being mapped doesn't have the stated current geography: {}".format(self.__class__))
            pdb.set_trace()

        mapped_data = cfg.geo.geo_map(getattr(self, attr), current_geography, converted_geography,
                                      current_data_type, geography_map_key, fill_value, filter_geo)

        if inplace:
            setattr(self, attr, mapped_data.sort())
        else:
            return mapped_data.sort()

    def account_for_foreign_gaus(self, attr, current_data_type, current_geography):
        geography_map_key = cfg.cfgfile.get('case', 'default_geography_map_key') if not hasattr(self, 'geography_map_key') else self.geography_map_key
        df = getattr(self, attr).copy()
        if cfg.include_foreign_gaus:
            native_gaus, current_gaus, foreign_gaus = cfg.geo.get_native_current_foreign_gaus(df, current_geography)
            if foreign_gaus:
                name = '{} {}'.format(self.sql_id_table, self.name if hasattr(self, 'name') else 'id ' + str(self.id))
                logging.info('      Detected foreign gaus for {}: {}'.format(name, ', '.join(
                    [cfg.geo.geography_names[f] for f in foreign_gaus])))
                df, current_geography = cfg.geo.incorporate_foreign_gaus(df, current_geography, current_data_type,
                                                                         geography_map_key)
        else:
            df = cfg.geo.filter_foreign_gaus(df, current_geography)
        return df, current_geography

    def _add_missing_geographies(self, df, current_geography, current_data_type):
        current_number_of_geographies = len(util.get_elements_from_level(df, current_geography))
        propper_number_of_geographies = len(cfg.geo.geographies_unfiltered[current_geography])
        if current_data_type == 'total' and current_number_of_geographies != propper_number_of_geographies:
            # we only want to do it when we have a total, otherwise we can't just fill with zero
            df = util.reindex_df_level_with_new_elements(df, current_geography,
                                                         cfg.geo.geographies_unfiltered[current_geography],
                                                         fill_value=np.nan)
        return df

    def _get_active_time_index(self, time_index, time_index_name):
        if time_index is None:
            time_index = getattr(self, time_index_name + "s") if hasattr(self, time_index_name + "s") else cfg.cfgfile.get('case', 'years')
        return time_index  # this is a list of years

    def _get_df_index_names_in_a_list(self, df):
        return df.index.names if df.index.nlevels > 1 else [df.index.name]

    def remap(self, map_from='raw_values', map_to='values', drivers=None, time_index_name='year',
              time_index=None, fill_timeseries=True, interpolation_method='missing', extrapolation_method='missing',
              converted_geography=None, current_geography=None, current_data_type=None, fill_value=0., lower=0,
              upper=None, filter_geo=True, driver_geography=None):
        """ Map data to drivers and geography
        Args:
            map_from (string): starting variable name (defaults to 'raw_values')
            map_to (string): ending variable name (defaults to 'values')
            drivers (list of or single dataframe): drivers for the remap
            input_type_override (string): either 'total' or 'intensity' (defaults to self.type)
        """
        driver_geography = cfg.disagg_geography if driver_geography is None else driver_geography
        current_data_type = self.input_type if current_data_type is None else current_data_type
        current_geography = self.geography if current_geography is None else current_geography
        time_index = self._get_active_time_index(time_index, time_index_name)
        if current_geography not in self._get_df_index_names_in_a_list(getattr(self, map_from)):
            raise ValueError('Current geography does not match the geography of the dataframe in remap')

        # deals with foreign gaus and updates the geography
        df, current_geography = self.account_for_foreign_gaus(map_from, current_data_type, current_geography)
        setattr(self, map_to, df)

        # This happens when we are on a geography level and some of the elements are missing. Such as no PR when we have all the other U.S. States.
        setattr(self, map_to, self._add_missing_geographies(df, current_geography, current_data_type))

        if (drivers is None) or (not len(drivers)):
            # we have no drivers, just need to do a clean timeseries and a geomap
            if fill_timeseries:
                self.clean_timeseries(attr=map_to, inplace=True, time_index=time_index, time_index_name=time_index_name,
                                      interpolation_method=interpolation_method,
                                      extrapolation_method=extrapolation_method,
                                      lower=lower, upper=upper)
            if current_geography != converted_geography:
                self.geo_map(converted_geography, attr=map_to, inplace=True, current_geography=current_geography,
                             current_data_type=current_data_type, fill_value=fill_value, filter_geo=filter_geo)
                current_geography = converted_geography
        else:
            # becomes an attribute of self just because we may do a geomap on it
            self.total_driver = util.DfOper.mult(util.put_in_list(drivers))
            # turns out we don't always have a year or vintage column for drivers. For instance when linked_demand_technology gets remapped
            if time_index_name in self.total_driver.index.names:
                # sometimes when we have a linked service demand driver in a demand subsector it will come in on a fewer number of years than self.years, making this clean timeseries necesary
                self.clean_timeseries(attr='total_driver', inplace=True, time_index_name=time_index_name,
                                      time_index=time_index, lower=None, upper=None, interpolation_method='missing',
                                      extrapolation_method='missing')

            # While not on primary geography, geography does have some information we would like to preserve
            if hasattr(self, 'drivers') and len(drivers) == len(self.drivers) and set(
                    [x.input_type for x in self.drivers.values()]) == set(['intensity']) and set(
                    [x.base_driver_id for x in self.drivers.values()]) == set([None]):
                driver_mapping_data_type = 'intensity'
            else:
                driver_mapping_data_type = 'total'
            total_driver_current_geo = self.geo_map(current_geography, attr='total_driver', inplace=False,
                                                    current_geography=driver_geography,
                                                    current_data_type=driver_mapping_data_type, fill_value=fill_value,
                                                    filter_geo=False)
            if current_data_type == 'total':
                if fill_value is np.nan:
                    df_intensity = util.DfOper.divi((getattr(self, map_to), total_driver_current_geo),
                                               expandable=(False, True), collapsible=(False, True),
                                               fill_value=fill_value).replace([np.inf], 0)
                else:
                    df_intensity = util.DfOper.divi((getattr(self, map_to), total_driver_current_geo),
                                               expandable=(False, True), collapsible=(False, True),
                                               fill_value=fill_value).replace([np.inf, np.nan, -np.nan], 0)
                setattr(self, map_to, df_intensity)

            # Clean the timeseries as an intensity
            if fill_timeseries:
                self.clean_timeseries(attr=map_to, inplace=True, time_index=time_index,
                                      interpolation_method=interpolation_method,
                                      extrapolation_method=extrapolation_method, lower=lower, upper=upper)

            #            self.geo_map(converted_geography, attr=map_to, inplace=True, current_geography=current_geography, current_data_type='intensity', fill_value=fill_value, filter_geo=filter_geo)
            #            total_driver_converted_geo = self.geo_map(converted_geography, attr='total_driver', inplace=False, current_geography=driver_geography, current_data_type=driver_mapping_data_type, fill_value=fill_value, filter_geo=filter_geo)
            if current_data_type == 'total':
                setattr(self, map_to,
                        util.DfOper.mult((getattr(self, map_to), total_driver_current_geo), fill_value=fill_value))
            else:
                setattr(self, map_to,
                        util.DfOper.mult((getattr(self, map_to), total_driver_current_geo), expandable=(True, False),
                                    collapsible=(False, True), fill_value=fill_value))
            self.geo_map(converted_geography, attr=map_to, inplace=True, current_geography=current_geography,
                         current_data_type='total', fill_value=fill_value, filter_geo=filter_geo)
            # we don't want to keep this around
            del self.total_driver
