import pdb
import logging
import numpy as np
import pandas as pd

from csvdb import CsvDatabase, CsvMetadata, DataObject
import RIO.config as cfg
from RIO.time_series import TimeSeries
import RIO.util as util

_RioMetadata = [
    CsvMetadata('GeographiesSpatialJoin',
                data_table=True),

    CsvMetadata('GeographyMapKeys',
                data_table=True),

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

    CsvMetadata('BLEND_CONNECTIONS'),

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
