from RIO.csvdb.csvdb.database import *

_Metadata = [
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
                df_cols=['vintage', 'value', 'sensitivity'],
                drop_cols=['source', 'notes']),

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


class TestDatabase(CsvDatabase):
    def __init__(self, pathname=None, load=True):
        super(TestDatabase, self).__init__(pathname=pathname, load=load, metadata=_Metadata)
